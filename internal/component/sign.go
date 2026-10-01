package component

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
)

// Signing-key discipline (COL-09): the ed25519 private key is generated and
// held offline by the release operator; only the public key travels with the
// product in the keyring. Private key files are created 0600, never
// overwritten by tooling, and must never be committed (see .gitignore).

// GenerateSigningKey creates a new ed25519 keypair for package signing.
func GenerateSigningKey() (ed25519.PublicKey, ed25519.PrivateKey, error) {
	return ed25519.GenerateKey(rand.Reader)
}

// SavePrivateKey writes a hex-encoded private key with owner-only
// permissions, refusing to overwrite an existing file: key rotation is a
// deliberate act, never a side effect.
func SavePrivateKey(path string, key ed25519.PrivateKey) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		if os.IsExist(err) {
			return fmt.Errorf("refusing to overwrite existing key file %s", path)
		}
		return err
	}
	if _, err := file.WriteString(hex.EncodeToString(key) + "\n"); err != nil {
		file.Close()
		_ = os.Remove(path)
		return err
	}
	return file.Close()
}

// LoadPrivateKey reads a hex-encoded ed25519 private key. On Unix the file
// must be owner-only; on Windows the ACL is the operator's responsibility
// (same split as the launcher's environment file checks).
func LoadPrivateKey(path string) (ed25519.PrivateKey, error) {
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("key file %s is missing or not a regular file", path)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0077 != 0 {
		return nil, fmt.Errorf("key file %s permissions must deny all group and other access", path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	key, err := hex.DecodeString(string(trimSpaceBytes(data)))
	if err != nil || len(key) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("key file %s does not contain a hex-encoded ed25519 private key", path)
	}
	return ed25519.PrivateKey(key), nil
}

// trimSpaceBytes trims surrounding ASCII whitespace.
func trimSpaceBytes(data []byte) []byte {
	for len(data) > 0 && (data[0] == ' ' || data[0] == '\t' || data[0] == '\r' || data[0] == '\n') {
		data = data[1:]
	}
	for len(data) > 0 {
		last := data[len(data)-1]
		if last != ' ' && last != '\t' && last != '\r' && last != '\n' {
			break
		}
		data = data[:len(data)-1]
	}
	return data
}

// BuildManifest hashes every regular file under dir (excluding a previous
// manifest.json) into an unsigned manifest. osName/arch default to this host;
// pass explicit values when cross-packaging for another platform.
func BuildManifest(dir, componentName, version string, format StateFormat, osName, arch string) (*Manifest, error) {
	if osName == "" {
		osName = runtime.GOOS
	}
	if arch == "" {
		arch = runtime.GOARCH
	}
	manifest := &Manifest{
		SchemaVersion: 1,
		Component:     componentName,
		Version:       version,
		OS:            osName,
		Architecture:  arch,
		StateFormat:   format,
		Files:         map[string]string{},
	}
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("package contains a non-regular file: %s", path)
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if rel == manifestFileName {
			return nil
		}
		digest, _, err := hashFile(path)
		if err != nil {
			return err
		}
		manifest.Files[rel] = digest
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(manifest.Files) == 0 {
		return nil, errors.New("package directory contains no files to sign")
	}
	return manifest, nil
}

// SignPackage builds the manifest of the package directory and writes it
// signed to <dir>/manifest.json, refusing to overwrite an existing manifest.
func SignPackage(dir, componentName, version string, format StateFormat, osName, arch, keyID string, key ed25519.PrivateKey) error {
	target := filepath.Join(dir, manifestFileName)
	if _, err := os.Stat(target); err == nil {
		return fmt.Errorf("%s already exists; refusing to re-sign in place", target)
	}
	manifest, err := BuildManifest(dir, componentName, version, format, osName, arch)
	if err != nil {
		return err
	}
	signature, err := SignManifest(manifest, keyID, key)
	if err != nil {
		return err
	}
	manifest.Signature = signature
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(target, append(data, '\n'), 0644)
}
