package component

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"runtime"
	"sort"
	"strings"
)

const (
	maxManifestBytes = 1 << 20
	manifestFileName = "manifest.json"

	signatureAlgorithm = "ed25519"
)

var (
	namePattern    = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$`)
	versionPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$`)
)

// StateFormat declares the on-disk registry/data format a component version
// writes and the oldest format it can still read. It is the compatibility
// contract that gates both upgrade and rollback: a binary must never be
// pointed at a data directory it cannot read, in either direction.
type StateFormat struct {
	// Version is the format version this component version writes.
	Version int `json:"version"`
	// MinReadable is the oldest on-disk format version this component
	// version can read.
	MinReadable int `json:"min_readable"`
}

// Signature signs the canonical encoding of the manifest with the signature
// field removed.
type Signature struct {
	Algorithm string `json:"algorithm"`
	KeyID     string `json:"key_id"`
	// Value is the base64-encoded signature.
	Value string `json:"value"`
}

// Manifest is the signed upgrade manifest shipped inside a component package
// directory. It pins the component, version, OS/architecture, the state
// format compatibility range, and the SHA-256 of every file in the package.
type Manifest struct {
	SchemaVersion int         `json:"schema_version"`
	Component     string      `json:"component"`
	Version       string      `json:"version"`
	OS            string      `json:"os"`
	Architecture  string      `json:"architecture"`
	StateFormat   StateFormat `json:"state_format"`
	// Files maps slash-separated package-relative paths to lowercase hex
	// SHA-256 digests.
	Files     map[string]string `json:"files"`
	Signature Signature         `json:"signature"`

	path string
}

// Keyring maps key IDs to the ed25519 public keys the supervisor trusts for
// component packages. A manifest signed by an unknown key is refused.
type Keyring map[string]ed25519.PublicKey

// LoadKeyring reads a JSON keyring file of the form
// {"<key_id>": "<hex-encoded ed25519 public key>", ...}.
func LoadKeyring(path string) (Keyring, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read keyring: %w", err)
	}
	if len(data) > 64<<10 {
		return nil, errors.New("keyring exceeds 64 KiB")
	}
	var raw map[string]string
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode keyring: %w", err)
	}
	if len(raw) == 0 {
		return nil, errors.New("keyring must contain at least one key")
	}
	ring := Keyring{}
	for id, text := range raw {
		if strings.TrimSpace(id) == "" {
			return nil, errors.New("keyring contains an empty key id")
		}
		key, err := hex.DecodeString(text)
		if err != nil || len(key) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("keyring key %q is not a hex-encoded ed25519 public key", id)
		}
		ring[id] = ed25519.PublicKey(key)
	}
	return ring, nil
}

// LoadManifest reads and fully validates the signed manifest of the package
// in packageDir against the keyring. Nothing outside this function may trust
// a manifest that has not been verified.
func LoadManifest(packageDir string, ring Keyring) (*Manifest, error) {
	manifest, unsigned, err := readManifest(packageDir)
	if err != nil {
		return nil, err
	}
	if err := manifest.validate(); err != nil {
		return nil, err
	}
	if err := verifySignature(manifest, unsigned, ring); err != nil {
		return nil, err
	}
	return manifest, nil
}

// SignManifest renders the canonical unsigned encoding of m and signs it with
// key, returning the signature to embed. It exists for package builders and
// tests; production keys never live on supervised hosts.
func SignManifest(m *Manifest, keyID string, key ed25519.PrivateKey) (Signature, error) {
	unsigned, err := canonicalUnsigned(m)
	if err != nil {
		return Signature{}, err
	}
	return Signature{
		Algorithm: signatureAlgorithm,
		KeyID:     keyID,
		Value:     base64.StdEncoding.EncodeToString(ed25519.Sign(key, unsigned)),
	}, nil
}

func readManifest(packageDir string) (*Manifest, []byte, error) {
	path := packageDir + string(os.PathSeparator) + manifestFileName
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("open package manifest: %w", err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxManifestBytes+1))
	if err != nil {
		return nil, nil, fmt.Errorf("read package manifest: %w", err)
	}
	if len(data) > maxManifestBytes {
		return nil, nil, errors.New("package manifest exceeds 1 MiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var manifest Manifest
	if err := decoder.Decode(&manifest); err != nil {
		return nil, nil, fmt.Errorf("decode package manifest: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, nil, errors.New("package manifest must contain exactly one JSON object")
	}
	unsigned, err := canonicalUnsigned(&manifest)
	if err != nil {
		return nil, nil, err
	}
	manifest.path = path
	return &manifest, unsigned, nil
}

// canonicalUnsigned marshals the manifest with the signature field removed.
// Struct field order makes the encoding deterministic, and the map is encoded
// with sorted keys by encoding/json.
func canonicalUnsigned(m *Manifest) ([]byte, error) {
	unsigned := *m
	unsigned.Signature = Signature{}
	data, err := json.Marshal(unsigned)
	if err != nil {
		return nil, fmt.Errorf("encode unsigned manifest: %w", err)
	}
	return data, nil
}

func verifySignature(m *Manifest, unsigned []byte, ring Keyring) error {
	if m.Signature.Algorithm != signatureAlgorithm {
		return fmt.Errorf("unsupported signature algorithm %q", m.Signature.Algorithm)
	}
	key, ok := ring[m.Signature.KeyID]
	if !ok {
		return fmt.Errorf("package manifest is signed by untrusted key %q", m.Signature.KeyID)
	}
	signature, err := base64.StdEncoding.DecodeString(m.Signature.Value)
	if err != nil || len(signature) != ed25519.SignatureSize {
		return errors.New("package manifest signature is malformed")
	}
	if !ed25519.Verify(key, unsigned, signature) {
		return errors.New("package manifest signature does not match its content")
	}
	return nil
}

func (m *Manifest) validate() error {
	if m.SchemaVersion != 1 {
		return fmt.Errorf("unsupported package manifest schema_version %d", m.SchemaVersion)
	}
	if !namePattern.MatchString(m.Component) {
		return fmt.Errorf("invalid component name %q", m.Component)
	}
	if !versionPattern.MatchString(m.Version) {
		return fmt.Errorf("invalid component version %q", m.Version)
	}
	if m.OS != runtime.GOOS || m.Architecture != runtime.GOARCH {
		return fmt.Errorf("package targets %s/%s but this host is %s/%s", m.OS, m.Architecture, runtime.GOOS, runtime.GOARCH)
	}
	if m.StateFormat.Version < 1 || m.StateFormat.MinReadable < 1 || m.StateFormat.MinReadable > m.StateFormat.Version {
		return fmt.Errorf("invalid state format range min_readable=%d version=%d", m.StateFormat.MinReadable, m.StateFormat.Version)
	}
	if len(m.Files) == 0 || len(m.Files) > 4096 {
		return errors.New("package manifest must list between 1 and 4096 files")
	}
	for path, digest := range m.Files {
		if err := validatePackagePath(path); err != nil {
			return err
		}
		if matched, _ := regexp.MatchString(`^[0-9a-f]{64}$`, digest); !matched {
			return fmt.Errorf("file %q has an invalid SHA-256 digest", path)
		}
	}
	return nil
}

// validatePackagePath rejects absolute paths, backslashes, and anything that
// could escape the package directory.
func validatePackagePath(path string) error {
	if path == "" || strings.HasPrefix(path, "/") || strings.Contains(path, `\`) {
		return fmt.Errorf("file %q is not a package-relative slash path", path)
	}
	for _, part := range strings.Split(path, "/") {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("file %q contains an unsafe path element", path)
		}
	}
	if path == manifestFileName {
		return fmt.Errorf("file %q is the manifest itself", path)
	}
	return nil
}

// sortedFiles returns the manifest file paths in a stable order.
func (m *Manifest) sortedFiles() []string {
	paths := make([]string, 0, len(m.Files))
	for path := range m.Files {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths
}
