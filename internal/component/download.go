package component

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

// Downloader fetches signed component packages from a release repository over
// HTTP(S). The repository is a plain file tree:
//
//	<base>/<component>/<version>/manifest.json
//	<base>/<component>/<version>/<file...>   one entry per manifest file
//
// Downloads land in a staging directory and are renamed into place only after
// the manifest signature and every file hash verify — a partial or tampered
// download never leaves a usable package behind.
type Downloader struct {
	Client  *http.Client
	Keyring Keyring
	// MaxFileBytes caps a single package file (default 2 GiB).
	MaxFileBytes int64
	// RateLimitBytesPerSecond throttles download throughput (0 = unlimited).
	RateLimitBytesPerSecond int64
}

func (d *Downloader) client() *http.Client {
	if d.Client != nil {
		return d.Client
	}
	return &http.Client{Timeout: 10 * time.Minute}
}

func (d *Downloader) maxFileBytes() int64 {
	if d.MaxFileBytes > 0 {
		return d.MaxFileBytes
	}
	return 2 << 30
}

// Fetch downloads component/version from repoBaseURL into destDir and returns
// the verified manifest. destDir must not exist yet.
func (d *Downloader) Fetch(ctx context.Context, repoBaseURL, componentName, version, destDir string) (*Manifest, error) {
	if d.Keyring == nil {
		return nil, errors.New("downloader has no keyring")
	}
	if !namePattern.MatchString(componentName) || !versionPattern.MatchString(version) {
		return nil, fmt.Errorf("invalid component %q or version %q", componentName, version)
	}
	base := strings.TrimRight(repoBaseURL, "/")
	if !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://") {
		return nil, fmt.Errorf("repository URL %q must be http(s)", repoBaseURL)
	}
	if _, err := os.Stat(destDir); err == nil {
		return nil, fmt.Errorf("destination %s already exists", destDir)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	parent := filepath.Dir(destDir)
	if err := os.MkdirAll(parent, 0755); err != nil {
		return nil, err
	}
	staging, err := os.MkdirTemp(parent, ".download-")
	if err != nil {
		return nil, err
	}
	// Any exit before the rename removes all partial content.
	defer os.RemoveAll(staging)

	packageBase := base + "/" + componentName + "/" + version
	manifestData, err := d.get(ctx, packageBase+"/"+manifestFileName, maxManifestBytes)
	if err != nil {
		return nil, fmt.Errorf("download package manifest: %w", err)
	}
	if err := os.WriteFile(filepath.Join(staging, manifestFileName), manifestData, 0644); err != nil {
		return nil, err
	}
	// Signature and structural validation run before any payload bytes are
	// fetched: an untrusted manifest must not even drive the download list.
	manifest, err := LoadManifest(staging, d.Keyring)
	if err != nil {
		return nil, err
	}
	if manifest.Component != componentName || manifest.Version != version {
		return nil, fmt.Errorf("repository served a manifest for %s %s, expected %s %s", manifest.Component, manifest.Version, componentName, version)
	}
	for _, rel := range manifest.sortedFiles() {
		if err := d.fetchFile(ctx, packageBase+"/"+rel, filepath.Join(staging, filepath.FromSlash(rel)), manifest.Files[rel]); err != nil {
			return nil, err
		}
	}
	if _, err := verifyPackageFiles(staging, manifest); err != nil {
		return nil, err
	}
	if err := os.Rename(staging, destDir); err != nil {
		return nil, fmt.Errorf("install downloaded package: %w", err)
	}
	return manifest, nil
}

func (d *Downloader) get(ctx context.Context, url string, limit int64) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := d.client().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: %s", url, resp.Status)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("GET %s: response exceeds %d bytes", url, limit)
	}
	return data, nil
}

// fetchFile streams one package file to disk while hashing it, then compares
// the digest against the signed manifest.
func (d *Downloader) fetchFile(ctx context.Context, url, dest, expectedSHA256 string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := d.client().Do(req)
	if err != nil {
		return fmt.Errorf("download %s: %w", path.Base(url), err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download %s: %s", path.Base(url), resp.Status)
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0755); err != nil {
		return err
	}
	out, err := os.OpenFile(dest, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	reader := io.Reader(resp.Body)
	if d.RateLimitBytesPerSecond > 0 {
		reader = &throttledReader{reader: reader, rate: d.RateLimitBytesPerSecond, started: time.Now()}
	}
	hasher := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(out, hasher), io.LimitReader(reader, d.maxFileBytes()+1))
	closeErr := out.Close()
	if copyErr != nil {
		return fmt.Errorf("download %s: %w", path.Base(url), copyErr)
	}
	if closeErr != nil {
		return closeErr
	}
	if written > d.maxFileBytes() {
		return fmt.Errorf("download %s exceeds the %d byte package file limit", path.Base(url), d.maxFileBytes())
	}
	if hex.EncodeToString(hasher.Sum(nil)) != expectedSHA256 {
		return fmt.Errorf("downloaded %s does not match its signed SHA-256", path.Base(url))
	}
	return nil
}

// throttledReader paces reads to rate bytes/second by sleeping whenever the
// transfer runs ahead of its time budget.
type throttledReader struct {
	reader  io.Reader
	rate    int64
	started time.Time
	read    int64
}

func (t *throttledReader) Read(p []byte) (int, error) {
	n, err := t.reader.Read(p)
	t.read += int64(n)
	budget := time.Duration(t.read * int64(time.Second) / t.rate)
	if ahead := budget - time.Since(t.started); ahead > 0 {
		time.Sleep(ahead)
	}
	return n, err
}
