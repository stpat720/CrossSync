// Package testgen creates deterministic filesystem test data INSIDE a folder
// root so convergence can be exercised end to end without shell access, SMB
// credentials or manual copying: the daemon on each node writes its own side
// from the same spec, and "the same data on both boxes" means identical bytes
// AND identical mtimes, not merely similar content.
//
// Everything is confined to a `testdata/` subtree of the folder root. The
// generator refuses to touch anything else, so a mis-typed path can never
// delete user data.
package testgen

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// RootPrefix is the only subtree the generator will ever write to or remove.
const RootPrefix = "testdata"

// Spec describes one generator operation. Zero fields are ignored, so the same
// struct serves "create a corpus", "add files", "rewrite a few files", "rename a
// directory" and "clean up".
type Spec struct {
	// Kind is base|add|modify|rename|cleanup.
	Kind string `json:"kind"`
	// Dir is the relative directory to operate on, under testdata/ (e.g.
	// "testdata/base"). For rename it is the source.
	Dir string `json:"dir"`
	// To is the rename destination (relative, under testdata/).
	To string `json:"to,omitempty"`
	// Files is how many files to create (base/add) or rewrite (modify).
	Files int `json:"files,omitempty"`
	// Size is the byte size of each created file (base/add).
	Size int64 `json:"size,omitempty"`
	// Seed makes content deterministic: the same seed on two nodes produces
	// byte-identical files, so a "mostly in sync" pair really is in sync.
	Seed int64 `json:"seed,omitempty"`
	// StartAt offsets the file numbering, so "add" appends rather than
	// overwriting (a rename/modify target must exist).
	StartAt int `json:"start_at,omitempty"`
	// MTime sets the modification time (unix seconds) for created files; 0 means
	// a deterministic value derived from the index, which is what makes two
	// independently generated corpora identical.
	MTime int64 `json:"mtime,omitempty"`
	// GapSeconds spaces the deterministic mtimes (default 1).
	GapSeconds int `json:"gap_seconds,omitempty"`
}

// Result reports what happened.
type Result struct {
	Kind    string `json:"kind"`
	Dir     string `json:"dir"`
	Files   int    `json:"files"`
	Bytes   int64  `json:"bytes"`
	Elapsed int64  `json:"elapsed_ms"`
}

// Run executes spec against root.
func Run(root string, spec Spec) (Result, error) {
	start := time.Now()
	res := Result{Kind: spec.Kind, Dir: spec.Dir}
	switch spec.Kind {
	case "base", "add":
		n, b, err := createFiles(root, spec)
		res.Files, res.Bytes = n, b
		if err != nil {
			return res, err
		}
	case "modify":
		n, b, err := modifyFiles(root, spec)
		res.Files, res.Bytes = n, b
		if err != nil {
			return res, err
		}
	case "rename":
		if err := renamePath(root, spec.Dir, spec.To); err != nil {
			return res, err
		}
		res.Files = 1
	case "cleanup":
		n, err := cleanup(root, spec.Dir)
		res.Files = n
		if err != nil {
			return res, err
		}
	default:
		return res, fmt.Errorf("unknown kind %q (want base|add|modify|rename|cleanup)", spec.Kind)
	}
	res.Elapsed = time.Since(start).Milliseconds()
	return res, nil
}

// resolve validates a relative path and returns its absolute form, refusing
// anything outside root/testdata.
func resolve(root, rel string) (string, error) {
	rel = strings.TrimSpace(strings.ReplaceAll(rel, "\\", "/"))
	if rel == "" {
		return "", errors.New("dir is required")
	}
	if strings.HasPrefix(rel, "/") || filepath.IsAbs(rel) {
		return "", fmt.Errorf("dir %q must be relative", rel)
	}
	clean := filepath.ToSlash(filepath.Clean(rel))
	if clean == "." || clean == "" || strings.HasPrefix(clean, "../") || clean == ".." {
		return "", fmt.Errorf("dir %q escapes the folder", rel)
	}
	if clean != RootPrefix && !strings.HasPrefix(clean, RootPrefix+"/") {
		return "", fmt.Errorf("dir %q must live under %s/", rel, RootPrefix)
	}
	abs := filepath.Join(root, filepath.FromSlash(clean))
	// Belt and braces: the joined path must still be inside the root.
	base := filepath.Clean(root)
	if abs != base && !strings.HasPrefix(abs, base+string(filepath.Separator)) {
		return "", fmt.Errorf("dir %q escapes the folder root", rel)
	}
	return abs, nil
}

// fill writes a deterministic pattern for (seed, index) into buf.
func fill(buf []byte, seed int64, index int) {
	x := uint64(seed)*0x9E3779B97F4A7C15 + uint64(index+1)*0xBF58476D1CE4E5B9 + 0xD1B54A32D192ED03
	for i := 0; i < len(buf); i += 8 {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		for j := 0; j < 8 && i+j < len(buf); j++ {
			buf[i+j] = byte(x >> (8 * j))
		}
	}
}

// mtimeFor returns the deterministic mtime for file index within a corpus.
func mtimeFor(spec Spec, index int) time.Time {
	gap := spec.GapSeconds
	if gap <= 0 {
		gap = 1
	}
	if spec.MTime > 0 {
		return time.Unix(spec.MTime+int64(index*gap), 0)
	}
	// 2026-01-01T00:00:00Z + index*gap: identical on every node.
	return time.Unix(1767225600+int64(index*gap), 0)
}

func createFiles(root string, spec Spec) (int, int64, error) {
	dir, err := resolve(root, spec.Dir)
	if err != nil {
		return 0, 0, err
	}
	if spec.Files <= 0 || spec.Size <= 0 {
		return 0, 0, errors.New("files and size must be > 0")
	}
	if spec.Size > 1<<30 {
		return 0, 0, errors.New("size must be <= 1 GiB")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return 0, 0, err
	}
	buf := make([]byte, spec.Size)
	created := 0
	var bytes int64
	for i := 0; i < spec.Files; i++ {
		index := spec.StartAt + i
		name := filepath.Join(dir, fmt.Sprintf("f%07d.bin", index))
		fill(buf, spec.Seed, index)
		f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			return created, bytes, err
		}
		if _, err := f.Write(buf); err != nil {
			_ = f.Close()
			return created, bytes, err
		}
		if err := f.Close(); err != nil {
			return created, bytes, err
		}
		mt := mtimeFor(spec, index)
		if err := os.Chtimes(name, mt, mt); err != nil {
			return created, bytes, err
		}
		created++
		bytes += spec.Size
	}
	return created, bytes, nil
}

func modifyFiles(root string, spec Spec) (int, int64, error) {
	dir, err := resolve(root, spec.Dir)
	if err != nil {
		return 0, 0, err
	}
	if spec.Files <= 0 {
		return 0, 0, errors.New("files must be > 0")
	}
	changed := 0
	var bytes int64
	for i := 0; i < spec.Files; i++ {
		index := spec.StartAt + i
		name := filepath.Join(dir, fmt.Sprintf("f%07d.bin", index))
		st, err := os.Stat(name)
		if err != nil {
			return changed, bytes, fmt.Errorf("modify %s: %w", name, err)
		}
		buf := make([]byte, st.Size())
		// A different seed AND a newer mtime: this is "a newer version of the
		// same file", which must win over the peer's copy.
		fill(buf, spec.Seed+1, index)
		if err := os.WriteFile(name, buf, 0o644); err != nil {
			return changed, bytes, err
		}
		mt := mtimeFor(spec, index).Add(48 * time.Hour)
		if err := os.Chtimes(name, mt, mt); err != nil {
			return changed, bytes, err
		}
		changed++
		bytes += st.Size()
	}
	return changed, bytes, nil
}

func renamePath(root, from, to string) error {
	src, err := resolve(root, from)
	if err != nil {
		return err
	}
	dst, err := resolve(root, to)
	if err != nil {
		return err
	}
	if _, err := os.Stat(src); err != nil {
		return fmt.Errorf("rename source %s: %w", from, err)
	}
	if _, err := os.Stat(dst); err == nil {
		return fmt.Errorf("rename target %s already exists", to)
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	return os.Rename(src, dst)
}

func cleanup(root, dir string) (int, error) {
	abs, err := resolve(root, dir)
	if err != nil {
		return 0, err
	}
	removed := 0
	_ = filepath.WalkDir(abs, func(_ string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if !d.IsDir() {
			removed++
		}
		return nil
	})
	if err := os.RemoveAll(abs); err != nil {
		return 0, err
	}
	return removed, nil
}
