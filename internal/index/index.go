// Package index implements the per-folder metadata index, stored in a
// dedicated SQLite database per synced folder (WAL mode). The filesystem
// is always the source of truth; the index is a rebuildable cache.
package index

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	_ "modernc.org/sqlite"

	"crosssync/internal/hash"
	"crosssync/internal/version"
)

// Type of a filesystem entry.
type Type int

const (
	TypeFile Type = iota
	TypeDirectory
	TypeSymlink
)

// FileInfo is the indexed metadata for one path in a folder.
type FileInfo struct {
	Name       string          // relative path, slash-separated
	Size       int64           // 0 for directories/symlinks
	ModifiedS  int64           // unix seconds
	ModifiedNs int32           // nanosecond remainder
	Mode       uint32          // permission bits
	Type       Type            // file/directory/symlink
	Deleted    bool            // tombstone
	Invalid    bool            // temporarily unavailable for sync
	Version    version.Vector  // version vector
	Sequence   int64           // local monotonic sequence
	BlockSize  int32           // block size used for Blocks (0 if none)
	Blocks     [][]byte        // per-block SHA-256 hashes
}

// Clone returns a deep copy of the FileInfo.
func (fi *FileInfo) Clone() *FileInfo {
	out := *fi
	out.Version = fi.Version.Clone()
	out.Blocks = make([][]byte, len(fi.Blocks))
	for i, b := range fi.Blocks {
		blk := make([]byte, len(b))
		copy(blk, b)
		out.Blocks[i] = blk
	}
	return &out
}

// Index is a single-folder index backed by SQLite.
type Index struct {
	mu       sync.Mutex // serializes writes; reads are safe via SQLite
	db       *sql.DB
	path     string
	folderID string
	nextSeq  int64
	indexID  string // cached; generated and persisted on first use
}

const schema = `
CREATE TABLE IF NOT EXISTS files (
	name        TEXT PRIMARY KEY,
	size        INTEGER NOT NULL DEFAULT 0,
	modified_s  INTEGER NOT NULL DEFAULT 0,
	modified_ns INTEGER NOT NULL DEFAULT 0,
	mode        INTEGER NOT NULL DEFAULT 0,
	type        INTEGER NOT NULL DEFAULT 0,
	deleted     INTEGER NOT NULL DEFAULT 0,
	invalid     INTEGER NOT NULL DEFAULT 0,
	version     TEXT NOT NULL DEFAULT '{}',
	sequence    INTEGER NOT NULL,
	block_size  INTEGER NOT NULL DEFAULT 0,
	blocks      BLOB NOT NULL DEFAULT x''
);
CREATE INDEX IF NOT EXISTS idx_files_sequence ON files(sequence);
CREATE TABLE IF NOT EXISTS meta (
	key   TEXT PRIMARY KEY,
	value TEXT NOT NULL
);
`

// Open opens (creating if needed) the per-folder index database at path.
// The parent directory is created if it does not exist.
func Open(path, folderID string) (*Index, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	ix := &Index{db: db, path: path, folderID: folderID}
	if err := ix.init(); err != nil {
		db.Close()
		return nil, err
	}
	return ix, nil
}

func (ix *Index) init() error {
	if _, err := ix.db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		return fmt.Errorf("index: enable WAL: %w", err)
	}
	if _, err := ix.db.Exec(`PRAGMA synchronous=FULL`); err != nil {
		return fmt.Errorf("index: synchronous: %w", err)
	}
	if _, err := ix.db.Exec(`PRAGMA busy_timeout=10000`); err != nil {
		return fmt.Errorf("index: busy timeout: %w", err)
	}
	if _, err := ix.db.Exec(schema); err != nil {
		return fmt.Errorf("index: schema: %w", err)
	}
	// Determine next sequence from the max stored sequence.
	var max int64
	if err := ix.db.QueryRow(`SELECT COALESCE(MAX(sequence),0) FROM files`).Scan(&max); err != nil {
		return fmt.Errorf("index: max sequence: %w", err)
	}
	ix.nextSeq = max + 1
	return nil
}

// Close closes the database.
func (ix *Index) Close() error { return ix.db.Close() }

// Path returns the database file path.
func (ix *Index) Path() string { return ix.path }

// FolderID returns the folder id this index belongs to.
func (ix *Index) FolderID() string { return ix.folderID }
// Stats is a single-pass aggregate of the index. For very large folders
// this is far cheaper than iterating every row in Go (COUNT/SUM run inside
// SQLite).
type Stats struct {
	Files         int
	Tombstones    int
	ConflictFiles int
	Size          int64
	ConflictBytes int64
}

// Stats computes the aggregate counts/sizes. conflictSubstr is the
// substring that marks a conflict copy (e.g. the .sync-conflict- suffix);
// only live (non-deleted) names containing it count as conflicts.
func (ix *Index) Stats(conflictSubstr string) (Stats, error) {
	var st Stats
	like := "%" + conflictSubstr + "%"
	err := ix.db.QueryRow(`SELECT
		COUNT(*),
		COALESCE(SUM(size),0),
		SUM(CASE WHEN deleted=1 THEN 1 ELSE 0 END),
		SUM(CASE WHEN deleted=0 AND name LIKE ? THEN 1 ELSE 0 END),
		COALESCE(SUM(CASE WHEN deleted=0 AND name LIKE ? THEN size ELSE 0 END),0)
		FROM files`, like, like).
		Scan(&st.Files, &st.Size, &st.Tombstones, &st.ConflictFiles, &st.ConflictBytes)
	return st, err
}
// IntegrityCheck runs SQLite's integrity check (full page verification).
// It is comparatively slow on large databases, so prefer QuickCheck for a
// fast startup check and run this when corruption is suspected.
func (ix *Index) IntegrityCheck() error {
	rows, err := ix.db.Query(`PRAGMA integrity_check`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var result string
		if err := rows.Scan(&result); err != nil {
			return err
		}
		if result != "ok" {
			return fmt.Errorf("index integrity: %s", result)
		}
	}
	return rows.Err()
}

// QuickCheck runs SQLite's fast structural check. It only verifies the
// on-disk structure (not every page checksum), so it is cheap enough to run
// at daemon startup to catch unreadable or truncated index files.
func (ix *Index) QuickCheck() error {
	rows, err := ix.db.Query(`PRAGMA quick_check`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var result string
		if err := rows.Scan(&result); err != nil {
			return err
		}
		if result != "ok" {
			return fmt.Errorf("index quick check: %s", result)
		}
	}
	return rows.Err()
}

// NextSequence reserves the next monotonic sequence number.
func (ix *Index) NextSequence() int64 {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	return ix.allocSequence()
}

func (ix *Index) allocSequence() int64 {
	seq := ix.nextSeq
	ix.nextSeq++
	return seq
}

// MaxSeq returns the highest sequence assigned so far (0 when empty).
func (ix *Index) MaxSeq() int64 {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	return ix.nextSeq - 1
}

// IndexID returns this index's generation identifier, generating and
// persisting a random one on first use. The ID changes if the database is
// ever recreated from scratch, which forces a full re-exchange.
func (ix *Index) IndexID() (string, error) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if ix.indexID != "" {
		return ix.indexID, nil
	}
	var id string
	err := ix.db.QueryRow(`SELECT value FROM meta WHERE key='index_id'`).Scan(&id)
	if err == nil && id != "" {
		ix.indexID = id
		return id, nil
	}
	if err != nil && err != sql.ErrNoRows {
		return "", err
	}
	randBytes := make([]byte, 16)
	if _, err := rand.Read(randBytes); err != nil {
		return "", err
	}
	id = hex.EncodeToString(randBytes)
	if _, err := ix.db.Exec(`INSERT INTO meta(key,value) VALUES('index_id',?)`, id); err != nil {
		return "", err
	}
	ix.indexID = id
	return id, nil
}

// Put upserts a FileInfo. If fi.Sequence is 0 a new sequence is allocated.
func (ix *Index) Put(fi *FileInfo) error {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if fi.Sequence == 0 {
		fi.Sequence = ix.allocSequence()
	}
	ver, err := json.Marshal(fi.Version)
	if err != nil {
		return err
	}
	_, err = ix.db.Exec(`
		INSERT INTO files(name,size,modified_s,modified_ns,mode,type,deleted,invalid,version,sequence,block_size,blocks)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(name) DO UPDATE SET
			size=excluded.size, modified_s=excluded.modified_s,
			modified_ns=excluded.modified_ns, mode=excluded.mode,
			type=excluded.type, deleted=excluded.deleted,
			invalid=excluded.invalid, version=excluded.version,
			sequence=excluded.sequence, block_size=excluded.block_size,
			blocks=excluded.blocks`,
		fi.Name, fi.Size, fi.ModifiedS, fi.ModifiedNs, fi.Mode,
		int(fi.Type), boolInt(fi.Deleted), boolInt(fi.Invalid),
		string(ver), fi.Sequence, fi.BlockSize, hash.Flatten(fi.Blocks))
	return err
}

// Delete removes an entry from the index.
func (ix *Index) Delete(name string) error {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	_, err := ix.db.Exec(`DELETE FROM files WHERE name=?`, name)
	return err
}

// Get returns the indexed entry for name, or (nil,false,nil) if absent.
func (ix *Index) Get(name string) (*FileInfo, bool, error) {
	row := ix.db.QueryRow(`SELECT name,size,modified_s,modified_ns,mode,type,deleted,invalid,version,sequence,block_size,blocks FROM files WHERE name=?`, name)
	fi := &FileInfo{}
	var ver string
	var deleted, invalid int
	var blocks []byte
	if err := row.Scan(&fi.Name, &fi.Size, &fi.ModifiedS, &fi.ModifiedNs, &fi.Mode,
		&fi.Type, &deleted, &invalid, &ver, &fi.Sequence, &fi.BlockSize, &blocks); err != nil {
		if err == sql.ErrNoRows {
			return nil, false, nil
		}
		return nil, false, err
	}
	fi.Deleted = deleted != 0
	fi.Invalid = invalid != 0
	if err := json.Unmarshal([]byte(ver), &fi.Version); err != nil {
		return nil, false, err
	}
	fi.Blocks = hash.Unflatten(blocks)
	return fi, true, nil
}

// List iterates over all non-deleted entries in the index.
func (ix *Index) List(fn func(*FileInfo) error) error {
	rows, err := ix.db.Query(`SELECT name,size,modified_s,modified_ns,mode,type,deleted,invalid,version,sequence,block_size,blocks FROM files WHERE deleted=0`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		fi := &FileInfo{}
		if err := scanRow(rows, fi); err != nil {
			return err
		}
		if err := fn(fi); err != nil {
			return err
		}
	}
	return rows.Err()
}

// ListAll iterates over every entry in the index, including tombstones
// (deleted entries). Used for full index exchange and propagation of
// deletions.
func (ix *Index) ListAll(fn func(*FileInfo) error) error {
	rows, err := ix.db.Query(`SELECT name,size,modified_s,modified_ns,mode,type,deleted,invalid,version,sequence,block_size,blocks FROM files`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		fi := &FileInfo{}
		if err := scanRow(rows, fi); err != nil {
			return err
		}
		if err := fn(fi); err != nil {
			return err
		}
	}
	return rows.Err()
}

// ListAfter iterates over every entry with sequence greater than seq,
// including tombstones. Used for delta index exchange: entries a peer has
// not seen since it last acknowledged maxSeq.
func (ix *Index) ListAfter(seq int64, fn func(*FileInfo) error) error {
	rows, err := ix.db.Query(`SELECT name,size,modified_s,modified_ns,mode,type,deleted,invalid,version,sequence,block_size,blocks FROM files WHERE sequence > ?`, seq)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		fi := &FileInfo{}
		if err := scanRow(rows, fi); err != nil {
			return err
		}
		if err := fn(fi); err != nil {
			return err
		}
	}
	return rows.Err()
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanRow(row rowScanner, fi *FileInfo) error {
	var ver string
	var deleted, invalid int
	var blocks []byte
	if err := row.Scan(&fi.Name, &fi.Size, &fi.ModifiedS, &fi.ModifiedNs, &fi.Mode,
		&fi.Type, &deleted, &invalid, &ver, &fi.Sequence, &fi.BlockSize, &blocks); err != nil {
		return err
	}
	fi.Deleted = deleted != 0
	fi.Invalid = invalid != 0
	if err := json.Unmarshal([]byte(ver), &fi.Version); err != nil {
		return err
	}
	fi.Blocks = hash.Unflatten(blocks)
	return nil
}

// SetMeta stores a metadata key/value in the index.
func (ix *Index) SetMeta(key, value string) error {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	_, err := ix.db.Exec(`
		INSERT INTO meta(key,value) VALUES(?,?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, value)
	return err
}

// GetMeta returns a metadata value, or (value,false,nil) if absent.
func (ix *Index) GetMeta(key string) (string, bool, error) {
	var value string
	err := ix.db.QueryRow(`SELECT value FROM meta WHERE key=?`, key).Scan(&value)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return value, true, nil
}

// peerIndexState is the persisted record of what a peer was last told
// about this folder's index.
type peerIndexState struct {
	IndexID string `json:"index_id"`
	MaxSeq  int64  `json:"max_seq"`
}

// GetPeerIndex returns the index state last sent to peerID, or ok=false if
// the peer has never received this folder's index from us.
func (ix *Index) GetPeerIndex(peerID uint64) (indexID string, maxSeq int64, ok bool, err error) {
	raw, found, err := ix.GetMeta(peerIndexKey(peerID))
	if err != nil || !found {
		return "", 0, false, err
	}
	var st peerIndexState
	if err := json.Unmarshal([]byte(raw), &st); err != nil {
		return "", 0, false, err
	}
	return st.IndexID, st.MaxSeq, true, nil
}

// SetPeerIndex records the index state sent to peerID. This must only be
// called after a session in which the peer actually received everything up
// to maxSeq; see node.sendIndexes/recordPeerIndex.
func (ix *Index) SetPeerIndex(peerID uint64, indexID string, maxSeq int64) error {
	raw, err := json.Marshal(peerIndexState{IndexID: indexID, MaxSeq: maxSeq})
	if err != nil {
		return err
	}
	return ix.SetMeta(peerIndexKey(peerID), string(raw))
}

func peerIndexKey(peerID uint64) string {
	return "peer_index:" + strconv.FormatUint(peerID, 10)
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
