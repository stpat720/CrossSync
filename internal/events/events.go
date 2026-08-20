// Package events implements the durable, append-only event store: every
// condition (conflict, skipped-by-rule, versioned, applied change, peer
// state, error) is a permanent, queryable record. Acknowledging an event
// only records who/when; the record stays, and a still-persistent condition
// re-opens it on the next occurrence — nothing is ever dismissed forever.
package events

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// Category classifies what happened.
type Category string

const (
	CatApplied   Category = "applied"   // a change was applied locally
	CatConflict  Category = "conflict"  // a conflict copy was created
	CatVersioned Category = "versioned" // an old version was archived
	CatSkipped   Category = "skipped"   // ignored by a rule
	CatUnsynced  Category = "unsynced"  // left unsynced (waiting/error)
	CatError     Category = "error"
	CatWarning   Category = "warning"
	CatPeer      Category = "peer"   // peer connectivity
	CatSystem    Category = "system" // daemon start/stop
)

// Severity of an event.
type Severity int

const (
	SevInfo Severity = iota
	SevWarn
	SevError
)

// Resolution state of an event.
type Resolution int

const (
	ResOpen         Resolution = iota // needs attention
	ResAcknowledged                   // someone acknowledged it (record persists)
	ResResolved                       // condition no longer present
	ResAutoResolved                   // resolved programmatically
)

// Event is one immutable record in the store.
type Event struct {
	ID         int64
	TS         time.Time
	Folder     string
	Path       string
	Category   Category
	Severity   Severity
	Reason     string
	Resolution Resolution
	AckBy      string
	AckTS      *time.Time
	Linked     string // optional linked state, e.g. the matching ignore rule
	PeerID     uint64 // device that caused the event (0 = this device)
}

// Filter selects events to return. Zero values are wildcards.
type Filter struct {
	Folder      string
	Path        string
	Category    Category
	MinSeverity *Severity
	OpenOnly    bool // only open events
	Limit       int  // 0 = no limit
	AfterID     int64
	Since       int64 // only events at/after this unix time (0 = all)
	PeerID      uint64
	PeerName    string
	Self        bool // only events caused by THIS device (peer_id=0)
}

// Store is a SQLite-backed append-only event store.
type Store struct {
	mu sync.Mutex
	db *sql.DB
}

const schema = `
CREATE TABLE IF NOT EXISTS events (
	id         INTEGER PRIMARY KEY AUTOINCREMENT,
	ts         INTEGER NOT NULL,
	ts_ns      INTEGER NOT NULL,
	folder     TEXT NOT NULL DEFAULT '',
	path       TEXT NOT NULL DEFAULT '',
	category   TEXT NOT NULL DEFAULT '',
	severity   INTEGER NOT NULL DEFAULT 0,
	reason     TEXT NOT NULL DEFAULT '',
	resolution INTEGER NOT NULL DEFAULT 0,
	ack_by     TEXT NOT NULL DEFAULT '',
	ack_ts     INTEGER,
	linked     TEXT NOT NULL DEFAULT '',
	peer_id    INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);
CREATE INDEX IF NOT EXISTS idx_events_category ON events(category);
CREATE INDEX IF NOT EXISTS idx_events_resolution ON events(resolution);
CREATE INDEX IF NOT EXISTS idx_events_folder_path ON events(folder, path);
`

// Open opens (creating if needed) the event store at path.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	if err := s.init(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) init() error {
	for _, pragma := range []string{
		`PRAGMA journal_mode=WAL`,
		`PRAGMA synchronous=FULL`,
		`PRAGMA busy_timeout=10000`,
	} {
		if _, err := s.db.Exec(pragma); err != nil {
			return fmt.Errorf("events: %s: %w", pragma, err)
		}
	}
	if _, err := s.db.Exec(schema); err != nil {
		return fmt.Errorf("events: schema: %w", err)
	}
	// Lightweight migration for stores created before the peer_id column.
	rows, err := s.db.Query(`PRAGMA table_info(events)`)
	if err != nil {
		return err
	}
	hasPeer := false
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull int
		var dflt any
		var pk int
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			rows.Close()
			return err
		}
		if name == "peer_id" {
			hasPeer = true
		}
	}
	rows.Close()
	if !hasPeer {
		if _, err := s.db.Exec(`ALTER TABLE events ADD COLUMN peer_id INTEGER NOT NULL DEFAULT 0`); err != nil {
			return err
		}
	}
	// Index on peer_id must be created only after the column exists (fresh
	// stores get it via the schema above; migrated stores get it here).
	if _, err := s.db.Exec(`CREATE INDEX IF NOT EXISTS idx_events_peer ON events(peer_id)`); err != nil {
		return fmt.Errorf("events: idx_events_peer: %w", err)
	}
	return nil
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

// Record appends one event and returns its id. If an acknowledged event
// already exists for the same (folder, path, category), it is re-opened
// first: a persistent condition cannot be permanently dismissed.
func (s *Store) Record(e *Event) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.db.Exec(
		`UPDATE events SET resolution=0, ack_by='', ack_ts=NULL
		 WHERE folder=? AND path=? AND category=? AND resolution=1`,
		e.Folder, e.Path, string(e.Category)); err != nil {
		return 0, err
	}
	var ackTS any
	if e.AckTS != nil {
		ackTS = e.AckTS.Unix()
	}
	res, err := s.db.Exec(
		`INSERT INTO events(ts,ts_ns,folder,path,category,severity,reason,resolution,ack_by,ack_ts,linked,peer_id)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		e.TS.Unix(), e.TS.Nanosecond(), e.Folder, e.Path, string(e.Category),
		int(e.Severity), e.Reason, int(e.Resolution), e.AckBy, ackTS, e.Linked, int64(e.PeerID))
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// Query returns events matching the filter, newest first.
func (s *Store) Query(f Filter) ([]Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := `SELECT id,ts,ts_ns,folder,path,category,severity,reason,resolution,ack_by,ack_ts,linked,peer_id FROM events WHERE 1=1`
	var args []any
	if f.Folder != "" {
		q += ` AND folder=?`
		args = append(args, f.Folder)
	}
	if f.Path != "" {
		q += ` AND path=?`
		args = append(args, f.Path)
	}
	if f.Category != "" {
		q += ` AND category=?`
		args = append(args, string(f.Category))
	}
	if f.MinSeverity != nil {
		q += ` AND severity>=?`
		args = append(args, int(*f.MinSeverity))
	}
	if f.OpenOnly {
		// "needing attention" = open warn+ events, matching CountOpen and
		// the red badge. The limit is intentionally NOT applied: attention
		// must always be complete so an old open condition can never hide
		// from the attention view.
		q += ` AND resolution=0 AND severity>=?`
		args = append(args, int(SevWarn))
	}
	if f.AfterID > 0 {
		q += ` AND id>?`
		args = append(args, f.AfterID)
	}
	if f.Since > 0 {
		q += ` AND ts>=?`
		args = append(args, f.Since)
	}
	if f.Self {
		// Local actions only: peer_id=0 means "this device caused it" (e.g.
		// local deletes, pulls we initiated). Peer-connectivity events (the
		// 'peers' folder) are about REMOTE devices and are excluded — they
		// still show under the peer they concern.
		q += ` AND peer_id=0 AND folder<>'peers'`
	} else if f.PeerID > 0 {
		// Events caused BY this peer, plus the peer-connectivity events for
		// that peer (which are stored under folder='peers', path=peer name).
		// int64() stores ids above MaxInt64 (high bit set) as signed
		// two's-complement, which SQLite/database-sql accept; read back as
		// uint64 below.
		q += ` AND (peer_id=? OR (folder='peers' AND path=?))`
		args = append(args, int64(f.PeerID), f.PeerName)
	}
	q += ` ORDER BY id DESC`
	// Attention queries (open=1) are never limited: the attention view must
	// always be complete so an old open condition can never hide from it.
	if f.Limit > 0 && !f.OpenOnly {
		q += ` LIMIT ?`
		args = append(args, f.Limit)
	}
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Event
	for rows.Next() {
		var e Event
		var ts, tsNs, peerID int64
		var ackTS sql.NullInt64
		if err := rows.Scan(&e.ID, &ts, &tsNs, &e.Folder, &e.Path, &e.Category,
			&e.Severity, &e.Reason, &e.Resolution, &e.AckBy, &ackTS, &e.Linked, &peerID); err != nil {
			return nil, err
		}
		e.PeerID = uint64(peerID) // back from two's-complement to the real id
		e.TS = time.Unix(ts, tsNs)
		if ackTS.Valid {
			t := time.Unix(ackTS.Int64, 0)
			e.AckTS = &t
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// Acknowledge marks an event acknowledged by who (with a timestamp). The
// record is not deleted; it re-opens if the condition persists.
func (s *Store) Acknowledge(id int64, by string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(
		`UPDATE events SET resolution=1, ack_by=?, ack_ts=? WHERE id=?`,
		by, time.Now().Unix(), id)
	return err
}

// AcknowledgeCondition acknowledges every OPEN occurrence of a condition
// (folder+path+category) at once. Used by the grouped "needs attention"
// view so one problem is acked as one unit.
func (s *Store) AcknowledgeCondition(folder, path string, cat Category, by string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(
		`UPDATE events SET resolution=1, ack_by=?, ack_ts=? WHERE resolution=0 AND folder=? AND path=? AND category=?`,
		by, time.Now().Unix(), folder, path, string(cat))
	return err
}

// ReopenMatching re-opens acknowledged events matching folder+path+category.
// Used when a condition is observed to still be present.
func (s *Store) ReopenMatching(folder, path string, cat Category) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(
		`UPDATE events SET resolution=0, ack_by='', ack_ts=NULL
		 WHERE folder=? AND path=? AND category=? AND resolution=1`,
		folder, path, string(cat))
	return err
}

// Resolve marks an event resolved (condition no longer present).
func (s *Store) Resolve(id int64, by string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(
		`UPDATE events SET resolution=2, ack_by=?, ack_ts=? WHERE id=?`,
		by, time.Now().Unix(), id)
	return err
}

// ResolveCondition resolves every open occurrence of a condition (all open
// events for folder+path+category). Use this when the underlying condition
// is gone, so a persistent problem cannot leave stale open records. Only
// warn+ (attention) events are touched — routine info events of the same
// condition stay neutral history.
func (s *Store) ResolveCondition(folder, path string, cat Category, by string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(
		`UPDATE events SET resolution=2, ack_by=?, ack_ts=? WHERE resolution=0 AND folder=? AND path=? AND category=? AND severity>=?`,
		by, time.Now().Unix(), folder, path, string(cat), int(SevWarn))
	return err
}

// ResolveAutoCondition is ResolveCondition but marks events as
// auto-resolved (resolution=3): the condition cleared itself programmatically
// rather than by user action. Used e.g. when a peer comes back online, so
// the "peer offline" attention notes stay in history as auto-resolved
// instead of needing a manual dismiss. Only warn+ events are touched.
func (s *Store) ResolveAutoCondition(folder, path string, cat Category, by string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(
		`UPDATE events SET resolution=3, ack_by=?, ack_ts=? WHERE resolution=0 AND folder=? AND path=? AND category=? AND severity>=?`,
		by, time.Now().Unix(), folder, path, string(cat), int(SevWarn))
	return err
}

// CountOpen returns the number of distinct conditions needing attention:
// open events of at least warning severity, deduplicated by (folder, path,
// category) so one persistent problem counts once no matter how many
// occurrences are recorded.
func (s *Store) CountOpen() (int64, error) {
	return s.countOpen("")
}

// CountOpenFolder is CountOpen restricted to one folder.
func (s *Store) CountOpenFolder(folder string) (int64, error) {
	return s.countOpen(folder)
}

func (s *Store) countOpen(folder string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int64
	q := `SELECT COUNT(*) FROM (SELECT 1 FROM events
		 WHERE resolution=0 AND severity>=?`
	args := []any{int(SevWarn)}
	if folder != "" {
		q += ` AND folder=?`
		args = append(args, folder)
	}
	q += ` GROUP BY folder, path, category)`
	err := s.db.QueryRow(q, args...).Scan(&n)
	return n, err
}

// OldestOpenWarn returns the timestamp of the oldest open warning/error
// event (the "needs attention" set). It is used as the smart archive
// cutoff: nothing that still needs attention is ever archived by default.
func (s *Store) OldestOpenWarn() (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ts int64
	err := s.db.QueryRow(
		`SELECT MIN(ts) FROM events WHERE resolution=0 AND severity>=?`,
		int(SevWarn)).Scan(&ts)
	if err != nil || ts == 0 {
		return time.Time{}, false
	}
	return time.Unix(ts, 0), true
}

// CountArchive returns how many events are older than cutoff, and how many
// of those are open attention events (open warn+). Used for the archive
// preview so the UI can warn before permanently deleting anything.
func (s *Store) CountArchive(cutoff time.Time) (older, openOlder int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cu := cutoff.Unix()
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM events WHERE ts<?`, cu).Scan(&older); err != nil {
		return 0, 0, err
	}
	if err := s.db.QueryRow(
		`SELECT COUNT(*) FROM events WHERE ts<? AND resolution=0 AND severity>=?`,
		cu, int(SevWarn)).Scan(&openOlder); err != nil {
		return 0, 0, err
	}
	return older, openOlder, nil
}

// Archive permanently deletes events older than cutoff. Open attention
// events (unviewed warning/error conditions) are preserved unless
// includeOpen is true — the caller must surface a warning in that case,
// because it silently drops badge items. It returns the number of events
// deleted, how many of those were open attention events, and how many open
// attention events were kept.
func (s *Store) Archive(cutoff time.Time, includeOpen bool) (deleted, deletedOpen, keptOpen int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cu := cutoff.Unix()
	// How many open attention events sit below the cutoff (relevant for the
	// warning when includeOpen=true, and for keptOpen otherwise).
	var openOlder int
	if err := s.db.QueryRow(
		`SELECT COUNT(*) FROM events WHERE ts<? AND resolution=0 AND severity>=?`,
		cu, int(SevWarn)).Scan(&openOlder); err != nil {
		return 0, 0, 0, err
	}
	if includeOpen {
		res, err := s.db.Exec(`DELETE FROM events WHERE ts<?`, cu)
		if err != nil {
			return 0, 0, 0, err
		}
		n, _ := res.RowsAffected()
		return int(n), openOlder, 0, nil
	}
	res, err := s.db.Exec(
		`DELETE FROM events WHERE ts<? AND NOT (resolution=0 AND severity>=?)`,
		cu, int(SevWarn))
	if err != nil {
		return 0, 0, 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), 0, openOlder, nil
}
