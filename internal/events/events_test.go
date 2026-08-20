package events

import (
	"path/filepath"
	"testing"
	"time"
)

func openTest(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func record(t *testing.T, s *Store, cat Category, folder, path, reason string) int64 {
	t.Helper()
	id, err := s.Record(&Event{
		TS: time.Unix(1_700_000_000, 0), Folder: folder, Path: path,
		Category: cat, Severity: SevInfo, Reason: reason,
	})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// TestResolveAutoConditionOnlyTouchesAttention verifies that resolving a
// condition (e.g. "peer offline" when the peer comes back) marks the warn+
// attention events auto-resolved but leaves routine info events of the same
// folder/path/category as neutral history — they are never swept.
func TestResolveAutoConditionOnlyTouchesAttention(t *testing.T) {
	s := openTest(t)
	// An open warn attention event + a routine info event, same condition.
	if _, err := s.Record(&Event{TS: time.Now(), Folder: "peers", Path: "nas-02",
		Category: CatPeer, Severity: SevWarn, Reason: "peer offline"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Record(&Event{TS: time.Now(), Folder: "peers", Path: "nas-02",
		Category: CatPeer, Severity: SevInfo, Reason: "peer back online"}); err != nil {
		t.Fatal(err)
	}
	if err := s.ResolveAutoCondition("peers", "nas-02", CatPeer, "system"); err != nil {
		t.Fatal(err)
	}
	evs, err := s.Query(Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 2 {
		t.Fatalf("expected 2 events, got %d", len(evs))
	}
	got := map[Severity]Resolution{}
	for _, e := range evs {
		got[e.Severity] = e.Resolution
	}
	if got[SevWarn] != ResAutoResolved {
		t.Fatalf("warn event resolution = %v, want auto-resolved: %+v", got[SevWarn], evs)
	}
	if got[SevInfo] != ResOpen {
		t.Fatalf("info event was swept: resolution = %v, want open: %+v", got[SevInfo], evs)
	}

	// ResolveCondition has the same guard.
	if err := s.ResolveCondition("peers", "nas-02", CatPeer, "system"); err != nil {
		t.Fatal(err)
	}
	evs, _ = s.Query(Filter{})
	for _, e := range evs {
		if e.Severity == SevInfo && e.Resolution != ResOpen {
			t.Fatalf("ResolveCondition swept an info event: %+v", e)
		}
	}
}

func TestRecordQuery(t *testing.T) {
	s := openTest(t)
	record(t, s, CatConflict, "data", "a.txt", "conflict copy created")
	record(t, s, CatSkipped, "data", "b.tmp", "ignored by rule *.tmp")
	record(t, s, CatSkipped, "data", "c.tmp", "ignored by rule *.tmp")

	all, err := s.Query(Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Fatalf("expected 3 events, got %d", len(all))
	}
	// Newest first.
	if all[0].Path != "c.tmp" {
		t.Fatalf("newest first: got %q", all[0].Path)
	}
	skips, err := s.Query(Filter{Category: CatSkipped})
	if err != nil {
		t.Fatal(err)
	}
	if len(skips) != 2 {
		t.Fatalf("expected 2 skipped events, got %d", len(skips))
	}
	conflicts, err := s.Query(Filter{Category: CatConflict})
	if err != nil {
		t.Fatal(err)
	}
	if len(conflicts) != 1 || conflicts[0].Path != "a.txt" {
		t.Fatalf("unexpected conflicts: %+v", conflicts)
	}
}

func TestAckPersistsAndReopens(t *testing.T) {
	s := openTest(t)
	id := record(t, s, CatSkipped, "data", "b.tmp", "ignored")

	if err := s.Acknowledge(id, "admin"); err != nil {
		t.Fatal(err)
	}
	evs, _ := s.Query(Filter{Path: "b.tmp"})
	if len(evs) != 1 || evs[0].Resolution != ResAcknowledged || evs[0].AckBy != "admin" {
		t.Fatalf("ack not recorded: %+v", evs[0])
	}

	// The condition persists -> recording it again re-opens the ack'd event.
	record(t, s, CatSkipped, "data", "b.tmp", "ignored")
	evs, _ = s.Query(Filter{Path: "b.tmp"})
	if len(evs) != 2 {
		t.Fatalf("expected the new occurrence recorded, got %d events", len(evs))
	}
	// The acknowledged one is open again (never permanently dismissed).
	foundAcked := false
	for _, e := range evs {
		if e.Resolution == ResAcknowledged {
			foundAcked = true
		}
	}
	if foundAcked {
		t.Fatal("acknowledged event should have re-opened once the condition persisted")
	}
	if evs[0].Resolution != ResOpen {
		t.Fatalf("newest occurrence should be open, got %+v", evs[0])
	}
}

func TestCountOpen(t *testing.T) {
	s := openTest(t)
	// Routine info events (skipped by rule) are history, not action items.
	record(t, s, CatSkipped, "data", "b.tmp", "skipped")
	// A warn-severity conflict is an action item.
	id, err := s.Record(&Event{TS: time.Now(), Folder: "data", Path: "a.txt",
		Category: CatConflict, Severity: SevWarn, Reason: "conflict"})
	if err != nil {
		t.Fatal(err)
	}
	n, err := s.CountOpen()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("CountOpen = %d, want 1 (only open warn+ events)", n)
	}
	// Acknowledging clears it from the action list...
	if err := s.Acknowledge(id, "admin"); err != nil {
		t.Fatal(err)
	}
	n, _ = s.CountOpen()
	if n != 0 {
		t.Fatalf("CountOpen after ack = %d, want 0", n)
	}
	// ...but the record persists and re-opens when the condition persists.
	if _, err := s.Record(&Event{TS: time.Now(), Folder: "data", Path: "a.txt",
		Category: CatConflict, Severity: SevWarn, Reason: "conflict"}); err != nil {
		t.Fatal(err)
	}
	n, _ = s.CountOpen()
	if n != 1 {
		t.Fatalf("CountOpen after condition persists = %d, want 1", n)
	}
	// Resolving the condition (all open occurrences) clears it.
	if err := s.ResolveCondition("data", "a.txt", CatConflict, "system"); err != nil {
		t.Fatal(err)
	}
	n, _ = s.CountOpen()
	if n != 0 {
		t.Fatalf("CountOpen after resolve = %d, want 0", n)
	}
}

// TestOpenOnlyMatchesAttention verifies that an open-only query is exactly
// the "needs attention" set: open warn+ events across all history, ignoring
// range and limit. This is what the web UI's attention view renders, and it
// must always agree with the red badge (CountOpen).
func TestOpenOnlyMatchesAttention(t *testing.T) {
	s := openTest(t)
	// Routine info events (open by default but not attention-worthy).
	record(t, s, CatSkipped, "data", "b.tmp", "skipped")
	record(t, s, CatApplied, "data", "c.txt", "synced")
	// Two warn conditions (one with two open occurrences).
	for i := 0; i < 2; i++ {
		if _, err := s.Record(&Event{TS: time.Now(), Folder: "peers", Path: "nas-02",
			Category: CatPeer, Severity: SevWarn, Reason: "peer offline"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Record(&Event{TS: time.Now(), Folder: "data", Path: "a.txt",
		Category: CatConflict, Severity: SevWarn, Reason: "conflict"}); err != nil {
		t.Fatal(err)
	}

	// open=1 with a tiny limit must still return every open warn+ event
	// (the limit is intentionally not applied to attention queries).
	evs, err := s.Query(Filter{OpenOnly: true, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 3 {
		t.Fatalf("open query returned %d events, want 3 (all open warn+, no limit)", len(evs))
	}
	for _, e := range evs {
		if e.Resolution != ResOpen || e.Severity < SevWarn {
			t.Fatalf("open query returned a non-attention event: %+v", e)
		}
	}

	// The attention set must match CountOpen's distinct conditions: 2 here
	// (peers/nas-02/peer and data/a.txt/conflict).
	n, err := s.CountOpen()
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("CountOpen = %d, want 2", n)
	}
	conds := map[string]bool{}
	for _, e := range evs {
		conds[e.Folder+"|"+e.Path+"|"+string(e.Category)] = true
	}
	if len(conds) != int(n) {
		t.Fatalf("open query has %d distinct conditions, CountOpen says %d", len(conds), n)
	}
}

// TestPeerAndSelfFilter verifies that filtering by a peer id returns events
// caused by that peer plus its connectivity events, and that the Self filter
// returns only local (peer_id=0) events.
func TestPeerAndSelfFilter(t *testing.T) {
	s := openTest(t)
	record(t, s, CatApplied, "data", "local.txt", "local delete") // self
	if _, err := s.Record(&Event{TS: time.Now(), Folder: "data", Path: "remote.txt",
		Category: CatApplied, Severity: SevInfo, Reason: "deleted (remote)",
		PeerID: 141095340884106589}); err != nil {
		t.Fatal(err)
	}
	// Peer connectivity event for nas-02 (lives in the 'peers' folder).
	if _, err := s.Record(&Event{TS: time.Now(), Folder: "peers", Path: "nas-02",
		Category: CatPeer, Severity: SevWarn, Reason: "peer offline"}); err != nil {
		t.Fatal(err)
	}

	// Filter by the peer: its attributed events + its connectivity events.
	evs, err := s.Query(Filter{PeerID: 141095340884106589, PeerName: "nas-02"})
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 2 {
		t.Fatalf("peer filter returned %d events, want 2 (remote + peer offline)", len(evs))
	}
	for _, e := range evs {
		if e.Folder == "peers" && e.Path != "nas-02" {
			t.Fatalf("peer filter returned unrelated peers event: %+v", e)
		}
		if e.Folder == "data" && e.PeerID != 141095340884106589 {
			t.Fatalf("peer filter returned non-attributed data event: %+v", e)
		}
	}

	// Filter by SELF: only peer_id=0 events, and NOT the connectivity events.
	evs, err = s.Query(Filter{Self: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 1 {
		t.Fatalf("self filter returned %d events, want 1 (local only)", len(evs))
	}
	if evs[0].Path != "local.txt" {
		t.Fatalf("self filter returned wrong event: %+v", evs[0])
	}
}

// TestHighBitPeerID verifies a peer device id with the high bit set (>= 2^63,
// common for cert-derived ids) round-trips through Record/Query. database/sql
// refuses uint64 args above MaxInt64, so we must store them as signed
// two's-complement int64 and convert back on read.
func TestHighBitPeerID(t *testing.T) {
	s := openTest(t)
	const peer = uint64(11535086261826064642) // > math.MaxInt64
	if _, err := s.Record(&Event{TS: time.Now(), Folder: "data", Path: "x.txt",
		Category: CatApplied, Severity: SevInfo, Reason: "deleted (remote)",
		PeerID: peer}); err != nil {
		t.Fatal(err)
	}
	// Read back un-filtered: the id must match exactly.
	evs, err := s.Query(Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 1 || evs[0].PeerID != peer {
		t.Fatalf("high-bit peer id did not round-trip: %+v", evs)
	}
	// Filter by that peer: must find it.
	evs, err = s.Query(Filter{PeerID: peer, PeerName: "nas-01"})
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 1 || evs[0].PeerID != peer {
		t.Fatalf("high-bit peer filter failed: %+v", evs)
	}
}

func TestReopenMatching(t *testing.T) {
	s := openTest(t)
	a := record(t, s, CatSkipped, "data", "x.tmp", "skipped")
	b := record(t, s, CatSkipped, "data", "y.tmp", "skipped")
	_ = s.Acknowledge(a, "u1")
	_ = s.Acknowledge(b, "u2")

	if err := s.ReopenMatching("data", "x.tmp", CatSkipped); err != nil {
		t.Fatal(err)
	}
	evs, _ := s.Query(Filter{Category: CatSkipped})
	for _, e := range evs {
		if e.Path == "x.tmp" && e.Resolution != ResOpen {
			t.Fatalf("x.tmp should be open after ReopenMatching: %+v", e)
		}
		if e.Path == "y.tmp" && e.Resolution != ResAcknowledged {
			t.Fatalf("y.tmp should stay acknowledged: %+v", e)
		}
	}
}

// TestArchiveKeepsOpenAttention verifies the core safety contract of the
// archive feature: with includeOpen=false, events older than the cutoff are
// deleted EXCEPT open attention (warn+) events, which survive regardless of
// age. The counts returned must match what the preview reports.
func TestArchiveKeepsOpenAttention(t *testing.T) {
	s := openTest(t)
	old := time.Unix(1_700_000_000, 0) // ~Nov 2023, well in the past
	now := time.Now()

	// Old events: two info (deletable), one open warn attention (must keep).
	seed := func(ts time.Time, cat Category, sev Severity, res Resolution, path string) {
		t.Helper()
		if _, err := s.Record(&Event{TS: ts, Folder: "data", Path: path,
			Category: cat, Severity: sev, Resolution: res, Reason: "r"}); err != nil {
			t.Fatal(err)
		}
	}
	seed(old, CatApplied, SevInfo, ResAcknowledged, "old-applied.txt")
	seed(old, CatSkipped, SevInfo, ResOpen, "old-skipped.tmp")
	seed(old, CatConflict, SevWarn, ResOpen, "old-conflict.txt")
	// A recent open attention event is newer than the cutoff -> untouched.
	seed(now, CatUnsynced, SevError, ResOpen, "recent-unsynced.txt")
	// A recent info event is newer than the cutoff -> untouched.
	seed(now, CatApplied, SevInfo, ResOpen, "recent-applied.txt")

	// Oldest open warn+ event is old-conflict.txt.
	if ts, ok := s.OldestOpenWarn(); !ok {
		t.Fatal("expected an oldest open warn event")
	} else if ts.Unix() != old.Unix() {
		t.Fatalf("OldestOpenWarn = %d, want %d", ts.Unix(), old.Unix())
	}

	// Preview semantics via CountArchive.
	older, openOlder, err := s.CountArchive(now)
	if err != nil {
		t.Fatal(err)
	}
	if older != 3 {
		t.Fatalf("CountArchive older = %d, want 3", older)
	}
	if openOlder != 1 {
		t.Fatalf("CountArchive openOlder = %d, want 1", openOlder)
	}

	// Archive with includeOpen=false: deletes the 2 info/skipped old events,
	// keeps the 1 open warn old event.
	deleted, deletedOpen, kept, err := s.Archive(now, false)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 2 || deletedOpen != 0 || kept != 1 {
		t.Fatalf("Archive(false) = (%d,%d,%d), want (2,0,1)", deleted, deletedOpen, kept)
	}
	// The open attention event survived; recent events survived.
	evs, _ := s.Query(Filter{})
	if len(evs) != 3 {
		t.Fatalf("after archive: %d events remain, want 3", len(evs))
	}
	for _, e := range evs {
		if e.Path == "old-conflict.txt" || e.Path == "recent-unsynced.txt" || e.Path == "recent-applied.txt" {
			continue
		}
		t.Fatalf("unexpected survivor: %+v", e)
	}

	// Re-run with includeOpen=true over the same cutoff: nothing left older
	// than now, including the old open attention event — and DeletedOpen
	// reports that one dismissed-but-unviewed attention note was lost.
	deleted, deletedOpen, kept, err = s.Archive(now, true)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 || deletedOpen != 1 || kept != 0 {
		t.Fatalf("Archive(true) = (%d,%d,%d), want (1,1,0)", deleted, deletedOpen, kept)
	}
	evs, _ = s.Query(Filter{})
	if len(evs) != 2 { // recent-unsynced + recent-applied
		t.Fatalf("after forced archive: %d events remain, want 2", len(evs))
	}
}

// TestOldestOpenWarnEmpty ensures the smart cutoff returns false when no
// attention events exist, so the caller falls back to "archive everything".
func TestOldestOpenWarnEmpty(t *testing.T) {
	s := openTest(t)
	if _, ok := s.OldestOpenWarn(); ok {
		t.Fatal("expected no oldest open warn event in an empty store")
	}
	record(t, s, CatApplied, "data", "x.txt", "applied") // info only
	if _, ok := s.OldestOpenWarn(); ok {
		t.Fatal("expected no oldest open warn event with only info events")
	}
}

