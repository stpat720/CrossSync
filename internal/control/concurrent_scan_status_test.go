package control_test

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"crosssync/internal/control"
)

// TestStatusResponsiveWithWalkAndScopedPassRunning pins the invariant that made
// /api/status cheap: the request path does no scanning work of its own. It is
// re-asserted here because a folder can now have TWO passes live at once (a full
// walk and a watcher-driven scoped pass), and the honest status has to show both
// without either pass holding a lock the request needs.
func TestStatusResponsiveWithWalkAndScopedPassRunning(t *testing.T) {
	n, root := testNode(t)
	f := n.Folders["data"]
	if err := os.MkdirAll(filepath.Join(root, "x"), 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(rel, body string) {
		if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(rel)), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("x/slow.bin", strings.Repeat("s", 4096))
	write("x/other.txt", "other")
	if _, err := n.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	write("x/slow.bin", strings.Repeat("S", 5000)) // a modification, so the walk hashes it

	// Park BOTH passes in their hash phases: one on the walk's file, one on the
	// scoped pass's new file.
	parks := map[string]chan struct{}{"x/slow.bin": make(chan struct{}), "x/new.txt": make(chan struct{})}
	entered := map[string]chan struct{}{"x/slow.bin": make(chan struct{}), "x/new.txt": make(chan struct{})}
	var mu sync.Mutex
	once := map[string]*sync.Once{"x/slow.bin": {}, "x/new.txt": {}}
	f.Scanner.SetHashHook(func(name string) {
		mu.Lock()
		gate, ok := parks[name]
		in, inok := entered[name]
		o := once[name]
		mu.Unlock()
		if !ok || !inok {
			return
		}
		o.Do(func() { close(in) })
		<-gate
	})
	t.Cleanup(func() { f.Scanner.SetHashHook(nil) })
	release := func(name string) {
		mu.Lock()
		g := parks[name]
		mu.Unlock()
		if g != nil {
			select {
			case <-g:
			default:
				close(g)
			}
		}
	}
	defer release("x/slow.bin")
	defer release("x/new.txt")

	walkDone := make(chan error, 1)
	go func() {
		_, err := n.ScanFolder("data")
		walkDone <- err
	}()
	select {
	case <-entered["x/slow.bin"]:
	case <-time.After(5 * time.Second):
		t.Fatal("the walk never reached its hash phase")
	}

	write("x/new.txt", "created while the walk runs")
	scopedDone := make(chan error, 1)
	go func() {
		_, err := n.ScanFolderPaths("data", []string{"x/new.txt"})
		scopedDone <- err
	}()
	select {
	case <-entered["x/new.txt"]:
	case <-time.After(5 * time.Second):
		t.Fatal("the scoped pass never reached its hash phase beside the walk")
	}

	svc := control.New(n, "test")
	var sawWalk, sawScoped bool
	for i := 0; i < 10; i++ {
		start := time.Now()
		st := svc.Status()
		if d := time.Since(start); d > 250*time.Millisecond {
			t.Fatalf("/api/status took %s with both passes running (limit 250ms): the request path must not touch the scanner or the disk", d)
		}
		for _, sc := range st.Scans {
			if sc.Folder != "data" {
				continue
			}
			sawWalk = sawWalk || sc.Running
			sawScoped = sawScoped || sc.Scoped.Running
			if sc.Scoped.Phase == "" {
				t.Fatalf("scans[] entry for data does not carry the scoped pass at all: %+v", sc)
			}
		}
	}
	if !sawWalk {
		t.Fatal("/api/status did not report the walk running while it was parked")
	}
	if !sawScoped {
		t.Fatal("/api/status did not report the scoped pass running while a walk was also running: both passes must be visible")
	}

	release("x/slow.bin")
	release("x/new.txt")
	if err := <-walkDone; err != nil {
		t.Fatalf("walk: %v", err)
	}
	if err := <-scopedDone; err != nil {
		t.Fatalf("scoped pass: %v", err)
	}
}
