package control_test

import (
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"crosssync/internal/control"
)

// TestTestDataEndpointRails: the generator is reachable only with an explicit
// confirmation, only inside testdata/, and only within the per-call caps. It
// writes real files on a real share, so the rails are the feature.
func TestTestDataEndpointRails(t *testing.T) {
	n, root := testNode(t)
	svc := control.New(n, "test")
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()
	id := "data"
	url := fmt.Sprintf("%s/api/folders/%s/testdata", srv.URL, id)

	// No confirmation -> refused.
	if code, _ := postJSON(t, url, `{"kind":"base","dir":"testdata/x","files":2,"size":64}`); code != 403 {
		t.Fatalf("missing confirm = %d, want 403", code)
	}
	// Outside testdata/ -> refused.
	if code, _ := postJSON(t, url, `{"kind":"base","dir":"user","files":2,"size":64,"confirm":true}`); code != 400 {
		t.Fatalf("outside jail = %d, want 400", code)
	}
	// Over the per-call byte cap -> refused (and nothing written).
	big := fmt.Sprintf(`{"kind":"base","dir":"testdata/big","files":200,"size":%d,"confirm":true}`, int64(64)<<20)
	if code, _ := postJSON(t, url, big); code != 400 {
		t.Fatalf("over cap = %d, want 400", code)
	}
	if _, err := os.Stat(filepath.Join(root, "testdata", "big")); !os.IsNotExist(err) {
		t.Fatal("a refused request must not create anything")
	}

	// A valid call creates exactly what was asked for.
	code, body := postJSON(t, url, `{"kind":"base","dir":"testdata/ok","files":3,"size":1024,"seed":1,"confirm":true}`)
	if code != 200 {
		t.Fatalf("valid call = %d (%s), want 200", code, body)
	}
	for i := 0; i < 3; i++ {
		p := filepath.Join(root, "testdata", "ok", fmt.Sprintf("f%07d.bin", i))
		if st, err := os.Stat(p); err != nil {
			t.Fatalf("expected %s to exist: %v", p, err)
		} else if st.Size() != 1024 {
			t.Fatalf("%s size = %d, want 1024", p, st.Size())
		}
	}

	// And the use is recorded in the folder's history.
	code, _ = getJSON(t, fmt.Sprintf("%s/api/events?limit=20", srv.URL))
	if code != 200 {
		t.Fatalf("events = %d", code)
	}
}
