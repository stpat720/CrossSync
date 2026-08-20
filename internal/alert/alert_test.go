package alert

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFirePostsJSON(t *testing.T) {
	var got payload
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Fatalf("content-type = %q", ct)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	n := New(srv.URL)
	if err := n.Fire("CrossSync nas-01", "disk full on data"); err != nil {
		t.Fatal(err)
	}
	if got.Title != "CrossSync nas-01" || got.Message != "disk full on data" {
		t.Fatalf("payload = %+v", got)
	}
	if len(got.Tags) == 0 {
		t.Fatal("expected tags on the alert (ntfy renders them as emoji)")
	}
}

func TestFireErrorsOnNon2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	n := New(srv.URL)
	if err := n.Fire("t", "m"); err == nil {
		t.Fatal("expected an error for a 500 response")
	}
}

func TestNilAndEmptyAreNoops(t *testing.T) {
	var nilN *Notifier
	if err := nilN.Fire("t", "m"); err != nil {
		t.Fatalf("nil notifier should be a no-op: %v", err)
	}
	empty := New("")
	if err := empty.Fire("t", "m"); err != nil {
		t.Fatalf("empty-url notifier should be a no-op: %v", err)
	}
}
