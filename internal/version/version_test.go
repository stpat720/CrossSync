package version

import "testing"

func TestCompare(t *testing.T) {
	a := Vector{1: 3}
	b := Vector{1: 5}
	if got := a.Compare(b); got != -1 {
		t.Fatalf("a.Compare(b) = %d, want -1", got)
	}
	if got := b.Compare(a); got != 1 {
		t.Fatalf("b.Compare(a) = %d, want 1", got)
	}
	c := Vector{1: 3}
	if got := a.Compare(c); got != 0 {
		t.Fatalf("a.Compare(c) = %d, want 0", got)
	}
	// Concurrent: a changed by device1, b changed by device2.
	d := Vector{1: 4}
	e := Vector{2: 1}
	if got := d.Compare(e); got != 2 {
		t.Fatalf("d.Compare(e) = %d, want 2", got)
	}
	if got := e.Compare(d); got != 2 {
		t.Fatalf("e.Compare(d) = %d, want 2", got)
	}
	// Domination with multiple devices.
	f := Vector{1: 4, 2: 3}
	g := Vector{1: 4, 2: 2}
	if got := f.Compare(g); got != 1 {
		t.Fatalf("f.Compare(g) = %d, want 1", got)
	}
}

func TestBump(t *testing.T) {
	v := New()
	v = v.Bump(7)
	if v.Get(7) != 1 {
		t.Fatalf("after first bump, counter = %d, want 1", v.Get(7))
	}
	v = v.Bump(7)
	if v.Get(7) != 2 {
		t.Fatalf("after second bump, counter = %d, want 2", v.Get(7))
	}
	// Original vector must be untouched.
	if v.Get(9) != 0 {
		t.Fatalf("unexpected counter for device 9: %d", v.Get(9))
	}
}

func TestMergeKeepsOwnCounter(t *testing.T) {
	local := Vector{1: 5}
	remote := Vector{2: 3, 1: 2}
	merged := local.Merge(remote)
	if merged.Get(1) != 5 {
		t.Fatalf("own counter must be preserved, got %d", merged.Get(1))
	}
	if merged.Get(2) != 3 {
		t.Fatalf("remote counter not adopted, got %d", merged.Get(2))
	}
}

func TestJSONRoundTrip(t *testing.T) {
	v := Vector{1: 2, 3: 4, 100: 1}
	b, err := v.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	var out Vector
	if err := out.UnmarshalJSON(b); err != nil {
		t.Fatal(err)
	}
	if !v.Equal(out) {
		t.Fatalf("round trip mismatch: %v != %v", v, out)
	}
	// Stable ordering check.
	want := `{"1":2,"3":4,"100":1}`
	if string(b) != want {
		t.Fatalf("marshaled = %s, want %s", b, want)
	}
}

func TestDeviceID(t *testing.T) {
	if DeviceID([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9}) != 0x0102030405060708 {
		t.Fatalf("DeviceID wrong: %x", DeviceID([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9}))
	}
	if DeviceID(nil) != 0 {
		t.Fatalf("DeviceID of nil should be 0")
	}
}
