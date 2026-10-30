package staging

import "testing"

// TestIsTransient pins the hard rule that filesystem/SMB bookkeeping names are
// never data. The failure it prevents was measured on Unraid: deleting a 1.8 GB
// file that something still had open left a .fuse_hidden<inode><gen> entry, which
// the scanner reported as a "local add" and synced to the peer — shipping the
// deleted bytes again under a name the operator never created.
func TestIsTransient(t *testing.T) {
	cases := []struct {
		rel  string
		want bool
	}{
		{".fuse_hidden002e778400000154", true},
		{"26013_Caraco/.fuse_hidden0000000b00000001", true},
		{".nfs0a1b2c3d", true},
		{".smbdeleteAAA123", true},
		{"deep/nested/path/.fuse_hidden999", true},
		{"keep.txt", false},
		{"my.fuse_hidden", false},          // the prefix must open the component
		{"dir/file.fuse_hidden0001", false}, // ... and only the name itself counts
		{"", false},
		{".", false},
	}
	for _, c := range cases {
		if got := IsTransient(c.rel); got != c.want {
			t.Errorf("IsTransient(%q) = %v, want %v", c.rel, got, c.want)
		}
	}
}
