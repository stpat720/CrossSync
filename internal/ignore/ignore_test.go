package ignore

import "testing"

func TestExtensionRule(t *testing.T) {
	m, err := Parse([]string{"*.tmp"})
	if err != nil {
		t.Fatal(err)
	}
	if ignored, r := m.Match("a/b/file.TMP", false); !ignored || r == nil {
		t.Fatalf("expected *.tmp to ignore file.TMP (case-insensitive), got ignored=%v rule=%v", ignored, r)
	}
	if ignored, _ := m.Match("a/b/file.txt", false); ignored {
		t.Fatalf("*.tmp should not ignore file.txt")
	}
	if ignored, _ := m.Match("a/b/note.tmp.bak", false); ignored {
		t.Fatalf("*.tmp should not ignore note.tmp.bak")
	}
}

func TestDirectoryRule(t *testing.T) {
	m, err := Parse([]string{".venv/"})
	if err != nil {
		t.Fatal(err)
	}
	if ignored, _ := m.Match(".venv/lib/python/site.py", false); !ignored {
		t.Fatalf(".venv/ should ignore files under .venv")
	}
	// Should not apply to files outside the dir.
	if ignored, _ := m.Match("src/site.py", false); ignored {
		t.Fatalf(".venv/ should not ignore src/site.py")
	}
}

func TestNegation(t *testing.T) {
	m, err := Parse([]string{"*.log", "!important.log"})
	if err != nil {
		t.Fatal(err)
	}
	if ignored, _ := m.Match("debug.log", false); !ignored {
		t.Fatalf("*.log should ignore debug.log")
	}
	if ignored, _ := m.Match("important.log", false); ignored {
		t.Fatalf("!important.log should re-include important.log")
	}
}

func TestExactName(t *testing.T) {
	m, err := Parse([]string{"desktop.ini", "thumbs.db"})
	if err != nil {
		t.Fatal(err)
	}
	if ignored, _ := m.Match("sub/thumbs.db", false); !ignored {
		t.Fatalf("thumbs.db should be ignored anywhere")
	}
	if ignored, _ := m.Match("sub/desktop.ini.bak", false); ignored {
		t.Fatalf("desktop.ini.bak should not be ignored")
	}
}

func TestSuffixMatch(t *testing.T) {
	m, err := Parse([]string{"*_cache"})
	if err != nil {
		t.Fatal(err)
	}
	if ignored, _ := m.Match("data/my_cache", false); !ignored {
		t.Fatalf("*_cache should match my_cache")
	}
	if ignored, _ := m.Match("data/mycache2", false); ignored {
		t.Fatalf("*_cache should not match mycache2")
	}
}

// TestDeepDirectoryRule covers the "*/name/" any-depth directory form: it
// matches a directory of that name at any depth (and everything beneath
// it), but never a mere name prefix like "proxyx".
func TestDeepDirectoryRule(t *testing.T) {
	m, err := Parse([]string{"*/proxy/"})
	if err != nil {
		t.Fatal(err)
	}
	for _, rel := range []string{"p/proxy", "p/proxy/f.mp4", "a/b/proxy", "a/b/proxy/x.bin"} {
		if ignored, _ := m.Match(rel, false); !ignored {
			t.Errorf("*/proxy/ should match %s", rel)
		}
		if ignored, _ := m.Match(rel, true); !ignored {
			t.Errorf("*/proxy/ should match %s as a directory", rel)
		}
	}
	for _, rel := range []string{"p/proxyx", "p/proxyx/f.mp4", "xproxy", "proj"} {
		if ignored, _ := m.Match(rel, false); ignored {
			t.Errorf("*/proxy/ should not match %s", rel)
		}
	}

	// A plain "dir/" rule stays anchored at the folder root.
	root, err := Parse([]string{"media/"})
	if err != nil {
		t.Fatal(err)
	}
	if ignored, _ := root.Match("media", true); !ignored {
		t.Error("media/ should match the root dir media")
	}
	if ignored, _ := root.Match("media/x", false); !ignored {
		t.Error("media/ should match files under the root dir media")
	}
	if ignored, _ := root.Match("p/media", false); ignored {
		t.Error("media/ must not match p/media (root-anchored)")
	}
}
