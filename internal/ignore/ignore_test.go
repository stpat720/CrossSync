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
