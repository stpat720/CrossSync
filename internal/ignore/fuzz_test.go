package ignore

import "testing"

// FuzzParse feeds arbitrary pattern lists at the matcher compiler. Patterns come
// from operator input (config file, REST API, UI textareas), so a panic here is
// a crash on a typo. Invariant: never panic, and a matcher that compiles must
// answer Match without panicking on arbitrary relative paths.
func FuzzParse(f *testing.F) {
	f.Add("*.tmp")
	f.Add("media/\n!media/keep/")
	f.Add("**/node_modules/\n#comment\n!\n")
	f.Add("/abs/path\n..\n../../etc\n")
	f.Add("a{b,c}d\n[a-z]*.log\n*\n")
	f.Add("")

	paths := []string{
		"", ".", "a.txt", "media/x.raw", "media/keep/y.raw",
		"deep/nested/dir/file.bin", ".sfx-hold/x", "../escape", "/abs/path",
	}
	f.Fuzz(func(t *testing.T, pattern string) {
		m, err := Parse([]string{pattern})
		if err != nil {
			return
		}
		if m == nil {
			return
		}
		for _, p := range paths {
			_, _ = m.Match(p, false)
			_, _ = m.Match(p, true)
		}
	})
}
