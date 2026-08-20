// Package ignore implements ignore rules (extension/name/path patterns)
// used to decide which files a folder excludes from synchronization.
package ignore

import (
	"path"
	"strings"
)

// Rule is a single compiled ignore rule.
type Rule struct {
	Raw     string
	Negate  bool   // a leading "!" re-includes matched paths
	Ext     string // if set, match by extension (lowercased, no dot)
	Exact   string // if set, match by basename exactly
	DirPath string // if set, match a directory prefix in the relative path
	IsDir   bool   // only applies to directories (trailing '/')
}

// Matcher holds a compiled set of rules. The last matching rule wins,
// so a "!" rule can override an earlier ignore.
type Matcher struct {
	rules []Rule
}

// Parse compiles ignore rules from lines. Blank lines and lines starting
// with '#' are ignored.
func Parse(lines []string) (*Matcher, error) {
	m := &Matcher{}
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		r := Rule{Raw: line}
		if strings.HasPrefix(line, "!") {
			r.Negate = true
			line = strings.TrimPrefix(line, "!")
		}
		line = strings.TrimSpace(line)
		if strings.HasSuffix(line, "/") {
			// A trailing slash makes this a directory rule: it matches the
			// directory itself and everything beneath it.
			r.IsDir = true
			r.DirPath = strings.Trim(line, "/")
		} else {
			switch {
			case strings.HasPrefix(line, "/"):
				// anchored directory path
				r.DirPath = strings.TrimPrefix(line, "/")
			case strings.Contains(line, "*.") && !strings.Contains(line, "/"):
				// extension pattern like *.py or *.tmp
				r.Ext = strings.ToLower(strings.SplitN(line, "*.", 2)[1])
			case strings.HasPrefix(line, "*"):
				// suffix match like *_cache (kept as a "*" marker so Match
				// knows it is a suffix rather than an exact basename)
				r.Exact = line
			case strings.Contains(line, "/"):
				// non-anchored directory path
				r.DirPath = strings.Trim(line, "/")
			default:
				// exact basename
				r.Exact = line
			}
		}
		m.rules = append(m.rules, r)
	}
	return m, nil
}

// Match reports whether rel (a slash-separated relative path) is ignored.
// isDir indicates whether the path refers to a directory. The returned rule
// is the one that decided the outcome (useful for the UI to show which rule
// matched).
func (m *Matcher) Match(rel string, isDir bool) (bool, *Rule) {
	if m == nil {
		return false, nil
	}
	base := path.Base(rel)
	lower := strings.ToLower(base)
	var decided *Rule
	ignored := false
	for i := range m.rules {
		r := &m.rules[i]
		match := false
		switch {
		case r.DirPath != "":
			// A directory rule matches the dir itself or anything beneath it.
			match = rel == r.DirPath || strings.HasPrefix(rel, r.DirPath+"/")
		case r.Ext != "":
			match = !isDir && strings.HasSuffix(lower, "."+r.Ext)
		case strings.HasPrefix(r.Exact, "*"):
			// suffix match on the basename
			match = !isDir && strings.HasSuffix(base, strings.TrimPrefix(r.Exact, "*"))
		case r.Exact != "":
			match = base == r.Exact
		}
		if match {
			ignored = !r.Negate
			decided = r
		}
	}
	if decided == nil {
		return false, nil
	}
	return ignored, decided
}
