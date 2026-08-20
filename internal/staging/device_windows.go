//go:build windows

package staging

// sameDevice on Windows: development builds run everything on one volume,
// so we assume the same device and rely on the EXDEV fallback in Commit if
// a cross-device rename ever occurs.
func sameDevice(a, b string) bool { return true }
