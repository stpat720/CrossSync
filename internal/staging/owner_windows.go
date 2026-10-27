//go:build windows

package staging

// OwnerOf has no meaning on Windows: files inherit the share's ACLs and there is
// no POSIX uid/gid to adopt.
func OwnerOf(path string) (uid, gid int, ok bool) {
	return 0, 0, false
}
