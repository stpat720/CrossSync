// Package version implements version vectors for conflict-free,
// multi-master synchronization (following the model used by BEP/Syncthing).
package version

import (
	"encoding/json"
	"sort"
	"strconv"
)

// Vector is a version vector mapping device ID -> counter.
// It is a value type; methods never mutate the receiver.
type Vector map[uint64]uint64

// New returns an empty vector.
func New() Vector { return Vector{} }

// Clone returns a deep copy of the vector.
func (v Vector) Clone() Vector {
	if v == nil {
		return nil
	}
	out := make(Vector, len(v))
	for k, val := range v {
		out[k] = val
	}
	return out
}

// Bump returns a new vector with the counter for device incremented.
func (v Vector) Bump(device uint64) Vector {
	out := v.Clone()
	out[device] = v[device] + 1
	return out
}

// Get returns the counter for device (0 if absent).
func (v Vector) Get(device uint64) uint64 { return v[device] }

// Merge returns the element-wise maximum of v and o. It is used when a
// device adopts a remote version: keeping our own counter guarantees our
// next local change supersedes theirs.
func (v Vector) Merge(o Vector) Vector {
	out := v.Clone()
	for k, val := range o {
		if val > out[k] {
			out[k] = val
		}
	}
	return out
}

// Compare returns:
//
//	-1 if v < o (o strictly dominates v)
//	 0 if v == o
//	 1 if v > o (v strictly dominates o)
//	 2 if v and o are concurrent (a conflict)
func (v Vector) Compare(o Vector) int {
	if len(v) == 0 && len(o) == 0 {
		return 0
	}
	gt, lt := false, false
	keys := make(map[uint64]struct{}, len(v)+len(o))
	for k := range v {
		keys[k] = struct{}{}
	}
	for k := range o {
		keys[k] = struct{}{}
	}
	for k := range keys {
		a, b := v[k], o[k]
		if a > b {
			gt = true
		}
		if a < b {
			lt = true
		}
	}
	switch {
	case gt && !lt:
		return 1
	case lt && !gt:
		return -1
	case !gt && !lt:
		return 0
	default:
		return 2
	}
}

// Equal reports whether v and o are identical.
func (v Vector) Equal(o Vector) bool { return v.Compare(o) == 0 }

// DeviceID derives the short device ID (first 64 bits) from a device
// identity, used as the per-device key in version vectors.
func DeviceID(id []byte) uint64 {
	var out uint64
	for i := 0; i < len(id) && i < 8; i++ {
		out = out<<8 | uint64(id[i])
	}
	return out
}

// MarshalJSON produces a stable object representation with sorted keys,
// e.g. {"1":2,"3":4}.
func (v Vector) MarshalJSON() ([]byte, error) {
	keys := make([]uint64, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	out := []byte("{")
	for i, k := range keys {
		if i > 0 {
			out = append(out, ',')
		}
		out = append(out, '"')
		out = strconv.AppendUint(out, k, 10)
		out = append(out, '"', ':')
		out = strconv.AppendUint(out, v[k], 10)
	}
	out = append(out, '}')
	return out, nil
}

// UnmarshalJSON reads the object representation produced by MarshalJSON.
func (v *Vector) UnmarshalJSON(b []byte) error {
	var raw map[string]uint64
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	out := make(Vector, len(raw))
	for k, val := range raw {
		id, err := strconv.ParseUint(k, 10, 64)
		if err != nil {
			return err
		}
		out[id] = val
	}
	*v = out
	return nil
}
