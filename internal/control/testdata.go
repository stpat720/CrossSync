package control

import (
	"encoding/json"
	"fmt"
	"net/http"

	"crosssync/internal/testgen"
)

// Test-data generation endpoint.
//
// Why it exists: validating convergence on a mostly-in-sync pair needs large,
// IDENTICAL corpora on two machines, plus controlled divergence (new files,
// renames, newer versions). Doing that by hand needs write access to both volumes
// and a lot of patience. This lets each daemon build its own side from the same
// spec, so "the same data on both boxes" really is byte- and mtime-identical.
//
// Safety rails, because an unauthenticated control API plus a bulk writer is a
// dangerous combination:
//   - it only ever touches <root>/testdata/ (see testgen.resolve);
//   - every call needs "confirm": true;
//   - one call is capped at maxTestDataBytes/maxTestDataFiles, so no single
//     request can fill an array;
//   - using it records a warning event, so it shows up in the history.
const (
	maxTestDataBytes = int64(8) << 30 // 8 GiB per call
	maxTestDataFiles = 200000
)

type testDataRequest struct {
	testgen.Spec
	Confirm bool `json:"confirm"`
}

func (s *Service) handleTestData(w http.ResponseWriter, r *http.Request) {
	var body testDataRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("invalid body: %w", err))
		return
	}
	if !body.Confirm {
		writeErr(w, http.StatusForbidden, fmt.Errorf(`test data generation requires "confirm": true`))
		return
	}
	spec := body.Spec
	if spec.Files > maxTestDataFiles {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("files %d exceeds the per-call cap of %d", spec.Files, maxTestDataFiles))
		return
	}
	if spec.Size > 0 && int64(spec.Files)*spec.Size > maxTestDataBytes {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("%d files x %d bytes exceeds the per-call cap of %d bytes", spec.Files, spec.Size, maxTestDataBytes))
		return
	}

	// The node holds the folder lock across the write (a scan must not index a
	// half-built corpus) and records an event so the use is visible in history.
	res, err := s.n.GenerateTestData(r.PathValue("id"), spec)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}
