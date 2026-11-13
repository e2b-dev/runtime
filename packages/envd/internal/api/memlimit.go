package api

import (
	"encoding/json"
	"net/http"

	"github.com/e2b-dev/infra/packages/envd/internal/host"
	"github.com/e2b-dev/infra/packages/envd/internal/services/cgroups"
)

// memoryErrorMaxLen bounds the error text in the X-Envd-Memory report, which the
// orchestrator refuses above 1 KiB.
const memoryErrorMaxLen = 128

// memoryReport is the X-Envd-Memory JSON on a served /init: the memory protection on
// envd's cgroup chain at the top level, as every envd that sends the header reports it,
// plus the limit envd keeps on the user and PTY cgroups and the guest's memory layout,
// the latter omitted while envd has not read it.
// A decoder that knows only the protection fields ignores the other two.
type memoryReport struct {
	cgroups.MemoryProtection

	Limit  *memoryLimitReport `json:"limit,omitempty"`
	Layout *host.MemoryLayout `json:"layout,omitempty"`
}

type memoryLimitReport struct {
	// MemTotal is the guest memory the applied limit was computed from, in bytes, not
	// the guest's current memory, so the host can compare it with what it plugged.
	MemTotal uint64 `json:"mem_total"`
	// Applied is the memory.max and memory.high envd last wrote to the user and PTY
	// cgroups, in bytes; 0 when it has written none.
	Applied uint64 `json:"applied"`
	// Pending is true when Applied is not computed from the guest memory envd last
	// observed, or the last recompute failed. It is as current as envd's last recompute
	// and can lag a change by one event or safety check; comparing MemTotal with the
	// plugged memory is the host's definitive check.
	Pending bool   `json:"pending,omitempty"`
	Error   string `json:"error,omitempty"`
}

// reportMemory replaces the protection-only X-Envd-Memory set on entry with the full
// report, on a served /init.
func (a *API) reportMemory(w http.ResponseWriter) {
	// The memory limit worker's snapshot: /init reads no files on the resume path.
	st := a.memLimits.Status()
	report := memoryReport{
		MemoryProtection: a.memory,
		Limit: &memoryLimitReport{
			MemTotal: st.MemTotal,
			Applied:  st.Limit,
			Pending:  st.Pending,
			Error:    truncateRunes(st.Err, memoryErrorMaxLen),
		},
	}
	if st.Layout != (host.MemoryLayout{}) {
		report.Layout = &st.Layout
	}
	b, err := json.Marshal(report)
	if err != nil {
		a.logger.Warn().Err(err).Msg("could not encode the memory report")

		return
	}
	w.Header().Set(memoryHeader, string(b))
}

// truncateRunes cuts s to at most n runes. envd keeps it local rather than linking
// the shared utils package, whose closure is outside the template recipe's hashed paths.
func truncateRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}

	return string(r[:n])
}
