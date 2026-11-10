package api

import (
	"encoding/json"
	"net/http"
)

// cpusHeader reports the CPU state on every /init response,
// after the target the request carried has been recorded.
const cpusHeader = "X-Envd-Cpus"

// cpuReport is the header's wire form: what the guest resumed with, the target envd
// holds after this request, and why this request's cpuCount was refused, if it was.
type cpuReport struct {
	Online   int    `json:"online"`
	Possible int    `json:"possible"`
	Target   int    `json:"target"`
	Rejected string `json:"rejected,omitempty"`
}

// reportCPUs sets the CPU state header. rejected is the error SetTarget returned for this
// request's cpuCount, nil when the count was accepted or absent.
func (a *API) reportCPUs(w http.ResponseWriter, rejected error) {
	st := a.cpuManager.Status()
	report := cpuReport{Online: st.Online, Possible: st.Possible, Target: st.Target}
	if rejected != nil {
		report.Rejected = rejected.Error()
	}
	b, err := json.Marshal(report)
	if err != nil {
		a.logger.Warn().Err(err).Msg("could not encode the cpu state report")

		return
	}
	w.Header().Set(cpusHeader, string(b))
}
