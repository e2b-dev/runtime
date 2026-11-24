package sandbox

import "github.com/e2b-dev/infra/packages/shared/pkg/utils"

// exposeLocalhostPorts is the exposeLocalhostPorts this sandbox's /init sends to envd at
// envdVersion: nil when the sandbox asked nothing, or when that envd predates the field and
// so keeps forwarding services bound to loopback until its template is rebuilt.
func (s *Sandbox) exposeLocalhostPorts(envdVersion string) *bool {
	ingress := s.Config.GetNetworkIngress()
	if ingress == nil || ingress.ExposeLocalhostPorts == nil {
		return nil
	}
	if ok, err := utils.IsGTEVersion(envdVersion, utils.MinEnvdVersionForExposeLocalhostPorts); err != nil || !ok {
		return nil
	}
	on := ingress.GetExposeLocalhostPorts()

	return &on
}
