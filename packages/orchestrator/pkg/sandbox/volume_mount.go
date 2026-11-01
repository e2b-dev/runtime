//go:build linux

package sandbox

// NFSMountPath names an export for this exact lifecycle, not the local workload
// mount point. Keeping it in the request prevents an old connection or retry
// from inheriting a replacement lifecycle when its network IP is reused.
func (s *Sandbox) NFSMountPath(volumeName string) string {
	return "/" + s.LifecycleID + "/" + volumeName
}
