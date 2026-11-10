package orchestrator

// This file is hand-written. It lives next to the generated code because a
// method can only be attached to ServiceInfoStatus from the package that
// declares it. generate.go only writes the .pb.go files, so this is not
// clobbered by regeneration.

// CanAcceptNewRequests reports whether a service reporting this status may be
// sent new requests. Only healthy services may. Draining is on its way out, and
// standby is parked by a sizer so it can be reaped or recalled, so both
// stay unroutable even though their process is still alive and answering
// requests it already accepted.
//
// This answers "may I send this service something new", not "is this service
// still working". A draining service keeps serving the sandboxes it already
// holds, so paths that act on existing work must not gate on this.
func (x ServiceInfoStatus) CanAcceptNewRequests() bool {
	switch x {
	case ServiceInfoStatus_Healthy:
		return true
	default:
		return false
	}
}
