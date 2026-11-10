//go:build unix

package main

// serve runs envd in the foreground; systemd owns its lifecycle, so nothing
// ever asks it to stop.
func serve(run func(stop <-chan struct{}) error) error {
	return run(make(chan struct{}))
}
