package socket

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"
)

const waitInterval = 10 * time.Millisecond

// Wait waits for the given file to exist.
func Wait(ctx context.Context, socketPath string) error {
	ticker := time.NewTicker(waitInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("cancelled wait for socket '%s': %w, cause: %w", socketPath, ctx.Err(), context.Cause(ctx))
		case <-ticker.C:
			if _, err := os.Stat(socketPath); err != nil {
				continue
			}

			return nil
		}
	}
}

// WaitAccepting waits until socketPath accepts a connection. The file appears
// when the server binds, before it listens, and a connect in between is
// refused; a socket another process serves is ready only once it accepts.
// Not for a socket this process serves: the probe connection would be taken
// for a client's.
func WaitAccepting(ctx context.Context, socketPath string) error {
	ticker := time.NewTicker(waitInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("cancelled wait for socket '%s': %w, cause: %w", socketPath, ctx.Err(), context.Cause(ctx))
		case <-ticker.C:
			conn, err := (&net.Dialer{Timeout: waitInterval}).DialContext(ctx, "unix", socketPath)
			if err != nil {
				continue
			}
			_ = conn.Close()

			return nil
		}
	}
}
