package placement

import "time"

const (
	maxRetries = 3

	// A single refusal should cost next to nothing, so the first window is
	// short; the cap bounds how fast a placement that finds every node busy
	// keeps asking.
	refusalBackoffBase = time.Millisecond
	refusalBackoffMax  = time.Second
)
