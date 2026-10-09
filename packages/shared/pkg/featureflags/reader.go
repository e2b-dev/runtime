package featureflags

import "context"

// StringReader evaluates a string flag by key with its caller's fallback and
// the process contexts, for a package that takes a small reader interface
// rather than this package's flag types.
//
// It never logs: it is read on hot paths, and the typed flag methods warn on
// every evaluation of a key the client does not hold.
type StringReader struct{ client *Client }

// StringReader returns a reader over this client; a nil client's reader
// always answers the fallback.
func (c *Client) StringReader() StringReader { return StringReader{client: c} }

// String returns the flag's value, or fallback when the flag is missing or not
// a string. A keyless client serves only the flags registered here and an
// unconnected one serves nothing, so neither is evaluated.
func (r StringReader) String(ctx context.Context, key, fallback string) string {
	c := r.client
	if !c.Live() || !c.ld.Initialized() {
		return fallback
	}

	// The SDK answers fallback on any evaluation error.
	value, _ := c.ld.StringVariationCtx(ctx, key, mergeContexts(ctx, c.allContexts(ctx, nil)), fallback)

	return value
}
