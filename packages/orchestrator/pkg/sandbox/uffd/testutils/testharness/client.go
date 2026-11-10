package testharness

import (
	"io"
	"net/rpc"
	"net/rpc/jsonrpc"
)

// Client is the typed parent-side wrapper around the JSON-RPC channel
// to the child helper process.
type Client struct {
	rpc  *rpc.Client
	conn io.Closer
}

// NewClient wraps an already-connected duplex stream. Closing the
// returned Client closes the underlying conn.
func NewClient(conn io.ReadWriteCloser) *Client {
	return &Client{
		rpc:  jsonrpc.NewClient(conn),
		conn: conn,
	}
}

func (c *Client) Bootstrap(args BootstrapArgs) error {
	return c.rpc.Call("Lifecycle.Bootstrap", &args, &BootstrapReply{})
}

func (c *Client) WaitReady() error {
	return c.rpc.Call("Lifecycle.WaitReady", &Empty{}, &Empty{})
}

func (c *Client) Shutdown() error {
	return c.rpc.Call("Lifecycle.Shutdown", &Empty{}, &Empty{})
}

func (c *Client) Pause() error {
	return c.rpc.Call("Paging.Pause", &Empty{}, &Empty{})
}

func (c *Client) Resume() error {
	return c.rpc.Call("Paging.Resume", &Empty{}, &Empty{})
}

func (c *Client) PageStates() ([]PageStateEntry, error) {
	var reply PageStatesReply
	if err := c.rpc.Call("Paging.States", &Empty{}, &reply); err != nil {
		return nil, err
	}

	return reply.Entries, nil
}

func (c *Client) InstallBarrier(addr uintptr, point Point) (uint64, error) {
	var reply FaultBarrierReply
	if err := c.rpc.Call("Barriers.Install", &FaultBarrierArgs{Addr: uint64(addr), Point: uint8(point)}, &reply); err != nil {
		return 0, err
	}

	return reply.Token, nil
}

func (c *Client) WaitFaultHeld(token uint64) error {
	return c.rpc.Call("Barriers.WaitHeld", &TokenArgs{Token: token}, &Empty{})
}

func (c *Client) ReleaseFault(token uint64) error {
	return c.rpc.Call("Barriers.Release", &TokenArgs{Token: token}, &Empty{})
}

func (c *Client) Close() error {
	return c.conn.Close()
}

// FaultOffsets returns the memfile offsets of every fault event the child's
// serve loop has dequeued so far, in dequeue order.
func (c *Client) FaultOffsets() ([]int64, error) {
	var reply FaultOffsetsReply
	if err := c.rpc.Call("Paging.FaultOffsets", &Empty{}, &reply); err != nil {
		return nil, err
	}

	return reply.Offsets, nil
}

// SourceStats returns how many source reads the child has issued and the
// most that were in flight at once.
func (c *Client) SourceStats() (SourceStatsReply, error) {
	var reply SourceStatsReply
	err := c.rpc.Call("Paging.SourceStats", &Empty{}, &reply)

	return reply, err
}

// ServeStats returns the child's serve counters.
func (c *Client) ServeStats() (ServeStatsReply, error) {
	var reply ServeStatsReply
	err := c.rpc.Call("Paging.ServeStats", &Empty{}, &reply)

	return reply, err
}

// Prefault installs one page through the child's Prefault path, with the
// page's bytes taken from the child's source.
func (c *Client) Prefault(offset int64) (bool, error) {
	var reply PrefaultReply
	err := c.rpc.Call("Paging.Prefault", &PrefaultArgs{Offset: offset}, &reply)

	return reply.Installed, err
}

// PageState returns the tracker state of one page without draining workers.
func (c *Client) PageState(offset int64) (uint8, error) {
	var reply PageStateReply
	err := c.rpc.Call("Paging.PageState", &PageStateArgs{Offset: offset}, &reply)

	return reply.State, err
}

// EventPending reports whether the child's uffd fd is readable, i.e. the
// kernel has queued an event the serve loop has not read yet.
func (c *Client) EventPending() (bool, error) {
	var reply EventPendingReply
	err := c.rpc.Call("Paging.EventPending", &Empty{}, &reply)

	return reply.Pending, err
}

// CoWBegin installs a CoW export window over the given page indices in the
// serving child (real BeginCoWExport: arm + install through the live uffd).
func (c *Client) CoWBegin(pages []uint64) error {
	return c.rpc.Call("CoW.Begin", &CoWBeginArgs{Pages: pages}, &Empty{})
}

// CoWSweep drives the child's window Sweep to completion and uninstalls it
// (real EndCoWExport through the serve loop's locks).
func (c *Client) CoWSweep() (CoWSweepReply, error) {
	var reply CoWSweepReply
	err := c.rpc.Call("CoW.Sweep", &Empty{}, &reply)

	return reply, err
}

// CoWState snapshots the child's window: capture progress, cancellation and
// the sink contents.
func (c *Client) CoWState() (CoWStateReply, error) {
	var reply CoWStateReply
	err := c.rpc.Call("CoW.State", &Empty{}, &reply)

	return reply, err
}
