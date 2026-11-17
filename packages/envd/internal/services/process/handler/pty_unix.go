//go:build unix

package handler

import (
	"context"
	"os"
	"os/exec"

	"github.com/creack/pty"
)

// startPty forks the command on a new pty and returns its master. The pty
// package starts the child itself, so Start finds it already running.
func (p *Handler) startPty(_ context.Context, cmd *exec.Cmd, cols, rows uint16) (*os.File, error) {
	return pty.StartWithSize(cmd, &pty.Winsize{Cols: cols, Rows: rows})
}

func (p *Handler) sysResizeTty(cols, rows uint16) error {
	return pty.Setsize(p.tty, &pty.Winsize{Cols: cols, Rows: rows})
}

func (p *Handler) sysWriteTty(data []byte) error {
	_, err := p.tty.Write(data)

	return err
}
