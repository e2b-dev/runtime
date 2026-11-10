//go:build linux

package port

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestScanner_ScanAndBroadcast_SkipsFailedScans verifies that a failed scan is
// not broadcast. Broadcasting its empty result would make the forwarder treat
// every forwarded port as closed and kill its socat.
//
// gopsutil reads TCP connections from $HOST_PROC/net/tcp. The test points it at
// a directory without that file, so every scan fails, then writes the file so
// the next scan succeeds. It cannot run in parallel because t.Setenv changes
// the process environment.
func TestScanner_ScanAndBroadcast_SkipsFailedScans(t *testing.T) {
	proc := t.TempDir()
	t.Setenv("HOST_PROC", proc)

	l := zerolog.Nop()
	scanner := NewScanner(&l, time.Millisecond)
	sub := scanner.AddSubscriber("test", nil)

	scanDone := make(chan struct{})
	go func() {
		defer close(scanDone)
		scanner.ScanAndBroadcast()
	}()
	// Registered after t.Setenv, so the scanner stops before HOST_PROC is restored.
	t.Cleanup(func() {
		scanner.Destroy()
		<-scanDone
	})

	select {
	case got := <-sub.Messages:
		t.Fatalf("failed scan was broadcast: %+v", got)
	case <-time.After(50 * time.Millisecond):
	}

	// One socket listening on 127.0.0.1:8080. Write it to a temp file and
	// rename it into place so a scan never reads a partial file.
	netDir := filepath.Join(proc, "net")
	require.NoError(t, os.Mkdir(netDir, 0o755))
	tcp := "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n" +
		"   0: 0100007F:1F90 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 12345 1 0000000000000000 100 0 0 10 0\n"
	require.NoError(t, os.WriteFile(filepath.Join(netDir, "tcp.tmp"), []byte(tcp), 0o644))
	require.NoError(t, os.Rename(filepath.Join(netDir, "tcp.tmp"), filepath.Join(netDir, "tcp")))

	select {
	case got := <-sub.Messages:
		require.Len(t, got, 1)
		assert.Equal(t, "127.0.0.1", got[0].Laddr.IP)
		assert.Equal(t, uint32(8080), got[0].Laddr.Port)
		assert.Equal(t, "LISTEN", got[0].Status)
	case <-time.After(time.Second):
		t.Fatal("subscriber never got the successful scan")
	}
}
