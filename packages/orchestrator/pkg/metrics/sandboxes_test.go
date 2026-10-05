//go:build linux

package metrics

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/envd"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

func TestUnseenOOMKills(t *testing.T) {
	t.Parallel()

	poll := func(sbx *sandbox.Sandbox, body string) []envd.OOMKill {
		var m sandbox.Metrics
		require.NoError(t, json.Unmarshal([]byte(body), &m))

		kills, more := unseenOOMKills(sbx, &m)
		assert.Zero(t, more)

		return kills
	}

	sbx := &sandbox.Sandbox{Metadata: &sandbox.Metadata{}}
	assert.Empty(t, poll(sbx, `{"oom_kills":[{"seq":4,"process":"template"},{"seq":6,"process":"early"}]}`),
		"a poll before the seed logs nothing")
	sbx.OOMKills.Seed([]envd.OOMKill{{Seq: 4, Process: "template"}})
	assert.Equal(t,
		[]envd.OOMKill{{Seq: 6, Process: "early"}, {Seq: 9, Process: "python3"}},
		poll(sbx, `{"oom_kills":[{"seq":4,"process":"template"},{"seq":6,"process":"early"},{"seq":9,"process":"python3"}]}`),
		"only the seed sets the watermark, so a kill the early poll saw is still logged",
	)
	assert.Empty(t, poll(sbx, `{"oom_kills":[{"seq":6,"process":"early"},{"seq":9,"process":"python3"}]}`), "each kill is logged once")
	assert.Empty(t, poll(sbx, `{}`), "an envd that doesn't report kills")
	sbx.OOMKills.Seed(nil)
	assert.Equal(t, []envd.OOMKill{{Seq: 12, Process: "node"}}, poll(sbx, `{"oom_kills":[{"seq":12,"process":"node"}]}`),
		"a second seed, after a checkpoint, keeps the watermark")

	unseeded := &sandbox.Sandbox{Metadata: &sandbox.Metadata{}}
	unseeded.OOMKills.SeedFailed()
	poll(unseeded, `{}`)
	assert.Empty(t, poll(unseeded, `{"oom_kills":[{"seq":4,"process":"old"}]}`), "without a seed, the first list sets the watermark")
	assert.Equal(t, []envd.OOMKill{{Seq: 7, Process: "new"}},
		poll(unseeded, `{"oom_kills":[{"seq":4,"process":"old"},{"seq":7,"process":"new"}]}`),
		"an omitted list doesn't set the watermark, so the log's older kills aren't taken for new ones")

	long := poll(sbx, `{"oom_kills":[{"seq":13,"process":"`+strings.Repeat("x", 1000)+`"}]}`)
	require.Len(t, long, 1)
	assert.Len(t, long[0].Process, maxOomProcessLen, "the guest can't make the line arbitrarily long")

	var flood strings.Builder
	for i := range maxOomKillsLogged + 5 {
		if i > 0 {
			flood.WriteString(",")
		}
		fmt.Fprintf(&flood, `{"seq":%d,"process":"p"}`, 100+i)
	}
	var m sandbox.Metrics
	require.NoError(t, json.Unmarshal([]byte(`{"oom_kills":[`+flood.String()+`]}`), &m))
	logged, more := unseenOOMKills(sbx, &m)
	assert.Len(t, logged, maxOomKillsLogged, "the guest can't make one poll write any number of lines")
	assert.Equal(t, int64(100), logged[0].Seq, "the oldest kills are logged")
	assert.Equal(t, 5, more)
	assert.Empty(t, poll(sbx, `{"oom_kills":[{"seq":114,"process":"p"}]}`), "the kills counted but not logged still count as seen")

	build := &sandbox.Sandbox{Metadata: &sandbox.Metadata{}}
	build.Runtime.SandboxType = sandboxtypes.SandboxTypeBuild
	build.OOMKills.Seed(nil)
	assert.Empty(t, poll(build, `{"oom_kills":[{"seq":3,"process":"apt"}]}`), "a build's kills stay out of the sandbox's logs")
}

func TestSandboxMemory(t *testing.T) {
	t.Parallel()

	m := &sandbox.Metrics{MemTotal: 4 << 30, MemUsed: 1 << 30}

	tests := []struct {
		name         string
		envdVersion  string
		wantTotal    int64
		wantUsed     int64
		wantReported bool
		wantErr      bool
	}{
		{name: "first envd reporting bytes", envdVersion: "0.2.4", wantTotal: 4 << 30, wantUsed: 1 << 30, wantReported: true},
		{name: "current envd", envdVersion: "0.9.0", wantTotal: 4 << 30, wantUsed: 1 << 30, wantReported: true},
		{name: "envd reporting only MiB", envdVersion: "0.2.3"},
		{name: "unparsable version", envdVersion: "not-a-version", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			total, used, reported, err := sandboxMemory(tt.envdVersion, m)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantReported, reported)
			assert.Equal(t, tt.wantTotal, total)
			assert.Equal(t, tt.wantUsed, used)
		})
	}
}
