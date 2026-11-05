package servicestatus

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
)

var (
	t0  = time.Unix(1, 0)
	now = time.Unix(2, 0)
)

func clock() time.Time { return now }

func at(changedAt time.Time, status Status, owner string) Snapshot {
	return Snapshot{Status: status, Owner: owner, ChangedAt: changedAt}
}

// fakeStore answers Load from its fields and records what the machine writes.
type fakeStore struct {
	loaded Snapshot
	found  bool
	err    error

	saved   []Snapshot
	cleared int
}

func (s *fakeStore) Load() (Snapshot, bool, error) { return s.loaded, s.found, s.err }

func (s *fakeStore) Save(snap Snapshot) error {
	s.saved = append(s.saved, snap)

	return s.err
}

func (s *fakeStore) Clear() error {
	s.cleared++

	return s.err
}

func TestRules_Table(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		current Snapshot
		req     Request
		next    Snapshot // on success
		reason  Reason   // empty on success
	}{
		{name: "ShuttingDown is process-only", current: at(t0, standby, "x"), req: Request{Status: shuttingDown, Owner: "x", Force: true}, reason: ReasonShuttingDown},
		{name: "NotReady is process-only", current: at(t0, healthy, ""), req: Request{Status: notReady, Force: true}, reason: ReasonNotReady},
		{name: "a status outside the enum", current: at(t0, healthy, ""), req: Request{Status: Status(99), Force: true}, reason: ReasonInvalidStatus},
		{name: "ShuttingDown is terminal", current: at(t0, shuttingDown, ""), req: Request{Status: healthy, Force: true}, reason: ReasonTerminal},
		{name: "invalid owner", current: at(t0, healthy, ""), req: Request{Status: standby, Owner: "Park", Force: true}, reason: ReasonInvalidOwner},
		{name: "owner too long", current: at(t0, healthy, ""), req: Request{Status: standby, Owner: strings.Repeat("a", MaxOwnerLength+1)}, reason: ReasonInvalidOwner},
		{name: "owner with a leading dash", current: at(t0, healthy, ""), req: Request{Status: standby, Owner: "-park"}, reason: ReasonInvalidOwner},
		{name: "owner with a trailing dash", current: at(t0, healthy, ""), req: Request{Status: standby, Owner: "park-"}, reason: ReasonInvalidOwner},
		{name: "owner with a space", current: at(t0, healthy, ""), req: Request{Status: standby, Owner: "park 1"}, reason: ReasonInvalidOwner},
		{name: "owner with an underscore", current: at(t0, healthy, ""), req: Request{Status: standby, Owner: "park_1"}, reason: ReasonInvalidOwner},
		{name: "owner of 63 characters", current: at(t0, healthy, ""), req: Request{Status: standby, Owner: strings.Repeat("a", MaxOwnerLength)}, next: at(now, standby, strings.Repeat("a", MaxOwnerLength))},
		{name: "owner as a uuid", current: at(t0, healthy, ""), req: Request{Status: standby, Owner: "6ba7b810-9dad-41d1-80b4-00c04fd430c8"}, next: at(now, standby, "6ba7b810-9dad-41d1-80b4-00c04fd430c8")},
		{name: "force Standby records the carried owner", current: at(t0, standby, "x"), req: Request{Status: standby, Owner: "y", Force: true}, next: at(now, standby, "y")},
		{name: "force Healthy drops the owner", current: at(t0, standby, "x"), req: Request{Status: healthy, Owner: "y", Force: true}, next: at(now, healthy, "")},
		{name: "force Healthy without an owner", current: at(t0, standby, "x"), req: Request{Status: healthy, Force: true}, next: at(now, healthy, "")},
		{name: "force Draining under an owner", current: at(t0, standby, "x"), req: Request{Status: draining, Force: true}, next: at(now, draining, "")},
		{name: "no owner: Draining to Standby", current: at(t0, draining, ""), req: Request{Status: standby, Owner: "x"}, reason: ReasonDrainingToStandby},
		{name: "force cannot park a draining node", current: at(t0, draining, ""), req: Request{Status: standby, Owner: "x", Force: true}, reason: ReasonDrainingToStandby},
		{name: "no owner: Standby records the owner", current: at(t0, healthy, ""), req: Request{Status: standby, Owner: "x"}, next: at(now, standby, "x")},
		{name: "no owner: Standby without an owner", current: at(t0, healthy, ""), req: Request{Status: standby}, next: at(now, standby, "")},
		{name: "no owner: Healthy", current: at(t0, notReady, ""), req: Request{Status: healthy}, next: at(now, healthy, "")},
		{name: "no owner: Draining", current: at(t0, healthy, ""), req: Request{Status: draining, Owner: "x"}, next: at(now, draining, "")},
		{name: "no owner: Unhealthy", current: at(t0, standby, ""), req: Request{Status: unhealthy}, next: at(now, unhealthy, "")},
		{name: "owner: Standby under the owner is a rewrite", current: at(t0, standby, "x"), req: Request{Status: standby, Owner: "x"}, next: at(t0, standby, "x")},
		{name: "owner: Standby without an owner", current: at(t0, standby, "x"), req: Request{Status: standby}, reason: ReasonOwnerRequired},
		{name: "owner: Standby under another owner", current: at(t0, standby, "x"), req: Request{Status: standby, Owner: "y"}, reason: ReasonOwnerMismatch},
		{name: "owner: Healthy under the owner", current: at(t0, standby, "x"), req: Request{Status: healthy, Owner: "x"}, next: at(now, healthy, "")},
		{name: "owner: Healthy without an owner", current: at(t0, standby, "x"), req: Request{Status: healthy}, reason: ReasonOwnerRequired},
		{name: "owner: Healthy under another owner", current: at(t0, standby, "x"), req: Request{Status: healthy, Owner: "y"}, reason: ReasonOwnerMismatch},
		{name: "owner: Draining", current: at(t0, standby, "x"), req: Request{Status: draining, Owner: "x"}, reason: ReasonDrainingUnderOwner},
		{name: "owner: Unhealthy", current: at(t0, standby, "x"), req: Request{Status: unhealthy}, next: at(now, unhealthy, "")},
		{name: "same status keeps the change time", current: at(t0, healthy, ""), req: Request{Status: healthy}, next: at(t0, healthy, "")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			next, err := Rules(tc.current, tc.req, now)
			if tc.reason == "" {
				require.NoError(t, err)
				require.Equal(t, tc.next, next)

				return
			}

			var refused *RefusedError
			require.ErrorAs(t, err, &refused)
			require.Equal(t, tc.reason, refused.Reason)
			require.Equal(t, tc.req, refused.Requested)
			require.Equal(t, tc.current, next)
		})
	}
}

func TestMachine_OverridePersistsStandbyAndClearsOnReady(t *testing.T) {
	t.Parallel()

	store := &fakeStore{}
	m := New(store, clock)

	require.NoError(t, m.Override(t.Context(), Request{Status: standby, Owner: "x"}))
	require.Equal(t, at(now, standby, "x"), m.Snapshot())
	require.Equal(t, []Snapshot{at(now, standby, "x")}, store.saved)
	require.Zero(t, store.cleared)

	var refused *RefusedError
	require.ErrorAs(t, m.Override(t.Context(), Request{Status: healthy}), &refused)
	require.Equal(t, ReasonOwnerRequired, refused.Reason)
	require.Equal(t, at(now, standby, "x"), m.Snapshot())
	require.Len(t, store.saved, 1, "a refusal touches no store")
	require.Zero(t, store.cleared)

	require.NoError(t, m.Override(t.Context(), Request{Status: healthy, Owner: "x"}))
	require.Equal(t, at(now, healthy, ""), m.Snapshot())
	require.Len(t, store.saved, 1)
	require.Equal(t, 1, store.cleared)
}

func TestMachine_OverrideSurvivesAStoreError(t *testing.T) {
	t.Parallel()

	store := &fakeStore{err: errors.New("disk full")}
	m := New(store, clock)

	require.NoError(t, m.Override(t.Context(), Request{Status: standby, Owner: "x"}))
	require.Equal(t, at(now, standby, "x"), m.Snapshot())
	require.Len(t, store.saved, 1)
}

func TestMachine_RestoreTakesAStandbyRecord(t *testing.T) {
	t.Parallel()

	m := New(&fakeStore{loaded: at(t0, standby, "x"), found: true}, clock)
	require.Equal(t, at(now, healthy, ""), m.Snapshot())

	require.NoError(t, m.Restore(t.Context()))
	require.Equal(t, at(t0, standby, "x"), m.Snapshot())
}

func TestMachine_RestoreIgnoresNonStandbyAndCorrupt(t *testing.T) {
	t.Parallel()

	for value, name := range orchestratorinfo.ServiceInfoStatus_name {
		if Status(value) == standby {
			continue
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			m := New(&fakeStore{loaded: at(t0, Status(value), "x"), found: true}, clock)
			require.NoError(t, m.Restore(t.Context()))
			require.Equal(t, at(now, healthy, ""), m.Snapshot())
		})
	}
	t.Run("no record", func(t *testing.T) {
		t.Parallel()

		m := New(&fakeStore{}, clock)
		require.NoError(t, m.Restore(t.Context()))
		require.Equal(t, at(now, healthy, ""), m.Snapshot())
	})
	t.Run("corrupt", func(t *testing.T) {
		t.Parallel()

		corrupt := errors.New("corrupt")
		m := New(&fakeStore{err: corrupt}, clock)
		require.ErrorIs(t, m.Restore(t.Context()), corrupt)
		require.Equal(t, at(now, healthy, ""), m.Snapshot())
	})
}

func TestMachine_SetShuttingDownIsTerminalAndKeepsTheStandbyRecord(t *testing.T) {
	t.Parallel()

	for value, name := range orchestratorinfo.ServiceInfoStatus_name {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			initial := at(t0, Status(value), "")
			if initial.Status == standby {
				initial.Owner = "x"
			}
			store := &fakeStore{}
			m := &Machine{snap: initial, store: store, now: clock}

			m.Set(t.Context(), shuttingDown)
			want := at(now, shuttingDown, initial.Owner)
			if initial.Status == shuttingDown {
				want.ChangedAt = t0
			}
			require.Equal(t, want, m.Snapshot())

			for target := range orchestratorinfo.ServiceInfoStatus_name {
				next := Status(target)
				m.Set(t.Context(), next)
				require.Equal(t, want, m.Snapshot())

				wantReason := ReasonTerminal
				switch next {
				case shuttingDown:
					wantReason = ReasonShuttingDown
				case notReady:
					wantReason = ReasonNotReady
				}
				var refused *RefusedError
				require.ErrorAs(t, m.Override(t.Context(), Request{Status: next, Owner: "x", Force: true}), &refused)
				require.Equal(t, wantReason, refused.Reason)
				require.Equal(t, want, m.Snapshot())
			}
			require.Empty(t, store.saved)
			require.Zero(t, store.cleared)
		})
	}
}

// The startup hold, its poller and the shutdown are the only callers of Set;
// a status an operator or a sizer set is Override's to move.
func TestMachine_SetLeavesOperatorStatusesAlone(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		from, to Status
		applied  bool
	}{
		{from: standby, to: notReady},
		{from: standby, to: healthy},
		{from: draining, to: healthy},
		{from: unhealthy, to: notReady},
		{from: healthy, to: notReady, applied: true},
		{from: notReady, to: healthy, applied: true},
		{from: healthy, to: draining, applied: true},
	} {
		t.Run(tc.from.String()+"To"+tc.to.String(), func(t *testing.T) {
			t.Parallel()

			initial := at(t0, tc.from, "")
			if tc.from == standby {
				initial.Owner = "x"
			}
			store := &fakeStore{}
			m := &Machine{snap: initial, store: store, now: clock}

			m.Set(t.Context(), tc.to)
			want := initial
			if tc.applied {
				want = at(now, tc.to, "")
			}
			require.Equal(t, want, m.Snapshot())
			require.Empty(t, store.saved)
			require.Zero(t, store.cleared)
		})
	}
}

func TestMachine_ConcurrentOverridesRaceWithShutdown(t *testing.T) {
	t.Parallel()

	m := New(NopStore{}, time.Now)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		<-start
		m.Set(t.Context(), shuttingDown)
	})
	for range 32 {
		wg.Go(func() {
			<-start
			for value := range orchestratorinfo.ServiceInfoStatus_name {
				_ = m.Override(t.Context(), Request{Status: Status(value), Owner: "x", Force: true})
			}
		})
	}
	close(start)
	wg.Wait()
	require.Equal(t, shuttingDown, m.Snapshot().Status)
}
