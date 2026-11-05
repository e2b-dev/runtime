package redis

import "errors"

// faultPoint names a place in a store operation between two commands, where
// a test hook can inject a failure. The error the hook returns at a point is
// returned by the command that follows it, which does not run, except at
// faultAddRecordWrite, consulted after the record write ran so that its
// error replaces the reply of a write that landed. errSimulatedCrash ends
// the operation, or the healer's repair, where it stands, locks left held as
// a crashed replica's would be. faultHealerUnderLock is consulted once the
// healer holds a nominee's lock, or both of a twin's, before it reads
// anything under them.
type faultPoint string

const (
	faultAddAfterLocks         faultPoint = "add.after_locks"
	faultAddBeforeOtherDel     faultPoint = "add.before_other_del"
	faultAddAfterOtherDel      faultPoint = "add.after_other_del"
	faultAddRecordWrite        faultPoint = "add.record_write"
	faultAddAfterRecordWrite   faultPoint = "add.after_record_write"
	faultAddAfterIndex         faultPoint = "add.after_index"
	faultRemoveAfterSREM       faultPoint = "remove.after_srem"
	faultRemoveAfterDelete     faultPoint = "remove.after_delete"
	faultUpdateAfterResolve    faultPoint = "update.after_resolve"
	faultReadersSecondPipeline faultPoint = "readers.second_pipeline"
	faultHealerAfterNominate   faultPoint = "healer.after_nominate"
	faultHealerUnderLock       faultPoint = "healer.under_lock"
)

var errSimulatedCrash = errors.New("simulated crash")

// SetFaultHook installs f, consulted at every fault point; nil, the
// production value, removes it.
func (s *Storage) SetFaultHook(f func(faultPoint) error) {
	s.faults = f
}

// fault returns the error the hook injects at p; nil without a hook.
func (s *Storage) fault(p faultPoint) error {
	if s.faults == nil {
		return nil
	}

	return s.faults(p)
}

// run runs cmd, unless the hook at the point before it injects an error,
// which it returns in cmd's place.
func (s *Storage) run(p faultPoint, cmd func() error) error {
	if err := s.fault(p); err != nil {
		return err
	}

	return cmd()
}

// crashed reports whether err is a simulated crash, after which an operation
// returns without another step, its lock release included.
func crashed(err error) bool {
	return errors.Is(err, errSimulatedCrash)
}
