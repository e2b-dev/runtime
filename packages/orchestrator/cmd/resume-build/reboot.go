package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/guest"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

// rebootResetWait is how long -reboot-on-reset waits, once the command has
// ended, for the guest to reset.
const rebootResetWait = 60 * time.Second

// printRebootLine prints the line a run of -reboot-on-reset is checked by.
func printRebootLine(path string, event fc.CrashCause, res sandbox.GuestRebootResult, took time.Duration) {
	fmt.Printf("REBOOT path=%s event=%s outcome=%s reset_to_ready=%s consecutive_resets=%d\n",
		path, event, res.Outcome, took.Round(time.Millisecond), res.ConsecutiveResets)
}

// startRebootReport prints the REBOOT line of a guest that reset before envd
// first answered.
func startRebootReport(_ context.Context, res sandbox.GuestRebootResult, resetAt time.Time) {
	printRebootLine("start", fc.CrashCauseGuestReset, res, time.Since(resetAt))
}

// cmdRebootOnReset runs the command, which is expected to reset the guest,
// boots the sandbox again in place the way the orchestrator does for a
// running sandbox, and checks that the rebooted sandbox runs a command on a
// new boot.
func (r *runner) cmdRebootOnReset(ctx context.Context, opts runOptions) error {
	runtime := sandboxtypes.RuntimeMetadata{
		TemplateID:  r.buildID,
		TeamID:      "local",
		SandboxID:   fmt.Sprintf("sbx-%d", time.Now().UnixNano()),
		ExecutionID: fmt.Sprintf("exec-%d", time.Now().UnixNano()),
	}

	fmt.Println("🚀 Starting sandbox...")
	t0 := time.Now()
	sbx, err := r.startSandbox(ctx, runtime, t0, t0.Add(24*time.Hour))
	if err != nil {
		return err
	}
	defer sbx.Close(context.WithoutCancel(ctx))
	fmt.Printf("✅ Sandbox started in %s\n", time.Since(t0))

	// A reboot in place takes the sandbox out of the live map.
	if err := r.factory.Sandboxes.MarkRunning(ctx, sbx); err != nil {
		return fmt.Errorf("register the sandbox: %w", err)
	}

	reset := r.sbxConfig.GuestProfile().Reset()
	before, err := bootID(ctx, sbx, reset)
	if err != nil {
		return err
	}
	fmt.Printf("   boot_id: %s\n", before)

	fmt.Printf("🔧 Running: %s\n", opts.cmd)
	// A reset can cut the command's stream, so its error alone decides nothing.
	if cmdErr := runCommandInSandbox(ctx, sbx, opts.cmd); cmdErr != nil {
		fmt.Printf("   command ended: %v\n", cmdErr)
	}

	waitCtx, cancel := context.WithTimeout(ctx, rebootResetWait)
	defer cancel()
	waitErr := sbx.Wait(waitCtx)
	if waitCtx.Err() != nil {
		return fmt.Errorf("the guest did not reset within %s of the command", rebootResetWait)
	}
	resetAt := time.Now()

	event := fc.ClassifyExit(sbx.FirecrackerExit())
	if waitErr != nil || sbx.GetStopReason() != sandbox.StopReasonCrashed || !event.GuestEvent().Restarts() {
		return errors.Join(fmt.Errorf("the sandbox ended (stop reason %s, exit %s), not on a guest reset", sbx.GetStopReason(), event), waitErr)
	}

	res := r.factory.RebootInPlace(ctx, sbx)
	printRebootLine("running", event, res, time.Since(resetAt))
	if res.Sandbox == nil {
		return fmt.Errorf("reboot in place ended %s: %w", res.Outcome, res.Err)
	}
	next := res.Sandbox
	defer next.Close(context.WithoutCancel(ctx))

	after, err := bootID(ctx, next, reset)
	if err != nil {
		return fmt.Errorf("the rebooted sandbox: %w", err)
	}
	fmt.Printf("   boot_id: %s\n", after)
	if reset.IsSameBoot(before, after) {
		return errors.New("the rebooted sandbox reports the boot ID it had before the reset")
	}

	fmt.Println("✅ The rebooted sandbox runs commands on a new boot")

	return nil
}

// bootID reads the guest's boot identity through envd with the profile's command.
func bootID(ctx context.Context, sbx *sandbox.Sandbox, reset guest.Reset) (string, error) {
	var out strings.Builder
	if err := runCommandInSandboxTo(ctx, sbx, reset.IdentityCommand(), &out); err != nil {
		return "", fmt.Errorf("read the boot ID: %w", err)
	}

	return strings.TrimSpace(out.String()), nil
}
