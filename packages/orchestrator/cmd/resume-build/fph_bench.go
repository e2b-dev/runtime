//go:build linux

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"

	"github.com/e2b-dev/infra/packages/orchestrator/cmd/internal/cmdutil"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

type fphBenchOptions struct {
	enabled    bool
	workload   string
	iterations int
	delay      time.Duration
}

const fphBenchDrainTimeout = 5 * time.Second

type fphBenchSample struct {
	memfileBytes int64
	hintBytes    uint64
	reportBytes  uint64
	pause        time.Duration
	err          error
}

// fphBench runs the workload N times under FPR-only and N times under FPR+FPH,
// printing the resulting memfile data size and pause time per iteration.
// Smaller memfile is better.
func (r *runner) fphBench(ctx context.Context, opts fphBenchOptions) error {
	if opts.iterations <= 0 {
		return errors.New("fph-bench: iterations must be > 0")
	}
	fmt.Printf("FPH bench (%d iterations per arm, workload=%q)\n", opts.iterations, opts.workload)

	noFph := r.fphBenchArm(ctx, opts, false, "FPR-only ")
	withFph := r.fphBenchArm(ctx, opts, true, "FPR + FPH")

	avg := func(s []fphBenchSample, f func(fphBenchSample) int64) int64 {
		var sum, n int64
		for _, x := range s {
			if x.err == nil {
				sum += f(x)
				n++
			}
		}
		if n == 0 {
			return 0
		}

		return sum / n
	}
	memfile := func(s fphBenchSample) int64 { return s.memfileBytes }
	pause := func(s fphBenchSample) int64 { return int64(s.pause) }

	fmt.Printf("FPR-only : memfile %s  pause %s\n", fmtMiB(avg(noFph, memfile)), time.Duration(avg(noFph, pause)))
	fmt.Printf("FPR + FPH: memfile %s  pause %s\n", fmtMiB(avg(withFph, memfile)), time.Duration(avg(withFph, pause)))

	for _, g := range [][]fphBenchSample{noFph, withFph} {
		for _, s := range g {
			if s.err != nil {
				return s.err
			}
		}
	}

	return nil
}

func (r *runner) fphBenchArm(ctx context.Context, opts fphBenchOptions, withFph bool, label string) []fphBenchSample {
	out := make([]fphBenchSample, 0, opts.iterations)
	for i := range opts.iterations {
		if ctx.Err() != nil {
			break
		}
		s := r.fphBenchOnce(ctx, opts, withFph)
		out = append(out, s)
		if s.err != nil {
			fmt.Printf("[%d/%d] %s: %v\n", i+1, opts.iterations, label, s.err)

			continue
		}
		fmt.Printf("[%d/%d] %s: memfile %s  fpr_freed %s  fph_freed %s  pause %s\n",
			i+1, opts.iterations, label,
			fmtMiB(s.memfileBytes),
			fmtMiB(int64(s.reportBytes)),
			fmtMiB(int64(s.hintBytes)),
			s.pause.Round(time.Millisecond))
	}

	return out
}

func (r *runner) fphBenchOnce(ctx context.Context, opts fphBenchOptions, withFph bool) fphBenchSample {
	if withFph {
		featureflags.NewJSONFlag("free-page-hinting-config", ldvalue.FromJSONMarshal(map[string]any{
			"enabled": true,
			"pause":   int(fphBenchDrainTimeout / time.Millisecond),
		}))
	} else {
		featureflags.NewJSONFlag("free-page-hinting-config", ldvalue.Null())
	}

	buildID := uuid.New().String()
	defer cleanupLocalBuild(buildID)

	runtime := sandboxtypes.RuntimeMetadata{
		TemplateID:  r.buildID,
		TeamID:      "local",
		SandboxID:   fmt.Sprintf("fph-bench-%d", time.Now().UnixNano()),
		ExecutionID: fmt.Sprintf("fph-bench-exec-%d", time.Now().UnixNano()),
	}
	t0 := time.Now()
	sbx, err := r.factory.ResumeSandbox(ctx, r.tmpl, r.sbxConfig, runtime, t0, t0.Add(24*time.Hour), nil)
	if err != nil {
		return fphBenchSample{err: fmt.Errorf("resume: %w", err)}
	}
	defer sbx.Close(context.WithoutCancel(ctx))

	if err := runCommandInSandbox(ctx, sbx, opts.workload); err != nil {
		return fphBenchSample{err: fmt.Errorf("workload: %w", err)}
	}
	if opts.delay > 0 {
		select {
		case <-ctx.Done():
			return fphBenchSample{err: ctx.Err()}
		case <-time.After(opts.delay):
		}
	}

	origMeta, err := r.tmpl.Metadata()
	if err != nil {
		return fphBenchSample{err: fmt.Errorf("metadata: %w", err)}
	}
	newMeta := origMeta
	newMeta.Template.BuildID = buildID

	pauseStart := time.Now()
	snapshot, err := sbx.Pause(ctx, newMeta, sandbox.SnapshotUseCasePause)
	pauseDur := time.Since(pauseStart)
	if err != nil {
		return fphBenchSample{pause: pauseDur, err: fmt.Errorf("pause: %w", err)}
	}
	defer snapshot.Close(context.WithoutCancel(ctx))

	balloon, _ := sbx.FlushAndReadBalloonMetrics(ctx)

	upload, err := sandbox.NewUpload(ctx, nil, snapshot, r.storage, storage.CompressConfig{}, nil, "", nil, nil)
	if err != nil {
		return fphBenchSample{pause: pauseDur, err: fmt.Errorf("upload prepare: %w", err)}
	}
	if err := upload.Run(ctx); err != nil {
		return fphBenchSample{pause: pauseDur, err: fmt.Errorf("upload: %w", err)}
	}

	memfileBytes, err := readLocalMemfileSize(buildID)
	if err != nil {
		return fphBenchSample{pause: pauseDur, err: fmt.Errorf("memfile size: %w", err)}
	}

	return fphBenchSample{
		memfileBytes: memfileBytes,
		hintBytes:    balloon.HintFreed,
		reportBytes:  balloon.ReportFreed,
		pause:        pauseDur,
	}
}

// localTemplateBasePath resolves the template storage and returns its local
// base path; errors when template storage is not on the local filesystem.
func localTemplateBasePath() (string, error) {
	spec, err := cfg.TemplateStorage()
	if err != nil {
		return "", err
	}
	if spec.Provider != storage.LocalStorageProvider {
		return "", fmt.Errorf("template storage provider is %s, not local", spec.Provider)
	}

	return spec.BasePath, nil
}

func readLocalMemfileSize(buildID string) (int64, error) {
	basePath, err := localTemplateBasePath()
	if err != nil {
		return 0, fmt.Errorf("-fph-bench requires local storage: %w", err)
	}

	return cmdutil.GetActualFileSize(filepath.Join(basePath, buildID, storage.MemfileName))
}

func cleanupLocalBuild(buildID string) {
	if base, err := localTemplateBasePath(); err == nil {
		_ = os.RemoveAll(filepath.Join(base, buildID))
	}
}

func fmtMiB(n int64) string { return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20)) }
