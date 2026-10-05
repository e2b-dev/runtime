//go:build linux

package base

import (
	"bufio"
	"bytes"
	"context"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	tt "text/template"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapio"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	sbxtemplate "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/config"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/core/filesystem"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/core/rootfs"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/layer"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/phases"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/writer"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/units"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	provisionTimeout = 5 * time.Minute
)

//go:embed provision.sh
var provisionScriptFile string
var ProvisionScriptTemplate = tt.Must(tt.New("provisioning-script").Parse(provisionScriptFile))

const (
	// provisionScriptFileName is a path where the provision script stores it's exit code.
	provisionScriptResultPath = "/provision.result"
	provisionLogPrefix        = "[external] "
)

type ProvisionScriptParams struct {
	BusyBox    string
	ResultPath string
	Provider   string
	// DistroSelector is the generated POSIX-sh block that selects the base
	// image's distro profile by its /etc/os-release ID.
	DistroSelector string
}

func getProvisionScript(
	ctx context.Context,
	params ProvisionScriptParams,
) (string, error) {
	var scriptDef bytes.Buffer
	err := ProvisionScriptTemplate.Execute(&scriptDef, params)
	if err != nil {
		return "", fmt.Errorf("error executing provision script: %w", err)
	}
	telemetry.ReportEvent(ctx, "executed provision script env")

	return scriptDef.String(), nil
}

func (bb *BaseBuilder) provisionSandbox(
	ctx context.Context,
	userLogger logger.Logger,
	sandboxConfig *sandbox.Config,
	sandboxRuntime sandboxtypes.RuntimeMetadata,
	localTemplate *sbxtemplate.LocalTemplate,
	rootfsPath string,
	logExternalPrefix string,
) (e error) {
	ctx, childSpan := tracer.Start(ctx, "provision-sandbox")
	defer childSpan.End()

	zapWriter := &zapio.Writer{Log: userLogger.Detach(ctx), Level: zap.DebugLevel}
	prefixedLogsWriter := &writer.PrefixFilteredWriter{Writer: zapWriter, PrefixFilter: logExternalPrefix}
	defer prefixedLogsWriter.Close()

	exitCodeReader, exitCodeWriter := io.Pipe()
	defer exitCodeWriter.Close()

	// read all incoming logs and detect message "{exitPrefix}:X" where X is the exit code
	done := utils.NewErrorOnce()
	go func() (e error) {
		defer io.Copy(io.Discard, exitCodeReader)
		defer func() {
			done.SetError(e)
		}()

		// Rolling tail of the guest's own provisioning output (the
		// prefix-marked lines only — kernel logs are unmarked). The full
		// stream is logged at debug, invisible in customer build logs; on
		// failure the tail is attached to the error, which IS user-visible,
		// so rejections name their real reason ("unsupported base image
		// distribution: …") instead of a bare exit status.
		// 30 lines, because package managers emit per-package error spam that
		// rotated the ROOT error ("No space left on device") out of an
		// 8-line window (observed with yum on Amazon Linux 2).
		const failureTailLines = 30
		var tail []string

		scanner := bufio.NewScanner(exitCodeReader)
		for scanner.Scan() {
			line := scanner.Text()
			if after, ok := strings.CutPrefix(line, rootfs.ProvisioningExitPrefix); ok {
				exitStatus := after
				if exitStatus == "0" {
					// Success exit code
					return nil
				}

				if len(tail) > 0 {
					return fmt.Errorf("exit status: %s; provisioning output tail:\n%s", exitStatus, strings.Join(tail, "\n"))
				}

				return fmt.Errorf("exit status: %s", exitStatus)
			}
			if after, ok := strings.CutPrefix(line, logExternalPrefix); ok {
				if trimmed := strings.TrimSpace(after); trimmed != "" {
					tail = append(tail, trimmed)
					if len(tail) > failureTailLines {
						tail = tail[1:]
					}
				}
			}
		}

		if err := scanner.Err(); err != nil && !errors.Is(err, io.EOF) {
			return err
		}

		return errors.New("exit code not detected")
	}()

	logsWriter := io.MultiWriter(prefixedLogsWriter, exitCodeWriter)

	sbx, err := bb.sandboxFactory.CreateSandbox(
		ctx,
		sandboxConfig,
		sandboxRuntime,
		localTemplate,
		provisionTimeout,
		rootfsPath,
		fc.ProcessOptions{
			// Set the IO Engine explicitly to the default value
			IoEngine: new(layer.DefaultIoEngine),

			InitScriptPath: rootfs.BusyBoxInitPath,
			// Always show kernel logs during the provisioning phase,
			// the sandbox is then started with systemd and without kernel logs.
			KernelLogs: true,

			KvmClock: true,

			// Show provision script logs to the user
			Stdout: logsWriter,
			Stderr: logsWriter,
		},
		nil,
		nil, // no pre-boot hook for provisioning
	)
	if err != nil {
		return fmt.Errorf("error creating sandbox: %w", err)
	}
	defer sbx.Close(ctx)

	defer func() {
		closeErr := bb.proxy.RemoveFromPool(sbx.LifecycleID)
		if closeErr != nil {
			// Errors here will be from forcefully closing the connections, so we can ignore them—they will at worst timeout on their own.
			bb.logger.Warn(ctx, "errors when manually closing connections to sandbox", zap.Error(closeErr))
		} else {
			bb.logger.Debug(
				ctx,
				"removed proxy from pool",
				logger.WithSandboxID(sbx.Runtime.SandboxID),
				logger.WithExecutionID(sbx.Runtime.ExecutionID),
			)
		}
	}()

	if err := done.WaitWithContext(ctx); err != nil {
		return phases.NewPhaseBuildError(bb.Metadata(), fmt.Errorf("error waiting for provisioning sandbox: %w", err))
	}

	userLogger.Info(ctx, "Provisioning was successful, cleaning up")

	err = sbx.Shutdown(ctx)
	if err != nil {
		return fmt.Errorf("error shutting down provisioned sandbox: %w", err)
	}

	err = filesystem.RemoveFile(ctx, rootfsPath, provisionScriptResultPath)
	if err != nil {
		return fmt.Errorf("result file cleanup failed: %w", err)
	}

	userLogger.Info(ctx, "Sandbox template provisioned")

	return nil
}

func (bb *BaseBuilder) enlargeDiskAfterProvisioning(
	ctx context.Context,
	template config.TemplateConfig,
	rootfs *block.Local,
) error {
	rootfsPath := rootfs.Path()

	// Resize rootfs to accommodate for the provisioning script size change
	rootfsFreeSpace, err := filesystem.GetFreeSpace(ctx, rootfsPath, template.RootfsBlockSize())
	if err != nil {
		return fmt.Errorf("error getting free space: %w", err)
	}
	sizeDiff := units.MBToBytes(template.DiskSizeMB) - rootfsFreeSpace
	logger.L().Debug(ctx, "adding provision size diff to rootfs",
		zap.Int64("size_add", sizeDiff),
		zap.Int64("size_free", rootfsFreeSpace),
		zap.Int64("size_target", units.MBToBytes(template.DiskSizeMB)),
	)
	if sizeDiff <= 0 {
		logger.L().Debug(ctx, "no need to enlarge rootfs, skipping")

		return nil
	}
	rootfsFinalSize, err := filesystem.Enlarge(ctx, rootfsPath, sizeDiff)
	if err != nil {
		// Debug filesystem stats on error
		cmd := exec.CommandContext(ctx, "tune2fs", "-l", rootfsPath)
		output, dErr := cmd.Output()
		logger.L().Error(ctx, string(output), zap.Error(dErr))

		return fmt.Errorf("error enlarging rootfs: %w", err)
	}

	// Check the rootfs filesystem corruption
	ext4Check, err := filesystem.CheckIntegrity(ctx, rootfsPath, false)
	if err != nil {
		logger.L().Error(ctx, "final enlarge filesystem ext4 integrity",
			zap.String("result", ext4Check),
			zap.Error(err),
		)

		// Occasionally there are Block bitmap differences. For this reason, we retry with fix.
		ext4Check, err := filesystem.CheckIntegrity(ctx, rootfsPath, true)
		logger.L().Error(ctx, "final enlarge filesystem ext4 integrity - retry with fix",
			zap.String("result", ext4Check),
			zap.Error(err),
		)
		if err != nil {
			return fmt.Errorf("error checking final enlarge filesystem integrity: %w", err)
		}
	} else {
		logger.L().Debug(ctx, "final enlarge filesystem ext4 integrity",
			zap.String("result", ext4Check),
		)
	}

	stat, err := os.Stat(rootfsPath)
	if err != nil {
		return fmt.Errorf("error getting rootfs file info: %w", err)
	}

	// Safety check to ensure the size matches the file size
	if rootfsFinalSize != stat.Size() {
		return fmt.Errorf("size mismatch: expected %d, got %d", rootfsFinalSize, stat.Size())
	}

	err = rootfs.UpdateHeaderSize()
	if err != nil {
		return fmt.Errorf("error updating rootfs header size: %w", err)
	}

	return nil
}
