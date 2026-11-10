//go:build linux

package main

import (
	"os"
	"syscall"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

// suppressNoisyLogs leaves the global zap logger at ERROR, silencing OTEL and
// LaunchDarkly output; the standard log stays on for fatal errors.
func suppressNoisyLogs() {
	cfg := zap.NewProductionConfig()
	cfg.Level = zap.NewAtomicLevelAt(zapcore.ErrorLevel)
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	errLogger, err := cfg.Build()
	if err == nil {
		zap.ReplaceGlobals(errLogger)
	}
}

func headerInfo(headerPath string) (totalSize, blockSize uint64) {
	data, err := os.ReadFile(headerPath)
	if err != nil {
		return 0, 0
	}
	h, err := header.DeserializeBytes(data)
	if err != nil {
		return 0, 0
	}

	return h.Metadata.Size, h.Metadata.BlockSize
}

// actualFileSize is the size a file takes on disk; st_blocks counts 512-byte
// units whatever the filesystem's block size.
func actualFileSize(path string) (int64, error) {
	var stat syscall.Stat_t
	if err := syscall.Stat(path, &stat); err != nil {
		return 0, err
	}

	return stat.Blocks * 512, nil
}

type artifactInfo struct {
	Name       string
	File       string
	HeaderFile string
}

func mainArtifacts() []artifactInfo {
	return []artifactInfo{
		{"Rootfs", storage.RootfsName, storage.RootfsName + storage.HeaderSuffix},
		{"Memfile", storage.MemfileName, storage.MemfileName + storage.HeaderSuffix},
	}
}

func smallArtifacts() []struct{ Name, File string } {
	return []struct{ Name, File string }{
		{"Rootfs header", storage.RootfsName + storage.HeaderSuffix},
		{"Memfile header", storage.MemfileName + storage.HeaderSuffix},
		{"Snapfile", storage.SnapfileName},
		{"Metadata", storage.MetadataName},
	}
}
