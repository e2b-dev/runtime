// Command dummy-orchestrator is a non-functional implementation of the
// orchestrator gRPC surface used for local API development.
//
// It compiles and runs on every supported platform (most importantly macOS,
// where the real Linux-only orchestrator cannot be built), implements the
// orchestrator.proto SandboxService against an in-memory store, and returns
// codes.Unimplemented for Chunk/Volume calls. It is meant only for wiring
// up the api package end-to-end without bringing up firecracker, NBD, NFS,
// cgroups, etc.
package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/dummyserver"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// defaultVersion identifies the dummy orchestrator binary so ServiceInfo
// responses are distinguishable from real orchestrators.
const defaultVersion = "0.0.0-dummy"

// commitSHA is populated by the linker via -ldflags on build.
var commitSHA string

func main() {
	grpcPort := envOrDefault("GRPC_PORT", "5008")
	healthPort := envOrDefault("HEALTH_PORT", "5018")
	nodeID := envOrDefault("NODE_ID", "dummy")
	labels := splitCSV(os.Getenv("NODE_LABELS"))
	// A state dir keeps a Standby across restarts; without one nothing persists.
	stateDir := os.Getenv("STATE_DIR")
	maxSandboxes, err := strconv.ParseInt(envOrDefault("MAX_SANDBOXES", "100"), 10, 64)
	if err != nil {
		log.Fatalf("invalid MAX_SANDBOXES: %v", err)
	}

	version := envOrDefault("SERVICE_VERSION", defaultVersion)
	commit := commitSHA
	if commit == "" {
		commit = "unknown"
	}

	if _, err := strconv.ParseUint(grpcPort, 10, 16); err != nil {
		log.Fatalf("invalid GRPC_PORT %q: %v", grpcPort, err)
	}
	if _, err := strconv.ParseUint(healthPort, 10, 16); err != nil {
		log.Fatalf("invalid HEALTH_PORT %q: %v", healthPort, err)
	}

	machine, err := dummyserver.NewMachine(context.Background(), stateDir)
	if machine == nil {
		log.Fatalf("state dir %q: %v", stateDir, err)
	}
	if err != nil {
		log.Printf("ignoring the stored service status: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// The status machine logs through the shared logger.
	devLogger, err := logger.NewDevelopmentLogger()
	if err != nil {
		cancel()
		//nolint:gocritic // cancel() is called explicitly above before log.Fatalf
		log.Fatalf("logger: %v", err)
	}
	restoreLogger := logger.ReplaceGlobals(ctx, devLogger)
	defer restoreLogger()

	var lc net.ListenConfig
	grpcLis, err := lc.Listen(ctx, "tcp", ":"+grpcPort)
	if err != nil {
		cancel()
		//nolint:gocritic // cancel() is called explicitly above before log.Fatalf
		log.Fatalf("failed to listen on gRPC port %s: %v", grpcPort, err)
	}

	srv := grpc.NewServer()

	sbxServer := dummyserver.NewSandbox(machine, maxSandboxes)
	orchestrator.RegisterSandboxServiceServer(srv, sbxServer)
	orchestrator.RegisterChunkServiceServer(srv, &orchestrator.UnimplementedChunkServiceServer{})
	orchestrator.RegisterVolumeServiceServer(srv, &orchestrator.UnimplementedVolumeServiceServer{})

	serviceID := uuid.NewString()
	orchestratorinfo.RegisterInfoServiceServer(srv, dummyserver.NewInfo(dummyserver.InfoOptions{
		NodeID: nodeID, ServiceID: serviceID, Version: version, Commit: commit, Labels: labels,
		Machine: machine, MaxSandboxes: maxSandboxes, SandboxCount: sbxServer.Count,
	}))

	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthSrv.SetServingStatus("SandboxService", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(srv, healthSrv)

	// Plain-HTTP /health endpoint so the integration test harness
	// (scripts/start-service.sh polls http://localhost:<grpc-port-or-similar>/health)
	// can detect readiness. Served on a separate port to avoid the cmux
	// machinery the real orchestrator uses.
	httpMux := http.NewServeMux()
	httpMux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	httpSrv := &http.Server{
		Addr:              ":" + healthPort,
		Handler:           httpMux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("dummy orchestrator listening grpc=:%s health=:%s nodeID=%s serviceID=%s clientID=%s version=%s commit=%s",
		grpcPort, healthPort, nodeID, serviceID, sbxServer.ClientID(), version, commit)

	grpcErr := make(chan error, 1)
	go func() {
		grpcErr <- srv.Serve(grpcLis)
	}()

	httpErr := make(chan error, 1)
	go func() {
		err := httpSrv.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		httpErr <- err
	}()

	shutdown := func() {
		log.Printf("shutting down")

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()

		if err := httpSrv.Shutdown(shutdownCtx); err != nil {
			log.Printf("http server shutdown error: %v", err)
		}

		srv.GracefulStop()
	}

	var grpcDone, httpDone bool

	select {
	case <-ctx.Done():
		shutdown()
	case err := <-grpcErr:
		grpcDone = true
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Printf("grpc server crashed: %v", err)
		}
		shutdown()
	case err := <-httpErr:
		httpDone = true
		if err != nil {
			log.Printf("http server crashed: %v", err)
		}
		shutdown()
	}

	// Drain remaining goroutines (only those that haven't sent yet).
	if !grpcDone {
		if err := <-grpcErr; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Printf("grpc server returned an error: %v", err)
		}
	}
	if !httpDone {
		if err := <-httpErr; err != nil {
			log.Printf("http server returned an error: %v", err)
		}
	}
}

func envOrDefault(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}

	return def
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}

	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}

	return out
}
