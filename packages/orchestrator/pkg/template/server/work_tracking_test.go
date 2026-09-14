//go:build linux

package server

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/launchdarkly/go-sdk-common/v3/ldcontext"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/service"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/buildlogger"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/cache"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	templatemanager "github.com/e2b-dev/infra/packages/shared/pkg/grpc/template-manager"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/templates"
)

func newWorkTrackingServer(t *testing.T, fcVersion string) (*ServerStore, *storage.MockStorageProvider, *templatemanager.TemplateCreateRequest) {
	t.Helper()

	td := ldtestdata.DataSource()
	td.Update(td.Flag(featureflags.BuildFirecrackerVersion.Key()).ValueForAll(ldvalue.String(fcVersion)))
	td.Update(td.Flag(featureflags.BuildEnvdVersion.Key()).ValueForAll(ldvalue.String("")))
	flags, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, flags.Close(context.WithoutCancel(t.Context()))) })
	provider := storage.NewMockStorageProvider(t)
	meter := noop.NewMeterProvider()
	buildMetrics, err := metrics.NewBuildMetrics(meter)
	require.NoError(t, err)
	l := logger.NewNopLogger()
	s := &ServerStore{
		info:            &service.ServiceInfo{},
		logger:          l,
		buildLogger:     l,
		featureFlags:    flags,
		buildCache:      cache.NewBuildCache(t.Context(), meter),
		templateStorage: provider,
		wg:              &sync.WaitGroup{},
		builder: build.NewBuilder(
			cfg.BuilderConfig{HostEnvdPath: filepath.Join(t.TempDir(), "missing-envd")},
			l, flags, nil, provider, nil, nil, nil, nil, nil, buildMetrics, nil,
		),
	}
	t.Cleanup(s.wg.Wait)

	return s, provider, &templatemanager.TemplateCreateRequest{
		Version: new(templates.TemplateV2BetaVersion),
		Template: &templatemanager.TemplateConfig{
			TemplateID: "template-id", BuildID: "build-id", TeamID: "team-id",
			Source: &templatemanager.TemplateConfig_FromImage{FromImage: "alpine:latest"},
		},
	}
}

func blockTemplateCleanup(t *testing.T, provider *storage.MockStorageProvider, buildID string, err error) (<-chan struct{}, func()) {
	t.Helper()

	started, unblock := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(unblock) })
	t.Cleanup(release)
	provider.EXPECT().DeleteObjectsWithPrefix(mock.Anything, buildID).
		RunAndReturn(func(context.Context, string) error {
			close(started)
			<-unblock

			return err
		}).Once()

	return started, release
}

func TestTemplateCreateRejectionReleasesWork(t *testing.T) {
	t.Parallel()

	noSource := func(req *templatemanager.TemplateCreateRequest) { req.Template.Source = nil }
	emptyImage := func(req *templatemanager.TemplateCreateRequest) {
		req.Template.Source = &templatemanager.TemplateConfig_FromImage{FromImage: ""}
	}

	for _, tc := range []struct {
		name, fcVersion, message string
		duplicate                bool
		mutate                   func(*templatemanager.TemplateCreateRequest)
		code                     codes.Code
	}{
		{"invalid firecracker", "invalid", "invalid resolved firecracker version", false, nil, codes.OK},
		{"duplicate build", featureflags.DefaultFirecrackerVersion, "already exists in cache", true, nil, codes.OK},
		{"no source", featureflags.DefaultFirecrackerVersion, "requires either fromImage or fromTemplate", false, noSource, codes.InvalidArgument},
		{"empty fromImage", featureflags.DefaultFirecrackerVersion, "requires either fromImage or fromTemplate", false, emptyImage, codes.InvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s, _, req := newWorkTrackingServer(t, tc.fcVersion)
			s.featureFlags.RegisterContextProvider(func(context.Context) ldcontext.Context {
				assert.Equal(t, int64(1), s.info.OutstandingWork())

				return ldcontext.New("work-tracking-test")
			})
			if tc.duplicate {
				_, err := s.buildCache.Create(req.GetTemplate().GetTeamID(), req.GetTemplate().GetBuildID(), buildlogger.NewLogEntryLogger())
				require.NoError(t, err)
			}
			if tc.mutate != nil {
				tc.mutate(req)
			}
			_, err := s.TemplateCreate(t.Context(), req)
			require.ErrorContains(t, err, tc.message)
			if tc.code != codes.OK {
				require.Equal(t, tc.code, status.Code(err))
				_, err := s.buildCache.Get(req.GetTemplate().GetBuildID())
				require.Error(t, err, "a rejected build must not enter the cache")
			}
			s.wg.Wait()
			require.Zero(t, s.info.OutstandingWork())
		})
	}
}

func TestTemplateCreateTracksCleanupAfterCallerCancellation(t *testing.T) {
	t.Parallel()

	s, provider, req := newWorkTrackingServer(t, featureflags.DefaultFirecrackerVersion)
	started, release := blockTemplateCleanup(t, provider, req.GetTemplate().GetBuildID(), nil)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	_, err := s.TemplateCreate(ctx, req)
	require.NoError(t, err)
	require.Equal(t, int64(1), s.info.OutstandingWork())
	cancel()
	<-started
	info, err := s.buildCache.Get(req.GetTemplate().GetBuildID())
	require.NoError(t, err)
	require.Equal(t, templatemanager.TemplateBuildState_Building, info.GetStatus())
	require.Equal(t, int64(1), s.info.OutstandingWork())
	release()
	s.wg.Wait()
	require.Zero(t, s.info.OutstandingWork())
	require.Equal(t, templatemanager.TemplateBuildState_Failed, info.GetStatus())
}

func TestTemplateBuildDeleteTracksCleanupWithoutReleasingBuild(t *testing.T) {
	t.Parallel()

	s, provider, req := newWorkTrackingServer(t, featureflags.DefaultFirecrackerVersion)
	buildStarted, releaseBuild := blockTemplateCleanup(t, provider, req.GetTemplate().GetBuildID(), nil)
	_, err := s.TemplateCreate(t.Context(), req)
	require.NoError(t, err)
	<-buildStarted
	deleteErr := errors.New("storage deletion failed")
	deleteStarted, releaseDelete := blockTemplateCleanup(t, provider, req.GetTemplate().GetBuildID(), deleteErr)
	deleted := make(chan error, 1)
	go func() {
		_, err := s.TemplateBuildDelete(t.Context(), &templatemanager.TemplateBuildDeleteRequest{
			TemplateID: req.GetTemplate().GetTemplateID(), BuildID: req.GetTemplate().GetBuildID(),
		})
		deleted <- err
	}()
	<-deleteStarted
	info, err := s.buildCache.Get(req.GetTemplate().GetBuildID())
	require.NoError(t, err)
	require.Equal(t, templatemanager.TemplateBuildState_Failed, info.GetStatus())
	require.Equal(t, int64(2), s.info.OutstandingWork())
	releaseDelete()
	require.ErrorIs(t, <-deleted, deleteErr)
	require.Equal(t, int64(1), s.info.OutstandingWork())
	releaseBuild()
	s.wg.Wait()
	require.Zero(t, s.info.OutstandingWork())
}

func TestTemplateBuildDeleteValidationReleasesWork(t *testing.T) {
	t.Parallel()

	s, _, _ := newWorkTrackingServer(t, featureflags.DefaultFirecrackerVersion)
	for _, req := range []*templatemanager.TemplateBuildDeleteRequest{
		{TemplateID: "template-id"}, {BuildID: "build-id"},
	} {
		_, err := s.TemplateBuildDelete(t.Context(), req)
		require.ErrorContains(t, err, "template id and build id are required fields")
		s.wg.Wait()
		require.Zero(t, s.info.OutstandingWork())
	}
}

type workTrackingSyncCore struct {
	zapcore.Core

	syncFunc func() error
}

func (c workTrackingSyncCore) With([]zapcore.Field) zapcore.Core { return c }
func (c workTrackingSyncCore) Sync() error                       { return c.syncFunc() }

func TestTemplateCreateTracksLogSyncAndPanicRecovery(t *testing.T) {
	t.Parallel()

	for name, panicOnSync := range map[string]bool{"sync": false, "panic": true} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			s, provider, req := newWorkTrackingServer(t, featureflags.DefaultFirecrackerVersion)
			started, release := blockTemplateCleanup(t, provider, req.GetTemplate().GetBuildID(), nil)
			synced := false
			s.buildLogger = logger.NewTracedLoggerFromCore(workTrackingSyncCore{
				Core: zapcore.NewNopCore(),
				syncFunc: func() error {
					synced = true
					assert.Equal(t, int64(1), s.info.OutstandingWork())
					if panicOnSync {
						panic("log sync failed")
					}

					return nil
				},
			})
			_, err := s.TemplateCreate(t.Context(), req)
			require.NoError(t, err)
			<-started
			release()
			s.wg.Wait()
			require.True(t, synced)
			require.Zero(t, s.info.OutstandingWork())
			info, err := s.buildCache.Get(req.GetTemplate().GetBuildID())
			require.NoError(t, err)
			require.Equal(t, templatemanager.TemplateBuildState_Failed, info.GetStatus())
		})
	}
}
