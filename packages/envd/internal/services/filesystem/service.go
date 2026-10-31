package filesystem

import (
	"sync"

	"connectrpc.com/connect"
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"

	"github.com/e2b-dev/infra/packages/envd/internal/execcontext"
	"github.com/e2b-dev/infra/packages/envd/internal/logs"
	"github.com/e2b-dev/infra/packages/envd/internal/services/legacy"
	spec "github.com/e2b-dev/infra/packages/envd/internal/services/spec/filesystem/filesystemconnect"
	"github.com/e2b-dev/infra/packages/envd/internal/services/streaming"
	"github.com/e2b-dev/infra/packages/envd/internal/utils"
)

type Service struct {
	logger   *zerolog.Logger
	watchers *utils.Map[string, *FileWatcher]
	defaults *execcontext.Defaults
	// watchersMu serializes watcher membership changes (CreateWatcher/
	// RemoveWatcher) and the event drain (GetWatcherEvents) against the
	// live-upgrade handover snapshot (ExportWatchers/ExportWatchersHold/
	// ImportWatchers), so the exported set is a consistent view, a watcher
	// created/removed concurrently isn't dropped or resurrected across the swap,
	// and no event is drained after the snapshot carried it (double-delivery).
	// A pointer so it stays shared across the value-type Service's copies.
	watchersMu *sync.Mutex
}

func Handle(server *chi.Mux, l *zerolog.Logger, defaults *execcontext.Defaults) Service {
	service := Service{
		logger:     l,
		watchers:   utils.NewMap[string, *FileWatcher](),
		defaults:   defaults,
		watchersMu: &sync.Mutex{},
	}

	interceptors := connect.WithInterceptors(
		logs.NewUnaryLogInterceptor(l),
		legacy.Convert(),
		streaming.DisableProxyBuffering(),
	)

	path, handler := spec.NewFilesystemHandler(service, interceptors)

	server.Mount(path, handler)

	// Returned so the live-upgrade handover (main.go) can export/import watcher
	// state. Service is a value type but its watchers map is a shared pointer.
	return service
}
