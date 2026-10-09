// Package mtls terminates mutual TLS on a service's existing port, in a mode
// that can change at runtime, with plaintext and TLS side by side until a
// listener is told otherwise.
//
// # Attach points
//
// A gRPC server built with the shared constructor takes ServerCredentials
// through its WithTransportCredentials option and the two interceptors
// through WithUnaryInterceptors and WithStreamInterceptors:
//
//	cfg := mtls.ServerConfig{Name: "grpc-internal", Files: files, Mode: mode, Allow: allow}
//	server := e2bgrpc.NewGRPCServer(tel,
//		e2bgrpc.WithTransportCredentials(mtls.NewServerCredentials(cfg)),
//		e2bgrpc.WithUnaryInterceptors(mtls.UnaryServerInterceptor(cfg)),
//		e2bgrpc.WithStreamInterceptors(mtls.StreamServerInterceptor(cfg)))
//
// An HTTP server is served through Listener, with HTTPGuard wrapped around
// the application handler before any h2c configuration, so every HTTP/2
// stream passes the guard. The guard takes the listener's ConnState, which
// carries each connection's chain verdict, something r.TLS cannot tell it;
// without it a required listener treats every TLS request as unverified:
//
//	l := mtls.NewListener(inner, cfg)
//	server.Handler = mtls.NewHTTPGuard(cfg, []string{"/health"}, app, mtls.WithGuardConnStateLookup(l.ConnState))
//	httpserver.ConfigureH2C(server)
//	err := server.Serve(l)
//
// Both attach points read the first byte of a connection: 0x16 starts a TLS
// handshake, anything else is plaintext. The gRPC branch negotiates ALPN h2,
// as grpc-go requires; the HTTP branch offers http/1.1 by default and hands
// the *tls.Conn to net/http unwrapped so r.TLS is populated. Classification
// and the handshake share one deadline, DefaultHandshakeTimeout, so a silent
// or stalled connection delays nobody.
//
// A client hop takes ClientCredentials in place of insecure credentials:
//
//	hop := mtls.ClientConfig{Name: "api-to-store", Files: files, Mode: hopMode,
//		ServerName: "store.platform.svc.cluster.local", ExpectedServerIDs: expected}
//	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(mtls.NewClientCredentials(hop)))
//
// # Modes
//
// A listener runs ModeOff, ModePermissive or ModeRequired; a client hop runs
// ClientOff or ClientOn. Off is today's behaviour: plaintext, no checks.
// Permissive accepts TLS and plaintext, runs every check and counts what it
// would refuse. Required accepts TLS from an allow-listed name and lets
// plaintext reach only the health paths and the gRPC health service. The
// mode is read on every handshake, request and RPC through a ModeSource:
// StaticMode for a value from the environment (ModeFromEnv), or a
// FlagModeSource over a FlagReader for a feature flag with the environment
// value as fallback. A flag that is missing or malformed keeps the last
// accepted value, and a flip to required is refused while the allow-list is
// empty. A required mode that any source yields while the list is empty is
// served as permissive and logged once, so a static mode from the environment
// and a flag fallback of required behave the same.
//
// # Identity
//
// Every peer is verified in tls.Config.VerifyConnection, which Go runs on
// resumed sessions too: the chain against the roots loaded at that moment,
// then exactly one URI SAN with the spiffe scheme, then the AllowList. The
// allow-list is re-checked on every request and RPC, so a removed name is
// refused at its next call; the connection itself ends at the server's
// maximum connection age. The chain verdict is kept with the connection:
// after a flip to required, a TLS connection admitted under permissive with
// a chain that did not verify is treated as plaintext, health paths only,
// until the service closes it. Handlers read the caller through
// PeerFromContext; Peer.ID is the name the certificate carried and is
// authenticated only when Peer.ChainVerified is set, so under off and
// permissive a handler must not authorize on the ID alone.
//
// # Files
//
// Files reads the certificate, key and trust bundle named by FileConfig
// (E2B_TLS_CERT_FILE, E2B_TLS_KEY_FILE and E2B_TLS_CA_FILE through
// FileConfigFromEnv) and re-reads them every minute. A new set replaces the
// old only when the key matches the certificate and every PEM block parses;
// otherwise the previous set keeps serving and the failure is counted. The
// process starts whatever the files hold; expiry is a metric, never fatal.
//
// # Metrics
//
// Metrics records, on the meter named after this package and with the
// e2b_mtls_ prefix, handshakes by outcome, refusals and would-be refusals by
// reason, plaintext requests and open plaintext connections, mTLS connections
// by caller, the mode in force per listener and client hop with its source,
// the certificate's and chain's expiry, reloads by outcome and the loaded
// roots by fingerprint. Pass Metrics explicitly in tests; DefaultMetrics
// builds them on the global provider otherwise. The caller attribute is
// bounded by the allow-list, since only an admitted name is counted; a
// wildcard entry makes it one series per ServiceAccount it matches. The
// chain expiry is the earliest not-after among the leaf and the
// intermediates in the certificate file, which is how the intermediate's
// expiry is reported; no fingerprint is emitted for the intermediate, only
// for the roots.
//
// # A port shared through a multiplexer
//
// When a Listener sits in front of a multiplexer that wraps connections in
// its own type, the servers behind it cannot see the *tls.Conn. The listener
// remembers what it established per remote address; hand
// (*Listener).ConnState to the gRPC credentials through WithConnStateLookup
// and to the guard through WithGuardConnStateLookup. A port that carries
// gRPC offers h2 as well, since grpc-go clients require it:
//
//	l := mtls.NewListener(inner, cfg, mtls.WithNextProtos("h2", "http/1.1"))
//
// # Closing connections the policy no longer admits
//
// Neither attach point ends a connection on its own when the policy changes:
// a plaintext connection opened under permissive keeps serving health paths
// after a flip to required, and so does a TLS connection whose chain did
// not verify; a name removed from the allow-list is refused at every request
// or RPC but keeps its connection. Listener and ServerCredentials record the
// connections they admitted, in every mode. CloseUnverified closes every
// connection a required handshake would refuse now, plaintext, unverified
// TLS and a name the allow-list does not admit alike, which a service calls
// when its mode flips to required. CloseByPeer
// closes every TLS connection of one name, which is how a removed name's
// keep-alive HTTP connection ends, since http.Server has no connection age.
// gRPC connections also end at the server's maximum connection age.
package mtls
