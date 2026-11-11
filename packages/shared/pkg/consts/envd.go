package consts

const (
	DefaultEnvdServerPort int64 = 49983

	// EnvdMaxInitBodyBytes is the per-request body cap envd enforces at its server
	// edge (bodylimit middleware) on POST /init and every other non-streaming path.
	// It is the one authoritative ceiling: the public API derives its own env-var
	// admission bound from it (MaxSandboxEnvVarsBytes) so a sandbox the API admits
	// cannot have its env map pushed past this cap once the orchestrator serialises
	// the full /init body. Keep envd bodylimit.DefaultMaxBytes tied to this value.
	EnvdMaxInitBodyBytes int64 = 1 << 20 // 1 MiB

	// EnvdInitBodyReservedBytes is the slice of the /init body the public API cannot
	// see at admission time and must leave room for: the egress-proxy CA bundle, the
	// envd access token, default user/workdir, volume mounts, the lifecycle ID, and
	// JSON framing. 512 KiB is far above what those fields ever occupy (a CA bundle
	// is a few KiB), so the remaining budget for user env vars stays generous while
	// the full serialised body provably stays under EnvdMaxInitBodyBytes.
	EnvdInitBodyReservedBytes int64 = 1 << 19 // 512 KiB

	// MaxSandboxEnvVarsBytes caps the JSON-serialised size of a sandbox's user env map
	// at the public API boundary. It is EnvdMaxInitBodyBytes minus the reserve above,
	// so an admitted sandbox's env vars can never be the reason its envd /init body is
	// rejected with 413 -- the failure the orchestrator treats as a fatal create or
	// resume. openapi.yml documents this cap on the EnvVars schema.
	MaxSandboxEnvVarsBytes int64 = EnvdMaxInitBodyBytes - EnvdInitBodyReservedBytes

	// SystemTag opts a process into envd's root cgroup (no user/pty/socat).
	// Used for maintenance commands that must outlive cgroup freezing.
	SystemTag = "_system"

	// TemplateDefaultUser is the user a template build makes envd's default.
	//
	// One definition, because two independent places have to agree on it and the
	// agreement is load-bearing: the finalize phase sends this literal for any build
	// below templates.TemplateV2ReleaseVersion and the recorded Context.User at or
	// above it, so this is the only value BOTH branches produce. A host reconstructing
	// what a build sent can therefore trust exactly this value and nothing else. Two
	// copies that drift would make the reconstruction re-send a user finalize never
	// sent.
	TemplateDefaultUser = "user"
)
