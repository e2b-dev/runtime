module github.com/e2b-dev/infra/packages/envd

go 1.26.10

replace github.com/e2b-dev/infra/packages/shared v0.0.0 => ../shared

tool github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen

require (
	connectrpc.com/authn v0.1.0
	connectrpc.com/connect v1.18.1
	connectrpc.com/cors v0.1.0
	github.com/awnumar/memguard v0.23.0
	github.com/creack/pty v1.1.24
	github.com/e2b-dev/fsnotify v0.0.2
	github.com/e2b-dev/infra/packages/shared v0.0.0
	github.com/getkin/kin-openapi v0.152.0
	github.com/go-chi/chi/v5 v5.5.0
	github.com/google/uuid v1.6.0
	github.com/oapi-codegen/runtime v1.8.0
	github.com/prometheus/procfs v0.20.1
	github.com/rs/cors v1.11.1
	github.com/rs/zerolog v1.35.1
	github.com/shirou/gopsutil/v4 v4.26.6
	github.com/stretchr/testify v1.12.1
	github.com/txn2/txeh v1.8.1
	golang.org/x/sync v0.24.0
	golang.org/x/sys v0.49.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/apapsch/go-jsonmerge/v2 v2.0.0 // indirect
	github.com/awnumar/memcall v0.4.0 // indirect
	github.com/dchest/uniuri v1.2.0 // indirect
	github.com/dprotaso/go-yit v0.0.0-20220510233725-9ba8df137936 // indirect
	github.com/ebitengine/purego v0.10.1 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/go-openapi/jsonpointer v1.0.1 // indirect
	github.com/go-openapi/testify/v2 v2.8.0 // indirect
	github.com/gofrs/uuid/v5 v5.5.1 // indirect
	github.com/jackc/pgx/v5 v5.11.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20260330125221-c963978e514e // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	github.com/oapi-codegen/oapi-codegen/v2 v2.8.0 // indirect
	github.com/oasdiff/yaml v0.1.1 // indirect
	github.com/oasdiff/yaml3 v0.0.14 // indirect
	github.com/onsi/ginkgo v1.16.5 // indirect
	github.com/onsi/gomega v1.38.2 // indirect
	github.com/orcaman/concurrent-map/v2 v2.0.1 // indirect
	github.com/pkg/xattr v0.4.12 // indirect
	github.com/power-devops/perfstat v0.0.0-20240221224432-82ca36839d55 // indirect
	github.com/santhosh-tekuri/jsonschema/v6 v6.0.3 // indirect
	github.com/sergi/go-diff v1.4.0 // indirect
	github.com/speakeasy-api/jsonpath v0.6.3 // indirect
	github.com/speakeasy-api/openapi v1.24.0 // indirect
	github.com/stretchr/objx v0.5.3 // indirect
	github.com/sumup/typeid v0.8.0 // indirect
	github.com/tklauser/go-sysconf v0.4.0 // indirect
	github.com/tklauser/numcpus v0.12.0 // indirect
	github.com/vmware-labs/yaml-jsonpath v0.3.2 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/mod v0.42.0 // indirect
	golang.org/x/text v0.43.0 // indirect
	golang.org/x/tools v0.51.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
