package process

import (
	"google.golang.org/protobuf/proto"

	rpc "github.com/e2b-dev/infra/packages/envd/internal/services/spec/process"
)

const redactedValue = "<redacted>"

// redact returns the copy of msg to log, with env var values replaced.
func redact(msg any) any {
	switch m := msg.(type) {
	case *rpc.StartRequest:
		if len(m.GetProcess().GetEnvs()) == 0 {
			return msg
		}

		c := proto.Clone(m).(*rpc.StartRequest)
		redactEnvValues(c.GetProcess())

		return c
	case *rpc.ListResponse:
		c := proto.Clone(m).(*rpc.ListResponse)
		for _, p := range c.GetProcesses() {
			redactEnvValues(p.GetConfig())
		}

		return c
	}

	return msg
}

func redactEnvValues(config *rpc.ProcessConfig) {
	for name := range config.GetEnvs() {
		config.Envs[name] = redactedValue
	}
}
