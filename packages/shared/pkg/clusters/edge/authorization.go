package edge

import (
	"context"

	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
)

// Authorization authenticates a call to a remote cluster's edge and routes it
// to one service instance behind it.
type Authorization struct {
	Token             string
	ServiceInstanceID string
	TLS               bool
}

func (a Authorization) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	return map[string]string{consts.EdgeRpcAuthHeader: a.Token, consts.EdgeRpcServiceInstanceIDHeader: a.ServiceInstanceID}, nil
}

func (a Authorization) RequireTransportSecurity() bool {
	return a.TLS
}
