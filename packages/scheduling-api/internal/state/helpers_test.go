package state_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/e2b-dev/infra/packages/scheduling-api/internal/state"
	statepb "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/scheduling/state"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxconfig"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
)

const hangTimeout = 30 * time.Second

func newClient(t *testing.T, storage sandboxtypes.Storage) statepb.StateServiceClient {
	t.Helper()

	return newClientWithWait(t, storage, hangTimeout)
}

func newClientWithWait(t *testing.T, storage sandboxtypes.Storage, maxWait time.Duration) statepb.StateServiceClient {
	t.Helper()

	return statepb.NewStateServiceClient(newConnWithWait(t, storage, maxWait))
}

func newConnWithWait(t *testing.T, storage sandboxtypes.Storage, maxWait time.Duration, options ...grpc.DialOption) *grpc.ClientConn {
	t.Helper()

	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	statepb.RegisterStateServiceServer(server, state.NewServer(storage, maxWait))
	go func() {
		assert.NoError(t, server.Serve(listener))
	}()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient("passthrough:///bufconn", append([]grpc.DialOption{
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, options...)...)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, conn.Close()) })

	return conn
}

func callContext(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), hangTimeout)
	t.Cleanup(cancel)

	return ctx
}

func requireCode(t *testing.T, err error, code codes.Code) *status.Status {
	t.Helper()

	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok, "not a status error: %v", err)
	require.Equal(t, code, st.Code(), st.Message())

	return st
}

func encode(t *testing.T, sbx sandboxtypes.Sandbox) []byte {
	t.Helper()

	data, err := json.Marshal(sbx)
	require.NoError(t, err)

	return data
}

func decode(t *testing.T, data []byte) sandboxtypes.Sandbox {
	t.Helper()

	var sbx sandboxtypes.Sandbox
	require.NoError(t, json.Unmarshal(data, &sbx))

	return sbx
}

func minimalRecord(teamID uuid.UUID, sandboxID string) sandboxtypes.Sandbox {
	now := time.Now().UTC().Truncate(time.Second)

	return sandboxtypes.Sandbox{
		SandboxID:   sandboxID,
		TeamID:      teamID,
		ExecutionID: uuid.NewString(),
		StartTime:   now,
		EndTime:     now.Add(time.Hour),
		State:       sandboxtypes.StateRunning,
	}
}

func fullRecord(teamID uuid.UUID, sandboxID string) sandboxtypes.Sandbox {
	at := func(offset time.Duration) time.Time {
		return time.Date(2026, 11, 13, 10, 11, 12, 123456789, time.UTC).Add(offset)
	}
	str := func(s string) *string { return &s }
	yes := true
	pausedTTL := 90 * time.Minute

	return sandboxtypes.Sandbox{
		SandboxID:                  sandboxID,
		TemplateID:                 "template",
		ClientID:                   "client",
		Alias:                      str("alias"),
		Domain:                     str("example.test"),
		ExecutionID:                uuid.NewString(),
		TeamID:                     teamID,
		BuildID:                    uuid.New(),
		BaseTemplateID:             "base-template",
		Metadata:                   map[string]string{"key": "value", "other": "x"},
		MaxInstanceLength:          24 * time.Hour,
		StartTime:                  at(0),
		EndTime:                    time.Now().UTC().Add(time.Hour).Truncate(time.Second).Add(987654321),
		VCpu:                       2,
		TotalDiskSizeMB:            1024,
		RamMB:                      512,
		MemoryMB:                   1024,
		PlacedMemoryMB:             2048,
		RequiredFeatures:           []string{"memory-resize"},
		KernelVersion:              "kernel",
		FirecrackerVersion:         "firecracker",
		FirecrackerVersionResolved: true,
		EnvdVersion:                "envd",
		EnvdAccessToken:            str("envd-token"),
		TrafficAccessToken:         str("traffic-token"),
		AllowInternetAccess:        &yes,
		NodeID:                     "node",
		ClusterID:                  uuid.New(),
		AutoPause:                  true,
		RefusedUntil:               at(time.Minute),
		RefusedSince:               at(2 * time.Minute),
		EnvdRefusedSince:           at(3 * time.Minute),
		EnvdRefusedUntil:           at(4 * time.Minute),
		AutoPauseFilesystemOnly:    true,
		AutoResume:                 &sandboxconfig.SandboxAutoResumeConfig{Policy: sandboxconfig.SandboxAutoResumeAny, Timeout: 30},
		Network: &sandboxconfig.SandboxNetworkConfig{
			Egress: &sandboxconfig.SandboxNetworkEgressConfig{
				AllowedAddresses: []string{"10.0.0.1", "example.test"},
				DeniedAddresses:  []string{"0.0.0.0/0"},
				Rules: map[string][]sandboxconfig.SandboxNetworkRule{
					"example.test": {{Transform: &sandboxconfig.SandboxNetworkTransform{Headers: map[string]string{"X-Header": "value"}}}},
				},
				DnsServers:          &[]string{},
				EgressProxyAddress:  "proxy.test:1080",
				EgressProxyUsername: "user",
				EgressProxyPassword: "password",
				EgressProxyTLS:      &sandboxconfig.SandboxEgressProxyTLSConfig{Enabled: true, ServerName: "proxy.test", CACert: "cert"},
			},
			Ingress: &sandboxconfig.SandboxNetworkIngressConfig{
				AllowPublicAccess: &yes,
				MaskRequestHost:   str("masked.test"),
				HTTPSPorts:        []uint32{443, 8443},
			},
		},
		VolumeMounts: []*sandboxconfig.SandboxVolumeMountConfig{{ID: "volume", Type: "type", Name: "name", Path: "/mnt"}},
		Iam: &sandboxconfig.SandboxIam{Tokens: map[string]sandboxconfig.SandboxIamToken{
			"token": {Audience: "audience", TokenType: "type"},
		}},
		PausedTTL:       &pausedTTL,
		PausedTTLStored: true,
		NetworkPlacement: &orchestrator.SandboxNetworkPlacement{
			ExecutionId:    "placement-execution",
			NetworkMode:    "mode",
			RouterId:       "router",
			IngressAddress: "10.0.0.2",
			IngressPort:    5000,
		},
		State:              sandboxtypes.StateRunning,
		LifecycleHold:      "hold",
		LifecycleHoldUntil: 1,
	}
}

var emptyAllowed = map[string]bool{"Network.Egress.DnsServers": true}

func requireNoZeroField(t *testing.T, value any) {
	t.Helper()

	var zero []string
	walkNonZero(reflect.ValueOf(value), reflect.TypeOf(value).Name(), "", &zero)
	require.Empty(t, zero, "fields left zero; set them so the round trip covers them")
}

func walkNonZero(v reflect.Value, label, path string, zero *[]string) {
	if v.IsZero() {
		*zero = append(*zero, label)

		return
	}

	switch v.Kind() {
	case reflect.Pointer:
		walkNonZero(v.Elem(), label, path, zero)
	case reflect.Struct:
		if v.Type() == reflect.TypeFor[time.Time]() {
			return
		}
		for i := range v.NumField() {
			field := v.Type().Field(i)
			if !field.IsExported() {
				continue
			}
			fieldPath := field.Name
			if path != "" {
				fieldPath = path + "." + field.Name
			}
			walkNonZero(v.Field(i), label+"."+field.Name, fieldPath, zero)
		}
	case reflect.Slice:
		if v.Len() == 0 && !emptyAllowed[strings.TrimPrefix(path, "Sandbox.")] {
			*zero = append(*zero, label+" (empty)")
		}
		for i := range v.Len() {
			walkNonZero(v.Index(i), label+"[]", path, zero)
		}
	case reflect.Map:
		if v.Len() == 0 {
			*zero = append(*zero, label+" (empty)")
		}
		for _, key := range v.MapKeys() {
			walkNonZero(v.MapIndex(key), label+"[]", path, zero)
		}
	default:
	}
}

const maxMessageRecordLen = 1 << 20

func readMessages[T any, P interface {
	*T
	GetRecords() [][]byte
}](t *testing.T, stream grpc.ServerStreamingClient[T]) ([][][]byte, error) {
	t.Helper()

	var messages [][][]byte
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return messages, err
		}
		messages = append(messages, P(resp).GetRecords())
	}

	for _, message := range messages {
		assert.NotEmpty(t, message, "no message is empty")
		assert.LessOrEqual(t, len(message), 256, "a message carries at most 256 records")
		if len(message) > 1 {
			size := 0
			for _, record := range message {
				size += len(record)
			}
			assert.LessOrEqual(t, size, maxMessageRecordLen, "a message of several records carries at most 1 MiB of them")
		}
	}

	return messages, nil
}

func flatten(messages [][][]byte) [][]byte {
	var records [][]byte
	for _, message := range messages {
		records = append(records, message...)
	}

	return records
}

func readTeamItems(t *testing.T, stream grpc.ServerStreamingClient[statepb.TeamItemsResponse]) ([][]byte, int, error) {
	t.Helper()

	messages, err := readMessages(t, stream)

	return flatten(messages), len(messages), err
}

func readExpiredItems(t *testing.T, stream grpc.ServerStreamingClient[statepb.ExpiredItemsResponse]) ([][]byte, int, error) {
	t.Helper()

	messages, err := readMessages(t, stream)

	return flatten(messages), len(messages), err
}
