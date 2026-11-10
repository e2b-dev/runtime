package server

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/e2b-dev/infra/packages/scheduling-api/internal/id"
	"github.com/e2b-dev/infra/packages/scheduling-api/internal/store"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/scheduling"
)

const (
	defaultStalePageSize    = 100
	invalidPageTokenMessage = "page_token: must be a next_page_token of ListStaleTransitions"
	// pageTokenVersion starts every page token, so that a later format can
	// tell its tokens apart.
	pageTokenVersion = "1"
)

// maxPageTokenMicro keeps a page token time inside the timestamp range of
// PostgreSQL, so that a forged token is invalid rather than a database error.
var maxPageTokenMicro = time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC).UnixMicro()

func (s *service) ListStaleTransitions(ctx context.Context, req *scheduling.ListStaleTransitionsRequest) (*scheduling.ListStaleTransitionsResponse, error) {
	after, ok := parsePageToken(req.GetPageToken())
	if !ok {
		return nil, status.Error(codes.InvalidArgument, invalidPageTokenMessage)
	}
	limit := req.GetPageSize()
	if limit == 0 {
		limit = defaultStalePageSize
	}

	stale, err := s.store.ListStaleTransitions(ctx, after, limit)
	if err != nil {
		return nil, internalError(ctx, err)
	}

	transitions := make([]*scheduling.StaleTransition, 0, len(stale))
	var lastSandboxID string
	for _, t := range stale {
		sandboxID, ok := id.UUIDToSandboxID(t.ActorID)
		if !ok {
			return nil, internalError(ctx, fmt.Errorf("list stale transitions: actor %s has no sandbox ID", t.ActorID))
		}
		action, err := transitionAction(t.Action)
		if err != nil {
			return nil, internalError(ctx, fmt.Errorf("list stale transitions: actor %s: %w", t.ActorID, err))
		}
		transitions = append(transitions, scheduling.StaleTransition_builder{
			ProjectId:    t.ProjectID.String(),
			SandboxId:    sandboxID,
			TransitionId: t.TransitionID.String(),
			Action:       action,
			TransitionAt: timestamppb.New(t.TransitionAt),
			ExecutionId:  t.ExecutionID,
			NodeId:       t.NodeID,
			ClusterId:    t.ClusterID.String(),
		}.Build())
		lastSandboxID = sandboxID
	}
	var next string
	if len(stale) == int(limit) {
		next = pageToken(stale[len(stale)-1].TransitionAt, lastSandboxID)
	}

	return scheduling.ListStaleTransitionsResponse_builder{
		Transitions:   transitions,
		NextPageToken: next,
	}.Build(), nil
}

func transitionAction(action store.TransitionAction) (scheduling.TransitionAction, error) {
	switch action {
	case store.ActionPause:
		return scheduling.TransitionAction_TRANSITION_ACTION_PAUSE, nil
	case store.ActionKill:
		return scheduling.TransitionAction_TRANSITION_ACTION_KILL, nil
	default:
		return 0, fmt.Errorf("transition action %d has no API value", action)
	}
}

// pageToken encodes the position after a transition. It carries the sandbox
// ID, not the internal actor UUID.
func pageToken(transitionAt time.Time, sandboxID string) string {
	raw := pageTokenVersion + ":" + strconv.FormatInt(transitionAt.UnixMicro(), 10) + ":" + sandboxID

	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func parsePageToken(token string) (store.StaleCursor, bool) {
	if token == "" {
		return store.StaleCursor{}, true
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return store.StaleCursor{}, false
	}
	parts := strings.Split(string(raw), ":")
	if len(parts) != 3 || parts[0] != pageTokenVersion {
		return store.StaleCursor{}, false
	}
	micros, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || micros < 0 || micros > maxPageTokenMicro {
		return store.StaleCursor{}, false
	}
	actorID, err := id.SandboxIDToUUID(parts[2])
	if err != nil {
		return store.StaleCursor{}, false
	}

	return store.StaleCursor{TransitionAt: time.UnixMicro(micros).UTC(), ActorID: actorID}, true
}
