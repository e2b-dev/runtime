package events

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLegacySandboxEventMigrationMapping(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		in      SandboxEvent
		want    SandboxEvent
		wantErr error
	}{
		"missing-version": {
			in: SandboxEvent{
				EventCategory: "lifecycle",
				EventLabel:    "create",
			},
			want: SandboxEvent{
				EventCategory: "lifecycle",
				EventLabel:    "create",
				Type:          "sandbox.lifecycle.created",
				Version:       "v1",
			},
			wantErr: nil,
		},
		"unknown-version": {
			in: SandboxEvent{
				EventCategory: "lifecycle",
				EventLabel:    "create",
				Version:       "v10",
			},
			want:    SandboxEvent{},
			wantErr: ErrUnknownEventFormat,
		},
		"v1-created": {
			in: SandboxEvent{
				EventCategory: "lifecycle",
				EventLabel:    "create",
				Version:       "v1",
			},
			want: SandboxEvent{
				EventCategory: "lifecycle",
				EventLabel:    "create",
				Type:          "sandbox.lifecycle.created",
				Version:       "v1",
			},
			wantErr: nil,
		},
		"v1-paused": {
			in: SandboxEvent{
				EventCategory: "lifecycle",
				EventLabel:    "pause",
				Version:       "v1",
			},
			want: SandboxEvent{
				EventCategory: "lifecycle",
				EventLabel:    "pause",
				Type:          "sandbox.lifecycle.paused",
				Version:       "v1",
			},
			wantErr: nil,
		},
		"v1-killed": {
			in: SandboxEvent{
				EventCategory: "lifecycle",
				EventLabel:    "kill",
				Version:       "v1",
			},
			want: SandboxEvent{
				EventCategory: "lifecycle",
				EventLabel:    "kill",
				Type:          "sandbox.lifecycle.killed",
				Version:       "v1",
			},
			wantErr: nil,
		},
		"v1-resumed": {
			in: SandboxEvent{
				EventCategory: "lifecycle",
				EventLabel:    "resume",
				Version:       "v1",
			},
			want: SandboxEvent{
				EventCategory: "lifecycle",
				EventLabel:    "resume",
				Type:          "sandbox.lifecycle.resumed",
				Version:       "v1",
			},
			wantErr: nil,
		},
		"v1-updated": {
			in: SandboxEvent{
				EventCategory: "lifecycle",
				EventLabel:    "update",
				Version:       "v1",
			},
			want: SandboxEvent{
				EventCategory: "lifecycle",
				EventLabel:    "update",
				Type:          "sandbox.lifecycle.updated",
				Version:       "v1",
			},
			wantErr: nil,
		},
		"v1-custom": {
			in: SandboxEvent{
				Type:    "sandbox.custom",
				Version: "v1",
			},
			want: SandboxEvent{
				Type:    "sandbox.custom",
				Version: "v1",
			},
			wantErr: nil,
		},
		"v2-created": {
			in: SandboxEvent{
				Version: "v2",
				Type:    "sandbox.lifecycle.created",
			},
			want: SandboxEvent{
				EventCategory: "lifecycle",
				EventLabel:    "create",
				Type:          "sandbox.lifecycle.created",
				Version:       "v2",
			},
			wantErr: nil,
		},
		"v2-paused": {
			in: SandboxEvent{
				Version: "v2",
				Type:    "sandbox.lifecycle.paused",
			},
			want: SandboxEvent{
				EventCategory: "lifecycle",
				EventLabel:    "pause",
				Type:          "sandbox.lifecycle.paused",
				Version:       "v2",
			},
			wantErr: nil,
		},
		"v2-killed": {
			in: SandboxEvent{
				Version: "v2",
				Type:    "sandbox.lifecycle.killed",
			},
			want: SandboxEvent{
				EventCategory: "lifecycle",
				EventLabel:    "kill",
				Type:          "sandbox.lifecycle.killed",
				Version:       "v2",
			},
			wantErr: nil,
		},
		"v2-resumed": {
			in: SandboxEvent{
				Version: "v2",
				Type:    "sandbox.lifecycle.resumed",
			},
			want: SandboxEvent{
				EventCategory: "lifecycle",
				EventLabel:    "resume",
				Type:          "sandbox.lifecycle.resumed",
				Version:       "v2",
			},
			wantErr: nil,
		},
		"v2-updated": {
			in: SandboxEvent{
				Version: "v2",
				Type:    "sandbox.lifecycle.updated",
			},
			want: SandboxEvent{
				EventCategory: "lifecycle",
				EventLabel:    "update",
				Type:          "sandbox.lifecycle.updated",
				Version:       "v2",
			},
			wantErr: nil,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, gotErr := LegacySandboxEventMigrationMapping(tc.in)
			require.ErrorIs(t, gotErr, tc.wantErr)
			require.Equal(t, tc.want.Version, got.Version)
			require.Equal(t, tc.want.Type, got.Type, "category=%q, label=%q", tc.in.EventCategory, tc.in.EventLabel)
			require.Equal(t, tc.want.EventCategory, got.EventCategory, "EventCategory mutated")
			require.Equal(t, tc.want.EventLabel, got.EventLabel, "EventLabel mutated")
		})
	}
}
