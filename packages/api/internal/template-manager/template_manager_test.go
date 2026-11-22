package template_manager

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/types"
	templatemanagergrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/template-manager"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

var _ templateManagerClient = (*fakeTemplateManagerClient)(nil)

type fakeTemplateManagerClient struct {
	setStatusError   error
	setFinishedError error

	getStatusResponse *templatemanagergrpc.TemplateBuildStatusResponse
	getStatusErr      error
}

func (f fakeTemplateManagerClient) SetTerminalStatus(context.Context, uuid.UUID, types.BuildStatusGroup, *templatemanagergrpc.TemplateBuildStatusReason) (bool, error) {
	return f.setStatusError == nil, f.setStatusError
}

func (f fakeTemplateManagerClient) DeleteBuild(context.Context, uuid.UUID, string, uuid.UUID, string) error {
	return nil
}

func (f fakeTemplateManagerClient) SetFinished(context.Context, uuid.UUID, int64, string, string, string) error {
	return f.setFinishedError
}

func (f fakeTemplateManagerClient) GetStatus(context.Context, uuid.UUID, string, uuid.UUID, string) (*templatemanagergrpc.TemplateBuildStatusResponse, error) {
	return f.getStatusResponse, f.getStatusErr
}

func TestPollBuildStatus_setStatus(t *testing.T) {
	t.Parallel()
	type fields struct {
		buildID               uuid.UUID
		templateManagerClient templateManagerClient
	}

	tests := []struct {
		name    string
		fields  fields
		wantErr bool
		status  *templatemanagergrpc.TemplateBuildStatusResponse
		err     error
	}{
		{
			name: "should return error if status is nil",
			fields: fields{
				templateManagerClient: &fakeTemplateManagerClient{
					getStatusResponse: nil,
				},
				buildID: uuid.New(),
			},
			wantErr: true,
		},
		// return context deadline exceeded
		{
			name: "should return context deadline exceeded",
			fields: fields{
				templateManagerClient: &fakeTemplateManagerClient{
					getStatusErr: errors.New("context deadline exceeded"),
				},
			},
			wantErr: true,
			status:  nil,
		},
		// if error is not context deadline exceeded, return error
		{
			name: "should return error if error is not context deadline exceeded",
			fields: fields{
				templateManagerClient: &fakeTemplateManagerClient{
					getStatusErr: errors.New("some other error"),
				},
			},
			wantErr: true,
			status:  nil,
		},
		// if status client present and err is nil, return nil
		{
			name: "should return nil if status client present and err is nil",
			fields: fields{
				templateManagerClient: &fakeTemplateManagerClient{
					getStatusErr: nil,
					getStatusResponse: &templatemanagergrpc.TemplateBuildStatusResponse{
						Status: templatemanagergrpc.TemplateBuildState_Completed,
					},
				},
			},
			wantErr: false,
			status: &templatemanagergrpc.TemplateBuildStatusResponse{
				Status: templatemanagergrpc.TemplateBuildState_Completed,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := &PollBuildStatus{
				client: tt.fields.templateManagerClient,
				logger: logger.NewNopLogger(),
			}
			err := c.setStatus(t.Context())
			if tt.wantErr {
				require.Error(t, err)
			}
			if !tt.wantErr {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.status.GetStatus(), c.status.GetStatus())
		})
	}
}

func TestPollBuildStatus_dispatchBasedOnStatus(t *testing.T) {
	t.Parallel()
	type fields struct {
		templateManagerClient templateManagerClient
	}
	type args struct {
		status *templatemanagergrpc.TemplateBuildStatusResponse
	}
	tests := []struct {
		name              string
		fields            fields
		args              args
		wantCompleteState bool
		wantErr           bool
	}{
		{
			name: "should return error if status is nil",
			fields: fields{
				templateManagerClient: &fakeTemplateManagerClient{},
			},
			args: args{
				status: nil,
			},
			wantCompleteState: false,
			wantErr:           true,
		},
		{
			name: "should handle failed status",
			fields: fields{
				templateManagerClient: &fakeTemplateManagerClient{
					setStatusError: errors.New("failed to set status"),
				},
			},
			args: args{
				status: &templatemanagergrpc.TemplateBuildStatusResponse{
					Status: templatemanagergrpc.TemplateBuildState_Failed,
				},
			},
			wantCompleteState: false,
			wantErr:           true,
		},
		{
			name: "should handle completed status with nil metadata",
			fields: fields{
				templateManagerClient: &fakeTemplateManagerClient{},
			},
			args: args{
				status: &templatemanagergrpc.TemplateBuildStatusResponse{
					Status: templatemanagergrpc.TemplateBuildState_Completed,
				},
			},
			wantCompleteState: false,
			wantErr:           true,
		},
		{
			name: "should handle completed status successfully",
			fields: fields{
				templateManagerClient: &fakeTemplateManagerClient{
					setFinishedError: errors.New("failed to set finished"),
				},
			},
			args: args{
				status: &templatemanagergrpc.TemplateBuildStatusResponse{
					Status: templatemanagergrpc.TemplateBuildState_Completed,
					Metadata: &templatemanagergrpc.TemplateBuildMetadata{
						RootfsSizeKey:  100,
						EnvdVersionKey: "1.0.0",
					},
				},
			},
			wantCompleteState: false,
			wantErr:           true,
		},
		{
			name: "should not send to done channel for building status",
			fields: fields{
				templateManagerClient: &fakeTemplateManagerClient{},
			},
			args: args{
				status: &templatemanagergrpc.TemplateBuildStatusResponse{
					Status: templatemanagergrpc.TemplateBuildState_Building,
				},
			},
			wantCompleteState: false,
			wantErr:           false,
		},
		// should not get error when no error setting status
		{
			name: "should not get error when no error setting status",
			fields: fields{
				templateManagerClient: &fakeTemplateManagerClient{},
			},
			args: args{
				status: &templatemanagergrpc.TemplateBuildStatusResponse{
					Status: templatemanagergrpc.TemplateBuildState_Building,
				},
			},
			wantErr:           false,
			wantCompleteState: false,
		},
		// should not get error when no error setting finished
		{
			name: "should not get error when no error setting finished",
			fields: fields{
				templateManagerClient: &fakeTemplateManagerClient{},
			},
			args: args{
				status: &templatemanagergrpc.TemplateBuildStatusResponse{
					Status: templatemanagergrpc.TemplateBuildState_Completed,
					Metadata: &templatemanagergrpc.TemplateBuildMetadata{
						RootfsSizeKey:  100,
						EnvdVersionKey: "1.0.0",
					},
				},
			},
			wantErr:           false,
			wantCompleteState: true,
		},
		// should error when nil metadata
		{
			name: "should error when nil metadata",
			fields: fields{
				templateManagerClient: &fakeTemplateManagerClient{},
			},
			args: args{
				status: &templatemanagergrpc.TemplateBuildStatusResponse{
					Status: templatemanagergrpc.TemplateBuildState_Completed,
				},
			},
			wantErr:           true,
			wantCompleteState: false,
		},
		// should not error when status is failure
		{
			name: "should not error when status is failure",
			fields: fields{
				templateManagerClient: &fakeTemplateManagerClient{},
			},
			args: args{
				status: &templatemanagergrpc.TemplateBuildStatusResponse{
					Status: templatemanagergrpc.TemplateBuildState_Failed,
				},
			},

			wantErr:           false,
			wantCompleteState: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := &PollBuildStatus{
				client: tt.fields.templateManagerClient,
				logger: logger.NewNopLogger(),
			}

			completed, err := c.dispatchBasedOnStatus(t.Context(), tt.args.status)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantCompleteState, completed)
		})
	}
}
