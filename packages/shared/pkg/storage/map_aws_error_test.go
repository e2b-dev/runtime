package storage

import (
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
)

// TestMapAWSError pins the AWS S3 error -> storage sentinel mapping shared by
// WriteTo, OpenRangeReader and Size: NoSuchKey (GetObject) and NotFound
// (HeadObject) both mean gone; InvalidObjectState means archived; anything else
// returns nil so the caller keeps its own operation-specific context.
func TestMapAWSError(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		err  error
		want error
	}{
		"NoSuchKey -> not exist":          {&types.NoSuchKey{}, ErrObjectNotExist},
		"NotFound -> not exist":           {&types.NotFound{}, ErrObjectNotExist},
		"InvalidObjectState -> archived":  {&types.InvalidObjectState{}, ErrObjectArchived},
		"wrapped NoSuchKey -> not exist":  {fmt.Errorf("get: %w", &types.NoSuchKey{}), ErrObjectNotExist},
		"wrapped archived -> archived":    {fmt.Errorf("get: %w", &types.InvalidObjectState{}), ErrObjectArchived},
		"unknown error -> nil (keep ctx)": {errors.New("some transient network error"), nil},
		"nil -> nil":                      {nil, nil},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got := mapAWSError(tc.err)
			if tc.want == nil {
				assert.Nil(t, got)
				return
			}
			assert.ErrorIs(t, got, tc.want)
		})
	}
}
