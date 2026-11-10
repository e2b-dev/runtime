//go:build linux

package factories

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestServiceErrorChannelRetainsFailureBeforeConsumer(t *testing.T) {
	t.Parallel()

	serviceError := newServiceErrorChannel()
	want := errors.New("startup service failed")

	reportServiceError(serviceError, want)

	select {
	case got := <-serviceError:
		require.ErrorIs(t, got, want)
	case <-time.After(time.Second):
		t.Fatal("startup failure was not retained for the Run consumer")
	}
}
