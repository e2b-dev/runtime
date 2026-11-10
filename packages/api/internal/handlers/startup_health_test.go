package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestHealthDrainWinsOverStartup(t *testing.T) {
	t.Parallel()

	store := &APIStore{}
	require.Equal(t, http.StatusServiceUnavailable, healthStatus(t, store))

	store.markStartupReady()
	require.Equal(t, http.StatusOK, healthStatus(t, store))

	store.BeginDrain()
	require.Equal(t, http.StatusServiceUnavailable, healthStatus(t, store))

	store.markStartupReady()
	require.Equal(t, http.StatusServiceUnavailable, healthStatus(t, store))
}

func TestHealthDrainWinsOverConcurrentStartup(t *testing.T) {
	t.Parallel()

	const rounds = 1000
	for range rounds {
		store := &APIStore{}
		start := make(chan struct{})
		done := make(chan struct{}, 2)

		go func() {
			<-start
			store.markStartupReady()
			done <- struct{}{}
		}()
		go func() {
			<-start
			store.BeginDrain()
			done <- struct{}{}
		}()

		close(start)
		<-done
		<-done
		require.Equal(t, startupStateDraining, store.startupState.Load())
		require.Equal(t, http.StatusServiceUnavailable, healthStatus(t, store))
	}
}

func healthStatus(t *testing.T, store *APIStore) int {
	t.Helper()

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	store.GetHealth(ginCtx)

	return recorder.Code
}
