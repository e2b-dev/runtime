package team

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/auth/pkg/auth/internal/authcontext"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
)

func TestEnforceBlockedTeam(t *testing.T) {
	t.Parallel()

	reason := "payment failed"
	allowlist := BlockedTeamAllowlist{
		http.MethodGet:    {"/sandboxes": {}},
		http.MethodDelete: {"/sandboxes/:sandboxID": {}},
	}

	cases := []struct {
		name           string
		method         string
		fullPath       string
		team           *types.Team
		wantHandlerRan bool
		wantStatus     int
		wantBodyHas    string
	}{
		{
			name:           "no team on context passes",
			method:         http.MethodPost,
			fullPath:       "/sandboxes",
			team:           nil,
			wantHandlerRan: true,
			wantStatus:     http.StatusOK,
		},
		{
			name:           "not blocked passes",
			method:         http.MethodPost,
			fullPath:       "/sandboxes",
			team:           types.NewTeam(authqueries.GetTeamByTeamIDRow{IsBlocked: false}),
			wantHandlerRan: true,
			wantStatus:     http.StatusOK,
		},
		{
			name:           "blocked and allowlisted get passes",
			method:         http.MethodGet,
			fullPath:       "/sandboxes",
			team:           types.NewTeam(authqueries.GetTeamByTeamIDRow{IsBlocked: true, BlockedReason: &reason}),
			wantHandlerRan: true,
			wantStatus:     http.StatusOK,
		},
		{
			name:           "blocked and allowlisted delete passes",
			method:         http.MethodDelete,
			fullPath:       "/sandboxes/:sandboxID",
			team:           types.NewTeam(authqueries.GetTeamByTeamIDRow{IsBlocked: true, BlockedReason: &reason}),
			wantHandlerRan: true,
			wantStatus:     http.StatusOK,
		},
		{
			name:           "blocked and non-allowlisted post denied",
			method:         http.MethodPost,
			fullPath:       "/sandboxes",
			team:           types.NewTeam(authqueries.GetTeamByTeamIDRow{IsBlocked: true, BlockedReason: &reason}),
			wantHandlerRan: false,
			wantStatus:     http.StatusForbidden,
			wantBodyHas:    "team is blocked: " + reason,
		},
		{
			name:           "blocked and method mismatch on allowlisted path denied",
			method:         http.MethodPost,
			fullPath:       "/sandboxes/:sandboxID",
			team:           types.NewTeam(authqueries.GetTeamByTeamIDRow{IsBlocked: true}),
			wantHandlerRan: false,
			wantStatus:     http.StatusForbidden,
			wantBodyHas:    "team is blocked",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			w, ran := runEnforceBlockedTeam(t, allowlist, tc.method, tc.fullPath, tc.team)

			assert.Equal(t, tc.wantHandlerRan, ran)
			assert.Equal(t, tc.wantStatus, w.Code)
			if tc.wantBodyHas != "" {
				assert.Contains(t, w.Body.String(), tc.wantBodyHas)
			}
		})
	}
}

func TestEnforceBlockedTeam_AllowlistParameterization(t *testing.T) {
	t.Parallel()

	team := types.NewTeam(authqueries.GetTeamByTeamIDRow{IsBlocked: true})
	const method, path = http.MethodGet, "/teams/:teamID/members"

	permissive := BlockedTeamAllowlist{method: {path: {}}}
	restrictive := BlockedTeamAllowlist{}

	wAllowed, ranAllowed := runEnforceBlockedTeam(t, permissive, method, path, team)
	require.True(t, ranAllowed)
	assert.Equal(t, http.StatusOK, wAllowed.Code)

	wDenied, ranDenied := runEnforceBlockedTeam(t, restrictive, method, path, team)
	require.False(t, ranDenied)
	assert.Equal(t, http.StatusForbidden, wDenied.Code)
}

func TestCheckTeamAccess(t *testing.T) {
	t.Parallel()

	reason := "payment failed"
	allowlist := BlockedTeamAllowlist{
		http.MethodGet: {"/sandboxes": {}},
	}

	cleanTeam := types.NewTeam(authqueries.GetTeamByTeamIDRow{})
	bannedTeam := types.NewTeam(authqueries.GetTeamByTeamIDRow{IsBanned: true})
	blockedTeam := types.NewTeam(authqueries.GetTeamByTeamIDRow{IsBlocked: true, BlockedReason: &reason})
	bannedAndBlockedTeam := types.NewTeam(authqueries.GetTeamByTeamIDRow{IsBanned: true, IsBlocked: true, BlockedReason: &reason})

	wantForbidden := func(t *testing.T, err error) {
		t.Helper()

		var target *ForbiddenError
		require.ErrorAs(t, err, &target)
	}
	wantBlocked := func(t *testing.T, err error) {
		t.Helper()

		var target *BlockedError
		require.ErrorAs(t, err, &target)
	}

	cases := []struct {
		name      string
		method    string
		fullPath  string
		team      *types.Team
		wantErrAs func(t *testing.T, err error)
	}{
		{
			name:     "nil team passes",
			method:   http.MethodPost,
			fullPath: "/sandboxes",
			team:     nil,
		},
		{
			name:     "clean team passes",
			method:   http.MethodPost,
			fullPath: "/sandboxes",
			team:     cleanTeam,
		},
		{
			name:      "banned non-allowlisted denied",
			method:    http.MethodPost,
			fullPath:  "/sandboxes",
			team:      bannedTeam,
			wantErrAs: wantForbidden,
		},
		{
			name:      "banned allowlisted still denied",
			method:    http.MethodGet,
			fullPath:  "/sandboxes",
			team:      bannedTeam,
			wantErrAs: wantForbidden,
		},
		{
			name:     "blocked allowlisted passes",
			method:   http.MethodGet,
			fullPath: "/sandboxes",
			team:     blockedTeam,
		},
		{
			name:      "blocked non-allowlisted denied",
			method:    http.MethodPost,
			fullPath:  "/sandboxes",
			team:      blockedTeam,
			wantErrAs: wantBlocked,
		},
		{
			name:      "banned and blocked allowlisted reports banned",
			method:    http.MethodGet,
			fullPath:  "/sandboxes",
			team:      bannedAndBlockedTeam,
			wantErrAs: wantForbidden,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := runCheckTeamAccess(t, allowlist, tc.method, tc.fullPath, tc.team)

			if tc.wantErrAs == nil {
				assert.NoError(t, err)

				return
			}

			require.Error(t, err)
			tc.wantErrAs(t, err)
		})
	}
}

func runCheckTeamAccess(
	t *testing.T,
	allowlist BlockedTeamAllowlist,
	method, fullPath string,
	team *types.Team,
) error {
	t.Helper()

	var gotErr error

	r := gin.New()
	r.Handle(method, fullPath, func(c *gin.Context) {
		gotErr = CheckTeamAccess(c, team, allowlist)
	})

	r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), method, concretePath(fullPath), nil))

	return gotErr
}

func runEnforceBlockedTeam(
	t *testing.T,
	allowlist BlockedTeamAllowlist,
	method, fullPath string,
	team *types.Team,
) (*httptest.ResponseRecorder, bool) {
	t.Helper()

	handlerRan := false

	r := gin.New()
	r.Handle(method, fullPath, func(c *gin.Context) {
		if team != nil {
			authcontext.SetTeamInfo(c, team)
		}
		EnforceBlockedTeam(allowlist)(c)
		if !c.IsAborted() {
			handlerRan = true
			c.Status(http.StatusOK)
		}
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), method, concretePath(fullPath), nil)
	r.ServeHTTP(w, req)

	return w, handlerRan
}

// concretePath substitutes ":param" segments with "x" so httptest hits
// the route while c.FullPath() still reports the original pattern.
func concretePath(pattern string) string {
	var b strings.Builder
	b.Grow(len(pattern))

	i := 0
	for i < len(pattern) {
		if pattern[i] == ':' {
			b.WriteByte('x')
			i++

			for i < len(pattern) && pattern[i] != '/' {
				i++
			}

			continue
		}

		b.WriteByte(pattern[i])
		i++
	}

	return b.String()
}
