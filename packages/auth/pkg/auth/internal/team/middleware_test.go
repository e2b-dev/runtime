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
	"github.com/e2b-dev/infra/packages/auth/pkg/teamdata"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
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
			team:           &types.Team{Team: &teamdata.Team{IsBlocked: false}},
			wantHandlerRan: true,
			wantStatus:     http.StatusOK,
		},
		{
			name:           "blocked and allowlisted get passes",
			method:         http.MethodGet,
			fullPath:       "/sandboxes",
			team:           &types.Team{Team: &teamdata.Team{IsBlocked: true, BlockedReason: &reason}},
			wantHandlerRan: true,
			wantStatus:     http.StatusOK,
		},
		{
			name:           "blocked and allowlisted delete passes",
			method:         http.MethodDelete,
			fullPath:       "/sandboxes/:sandboxID",
			team:           &types.Team{Team: &teamdata.Team{IsBlocked: true, BlockedReason: &reason}},
			wantHandlerRan: true,
			wantStatus:     http.StatusOK,
		},
		{
			name:           "blocked and non-allowlisted post denied",
			method:         http.MethodPost,
			fullPath:       "/sandboxes",
			team:           &types.Team{Team: &teamdata.Team{IsBlocked: true, BlockedReason: &reason}},
			wantHandlerRan: false,
			wantStatus:     http.StatusForbidden,
			wantBodyHas:    "team is blocked: " + reason,
		},
		{
			name:           "blocked and method mismatch on allowlisted path denied",
			method:         http.MethodPost,
			fullPath:       "/sandboxes/:sandboxID",
			team:           &types.Team{Team: &teamdata.Team{IsBlocked: true}},
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

	team := &types.Team{Team: &teamdata.Team{IsBlocked: true}}
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

	cleanTeam := &types.Team{Team: &teamdata.Team{}}
	bannedTeam := &types.Team{Team: &teamdata.Team{IsBanned: true}}
	blockedTeam := &types.Team{Team: &teamdata.Team{IsBlocked: true, BlockedReason: &reason}}
	bannedAndBlockedTeam := &types.Team{Team: &teamdata.Team{IsBanned: true, IsBlocked: true, BlockedReason: &reason}}

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

// A viewer reads and does nothing else. A role this build has never heard of
// is held to the same, and a team resolved without a user is not restricted.
func TestEnforceReadOnlyMember(t *testing.T) {
	t.Parallel()

	deniedReads := ReadOnlyDeniedReads{
		http.MethodGet: {"/templates/:templateID/files/:hash": {}},
	}
	withRole := func(role string) *types.Team {
		team := &types.Team{Team: &teamdata.Team{}}
		team.Role = role

		return team
	}

	cases := []struct {
		name     string
		method   string
		fullPath string
		team     *types.Team
		wantRan  bool
	}{
		{name: "no team on context passes", method: http.MethodPost, fullPath: "/sandboxes", wantRan: true},
		{name: "api key writes", method: http.MethodPost, fullPath: "/sandboxes", team: withRole(""), wantRan: true},
		{name: "owner writes", method: http.MethodPost, fullPath: "/sandboxes", team: withRole(types.TeamRoleOwner), wantRan: true},
		{name: "member writes", method: http.MethodDelete, fullPath: "/sandboxes/:sandboxID", team: withRole(types.TeamRoleMember), wantRan: true},
		{name: "viewer reads", method: http.MethodGet, fullPath: "/sandboxes", team: withRole(types.TeamRoleViewer), wantRan: true},
		{name: "viewer denied post", method: http.MethodPost, fullPath: "/sandboxes", team: withRole(types.TeamRoleViewer)},
		{name: "viewer denied put", method: http.MethodPut, fullPath: "/sandboxes/:sandboxID/network", team: withRole(types.TeamRoleViewer)},
		{name: "viewer denied patch", method: http.MethodPatch, fullPath: "/api-keys/:apiKeyID", team: withRole(types.TeamRoleViewer)},
		{name: "viewer denied delete", method: http.MethodDelete, fullPath: "/sandboxes/:sandboxID", team: withRole(types.TeamRoleViewer)},
		{name: "viewer denied a read that writes", method: http.MethodGet, fullPath: "/templates/:templateID/files/:hash", team: withRole(types.TeamRoleViewer)},
		{name: "member reaches a read that writes", method: http.MethodGet, fullPath: "/templates/:templateID/files/:hash", team: withRole(types.TeamRoleMember), wantRan: true},
		{name: "unknown role reads", method: http.MethodGet, fullPath: "/sandboxes", team: withRole("auditor"), wantRan: true},
		{name: "unknown role denied post", method: http.MethodPost, fullPath: "/sandboxes", team: withRole("auditor")},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ran := false
			r := gin.New()
			r.Handle(tc.method, tc.fullPath, func(c *gin.Context) {
				if tc.team != nil {
					authcontext.SetTeamInfo(c, tc.team)
				}
				EnforceReadOnlyMember(deniedReads)(c)
				if !c.IsAborted() {
					ran = true
					c.Status(http.StatusOK)
				}
			})

			w := httptest.NewRecorder()
			r.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), tc.method, concretePath(tc.fullPath), nil))

			assert.Equal(t, tc.wantRan, ran)
			if tc.wantRan {
				assert.Equal(t, http.StatusOK, w.Code)
			} else {
				assert.Equal(t, http.StatusForbidden, w.Code)
				assert.Contains(t, w.Body.String(), "read-only")
			}
		})
	}
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
