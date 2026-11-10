package management

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

func TestValidateProjectMemberProjection(t *testing.T) {
	t.Parallel()

	identity := ProjectMemberIdentity{Issuer: "https://issuer.test", Subject: "subject"}
	valid := ProjectMemberProjection{
		ProjectID:  uuid.New(),
		UserID:     uuid.New(),
		Revision:   1,
		Present:    true,
		Identities: []ProjectMemberIdentity{identity},
	}

	require.NoError(t, validateProjectMemberProjection(valid))

	for _, test := range []struct {
		projection ProjectMemberProjection
		err        error
	}{
		{projection: ProjectMemberProjection{ProjectID: valid.ProjectID, UserID: valid.UserID, Revision: 0, Present: true, Identities: []ProjectMemberIdentity{identity}}, err: ErrInvalidProjectMember},
		{projection: ProjectMemberProjection{ProjectID: valid.ProjectID, UserID: valid.UserID, Revision: 1, Present: true}, err: ErrInvalidProjectMember},
		{projection: ProjectMemberProjection{ProjectID: valid.ProjectID, UserID: valid.UserID, Revision: 1, Present: false, Identities: []ProjectMemberIdentity{identity}}, err: ErrInvalidProjectMember},
		{projection: ProjectMemberProjection{ProjectID: valid.ProjectID, UserID: valid.UserID, Revision: 1, Present: true, Identities: []ProjectMemberIdentity{identity, identity}}, err: ErrDuplicateProjectIdentity},
		{projection: ProjectMemberProjection{ProjectID: valid.ProjectID, UserID: valid.UserID, Revision: 1, Present: true, Identities: []ProjectMemberIdentity{identity, {Issuer: identity.Issuer, Subject: "other-subject"}}}, err: ErrInvalidProjectMember},
		{projection: ProjectMemberProjection{ProjectID: valid.ProjectID, UserID: valid.UserID, Revision: 1, Present: true, Role: "admin", Identities: []ProjectMemberIdentity{identity}}, err: ErrInvalidProjectMember},
	} {
		require.ErrorIs(t, validateProjectMemberProjection(test.projection), test.err)
	}
}

// A source that sends no role must not change what is stored: a new member
// starts as a member, and a later push without a role keeps an owner an owner.
func TestApplyProjectMemberStoresTheRole(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	projectID := testutils.CreateTestTeam(t, db)
	service, _ := newService(db)
	userID := uuid.New()
	identity := ProjectMemberIdentity{Issuer: "https://issuer.test", Subject: "subject"}

	for revision, step := range []struct{ sent, stored string }{
		{sent: "", stored: ProjectMemberRoleMember},
		{sent: ProjectMemberRoleOwner, stored: ProjectMemberRoleOwner},
		{sent: "", stored: ProjectMemberRoleOwner},
		{sent: ProjectMemberRoleMember, stored: ProjectMemberRoleMember},
	} {
		require.NoError(t, service.ApplyProjectMember(t.Context(), ProjectMemberProjection{
			ProjectID:  projectID,
			UserID:     userID,
			Revision:   int64(revision + 1),
			Present:    true,
			Role:       step.sent,
			Identities: []ProjectMemberIdentity{identity},
		}))
		require.Equal(t, step.stored, teamMemberRole(t, db, projectID, userID), "after revision %d", revision+1)
	}
}

func TestApplyProjectMemberHonorsTheNewestRevision(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	projectID := testutils.CreateTestTeam(t, db)
	service, cache := newService(db)
	userID := uuid.New()
	identity := ProjectMemberIdentity{Issuer: "https://issuer.test", Subject: "subject"}

	require.NoError(t, service.ApplyProjectMember(t.Context(), ProjectMemberProjection{
		ProjectID:  projectID,
		UserID:     userID,
		Revision:   1,
		Present:    true,
		IsDefault:  true,
		Identities: []ProjectMemberIdentity{identity},
	}))
	require.Equal(t, []uuid.UUID{userID}, teamMembers(t, db, projectID))
	require.True(t, teamMemberIsDefault(t, db, projectID, userID))
	require.True(t, publicUserExists(t, db, userID))
	require.Equal(t, userID, identityOwner(t, db, identity))

	cache.reset()
	require.NoError(t, service.ApplyProjectMember(t.Context(), ProjectMemberProjection{
		ProjectID: projectID,
		UserID:    userID,
		Revision:  2,
		Present:   false,
	}))
	require.Empty(t, teamMembers(t, db, projectID))
	require.True(t, publicUserExists(t, db, userID))
	require.Equal(t, userID, identityOwner(t, db, identity))
	require.Equal(t, []memberKey{{userID: userID, teamID: projectID.String()}}, cache.members)

	cache.reset()
	require.NoError(t, service.ApplyProjectMember(t.Context(), ProjectMemberProjection{
		ProjectID:  projectID,
		UserID:     userID,
		Revision:   1,
		Present:    true,
		Identities: []ProjectMemberIdentity{identity},
	}))
	require.Empty(t, teamMembers(t, db, projectID))
	require.Equal(t, []memberKey{{userID: userID, teamID: projectID.String()}}, cache.members)
}

func TestApplyProjectMemberReplacesTheExistingDefault(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	firstProjectID := testutils.CreateTestTeam(t, db)
	secondProjectID := testutils.CreateTestTeam(t, db)
	service, _ := newService(db)
	userID := uuid.New()
	identity := ProjectMemberIdentity{Issuer: "https://issuer.test", Subject: "subject"}

	require.NoError(t, service.ApplyProjectMember(t.Context(), ProjectMemberProjection{
		ProjectID: firstProjectID, UserID: userID, Revision: 1, Present: true, IsDefault: true,
		Identities: []ProjectMemberIdentity{identity},
	}))
	require.NoError(t, service.ApplyProjectMember(t.Context(), ProjectMemberProjection{
		ProjectID: secondProjectID, UserID: userID, Revision: 1, Present: true, IsDefault: true,
		Identities: []ProjectMemberIdentity{identity},
	}))

	require.False(t, teamMemberIsDefault(t, db, firstProjectID, userID))
	require.True(t, teamMemberIsDefault(t, db, secondProjectID, userID))
}

func TestApplyProjectMemberReportsAnUnknownProject(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	service, cache := newService(db)

	err := service.ApplyProjectMember(t.Context(), ProjectMemberProjection{
		ProjectID: uuid.New(),
		UserID:    uuid.New(),
		Revision:  1,
		Present:   false,
	})

	require.ErrorIs(t, err, ErrProjectNotFound)
	require.Empty(t, cache.members)
}

func TestApplyProjectMemberRejectsAnIdentityOwnedByAnotherUser(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	projectID := testutils.CreateTestTeam(t, db)
	service, cache := newService(db)
	owner, requested := uuid.New(), uuid.New()
	identity := ProjectMemberIdentity{Issuer: "https://issuer.test", Subject: "subject"}

	require.NoError(t, service.ApplyProjectMember(t.Context(), ProjectMemberProjection{
		ProjectID:  projectID,
		UserID:     owner,
		Revision:   1,
		Present:    true,
		Identities: []ProjectMemberIdentity{identity},
	}))
	cache.reset()

	err := service.ApplyProjectMember(t.Context(), ProjectMemberProjection{
		ProjectID:  projectID,
		UserID:     requested,
		Revision:   1,
		Present:    true,
		Identities: []ProjectMemberIdentity{identity},
	})

	require.ErrorIs(t, err, ErrProjectIdentityOwnedByUser)
	require.Equal(t, owner, identityOwner(t, db, identity))
	require.False(t, publicUserExists(t, db, requested))
	require.Equal(t, []uuid.UUID{owner}, teamMembers(t, db, projectID))
	require.Empty(t, cache.members)
}
