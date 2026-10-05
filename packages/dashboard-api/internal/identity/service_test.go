package identity

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

type fakeDirectory struct {
	identities map[string]Identity
	byEmail    map[string][]Identity

	listErr     error
	listedBatch [][]string
	searchedFor []string
}

func newFakeDirectory(identities ...Identity) *fakeDirectory {
	byID := make(map[string]Identity, len(identities))
	for _, id := range identities {
		byID[id.Subject] = id
	}

	return &fakeDirectory{identities: byID}
}

func (d *fakeDirectory) ListIdentities(_ context.Context, subjects []string) ([]Identity, error) {
	d.listedBatch = append(d.listedBatch, subjects)
	if d.listErr != nil {
		return nil, d.listErr
	}

	found := make([]Identity, 0, len(subjects))
	for _, subject := range subjects {
		if id, ok := d.identities[subject]; ok {
			found = append(found, id)
		}
	}

	return found, nil
}

func (d *fakeDirectory) SearchByEmail(_ context.Context, email string) ([]Identity, error) {
	d.searchedFor = append(d.searchedFor, email)

	return d.byEmail[email], nil
}

type fakeLinkage struct {
	rows []LinkedIdentity

	requestedIssuers []string
}

func (l *fakeLinkage) IdentitiesForUsers(_ context.Context, issuers []string, userIDs []uuid.UUID) ([]LinkedIdentity, error) {
	l.requestedIssuers = issuers

	allowed := make(map[string]struct{}, len(issuers))
	for _, issuer := range issuers {
		allowed[issuer] = struct{}{}
	}
	wanted := make(map[uuid.UUID]struct{}, len(userIDs))
	for _, id := range userIDs {
		wanted[id] = struct{}{}
	}

	matched := make([]LinkedIdentity, 0, len(l.rows))
	for _, row := range l.rows {
		if _, ok := allowed[row.Issuer]; !ok {
			continue
		}
		if _, ok := wanted[row.UserID]; !ok {
			continue
		}
		matched = append(matched, row)
	}

	return matched, nil
}

func (l *fakeLinkage) UsersForSubjects(_ context.Context, issuer string, subjects []string) ([]LinkedIdentity, error) {
	wanted := make(map[string]struct{}, len(subjects))
	for _, subject := range subjects {
		wanted[subject] = struct{}{}
	}

	matched := make([]LinkedIdentity, 0, len(l.rows))
	for _, row := range l.rows {
		if row.Issuer != issuer {
			continue
		}
		if _, ok := wanted[row.Subject]; !ok {
			continue
		}
		matched = append(matched, row)
	}

	return matched, nil
}

const (
	issuerA = "https://issuer-a.example.test"
	issuerB = "https://issuer-b.example.test"
)

func TestNewServiceValidatesInput(t *testing.T) {
	t.Parallel()

	if _, err := NewService(nil, &fakeLinkage{}); err == nil {
		t.Fatal("expected error for empty directory registry")
	}
	if _, err := NewService(map[string]Directory{issuerA: newFakeDirectory()}, nil); err == nil {
		t.Fatal("expected error for nil linkage")
	}
	if _, err := NewService(map[string]Directory{" ": newFakeDirectory()}, &fakeLinkage{}); err == nil {
		t.Fatal("expected error for blank issuer")
	}
	if _, err := NewService(map[string]Directory{issuerA: nil}, &fakeLinkage{}); err == nil {
		t.Fatal("expected error for nil directory")
	}
}

func TestServiceProfilesByUserIDRoutesAcrossIssuers(t *testing.T) {
	t.Parallel()

	userA := uuid.New()
	userB := uuid.New()
	subjectA := uuid.NewString()
	subjectB := uuid.NewString()

	directoryA := newFakeDirectory(Identity{Subject: subjectA, Email: "a@example.test"})
	directoryB := newFakeDirectory(Identity{Subject: subjectB, Email: "b@example.test"})
	linkage := &fakeLinkage{rows: []LinkedIdentity{
		{Issuer: issuerA, Subject: subjectA, UserID: userA},
		{Issuer: issuerB, Subject: subjectB, UserID: userB},
	}}

	svc, err := NewService(map[string]Directory{issuerA: directoryA, issuerB: directoryB}, linkage)
	if err != nil {
		t.Fatalf("failed to build service: %v", err)
	}

	profiles, err := svc.ProfilesByUserID(t.Context(), []uuid.UUID{userA, userB})
	if err != nil {
		t.Fatalf("ProfilesByUserID returned error: %v", err)
	}

	if len(profiles) != 2 {
		t.Fatalf("expected 2 profiles, got %d", len(profiles))
	}
	if profiles[userA].Email != "a@example.test" {
		t.Fatalf("expected profile for user A from issuer A, got %+v", profiles[userA])
	}
	if profiles[userB].Email != "b@example.test" {
		t.Fatalf("expected profile for user B from issuer B, got %+v", profiles[userB])
	}
	if len(linkage.requestedIssuers) != 2 {
		t.Fatalf("expected linkage lookup filtered to both registered issuers, got %v", linkage.requestedIssuers)
	}
}

func TestServiceUserKeyedOpsIgnoreUnregisteredIssuerRows(t *testing.T) {
	t.Parallel()

	userID := uuid.New()
	linkage := &fakeLinkage{rows: []LinkedIdentity{
		{Issuer: "https://foreign.example.test", Subject: uuid.NewString(), UserID: userID},
	}}

	svc, err := NewService(map[string]Directory{issuerA: newFakeDirectory()}, linkage)
	if err != nil {
		t.Fatalf("failed to build service: %v", err)
	}

	profiles, err := svc.ProfilesByUserID(t.Context(), []uuid.UUID{userID})
	if err != nil {
		t.Fatalf("ProfilesByUserID returned error: %v", err)
	}
	if len(profiles) != 0 {
		t.Fatalf("expected foreign-issuer rows to be invisible, got %d profiles", len(profiles))
	}
}

func TestServiceFindProfilesByEmailIntersectsLinkage(t *testing.T) {
	t.Parallel()

	userID := uuid.New()
	linkedSubject := uuid.NewString()
	unlinkedSubject := uuid.NewString()
	email := "ada@example.test"

	directory := newFakeDirectory()
	directory.byEmail = map[string][]Identity{email: {
		{Subject: linkedSubject, Email: email},
		{Subject: unlinkedSubject, Email: email},
	}}
	linkage := &fakeLinkage{rows: []LinkedIdentity{{Issuer: issuerA, Subject: linkedSubject, UserID: userID}}}

	svc, err := NewService(map[string]Directory{issuerA: directory}, linkage)
	if err != nil {
		t.Fatalf("failed to build service: %v", err)
	}

	profiles, err := svc.FindProfilesByEmail(t.Context(), email)
	if err != nil {
		t.Fatalf("FindProfilesByEmail returned error: %v", err)
	}
	if len(profiles) != 1 {
		t.Fatalf("expected only linked identities to surface, got %d profiles", len(profiles))
	}
	if profiles[0].UserID != userID {
		t.Fatalf("expected user %s, got %s", userID, profiles[0].UserID)
	}
}
