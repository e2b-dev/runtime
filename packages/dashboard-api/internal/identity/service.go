package identity

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/google/uuid"
)

type Service interface {
	ProfilesByUserID(ctx context.Context, userIDs []uuid.UUID) (map[uuid.UUID]Profile, error)
	FindProfilesByEmail(ctx context.Context, email string) ([]Profile, error)
}

type service struct {
	directories map[string]Directory
	issuers     []string
	linkage     Linkage
}

func NewService(directories map[string]Directory, linkage Linkage) (Service, error) {
	if len(directories) == 0 {
		return nil, errors.New("at least one identity directory is required")
	}
	if linkage == nil {
		return nil, errors.New("identity linkage is required")
	}

	registry := make(map[string]Directory, len(directories))
	issuers := make([]string, 0, len(directories))
	for issuer, directory := range directories {
		issuer = strings.TrimSpace(issuer)
		if issuer == "" {
			return nil, errors.New("identity directory issuer must not be empty")
		}
		if directory == nil {
			return nil, fmt.Errorf("identity directory for issuer %q is nil", issuer)
		}
		registry[issuer] = directory
		issuers = append(issuers, issuer)
	}
	slices.Sort(issuers)

	return &service{
		directories: registry,
		issuers:     issuers,
		linkage:     linkage,
	}, nil
}

func (s *service) ProfilesByUserID(ctx context.Context, userIDs []uuid.UUID) (map[uuid.UUID]Profile, error) {
	identities, err := s.identitiesByUserID(ctx, userIDs)
	if err != nil {
		return nil, err
	}

	profiles := make(map[uuid.UUID]Profile, len(identities))
	for userID, id := range identities {
		profiles[userID] = ProfileFromIdentity(userID, id)
	}

	return profiles, nil
}

func (s *service) FindProfilesByEmail(ctx context.Context, email string) ([]Profile, error) {
	normalized := strings.TrimSpace(email)
	if normalized == "" {
		return []Profile{}, nil
	}

	profiles := make([]Profile, 0)
	for _, issuer := range s.issuers {
		directory := s.directories[issuer]

		identities, err := directory.SearchByEmail(ctx, normalized)
		if err != nil {
			return nil, err
		}
		if len(identities) == 0 {
			continue
		}

		subjects := make([]string, 0, len(identities))
		for _, id := range identities {
			subjects = append(subjects, id.Subject)
		}

		linked, err := s.linkage.UsersForSubjects(ctx, issuer, subjects)
		if err != nil {
			return nil, fmt.Errorf("lookup user ids by subjects: %w", err)
		}

		userIDBySubject := make(map[string]uuid.UUID, len(linked))
		for _, row := range linked {
			userIDBySubject[row.Subject] = row.UserID
		}

		for _, id := range identities {
			userID, ok := userIDBySubject[id.Subject]
			if !ok {
				continue
			}
			profiles = append(profiles, ProfileFromIdentity(userID, id))
		}
	}

	return profiles, nil
}

func (s *service) identitiesByUserID(ctx context.Context, userIDs []uuid.UUID) (map[uuid.UUID]Identity, error) {
	unique := uniqueUUIDs(userIDs)
	if len(unique) == 0 {
		return map[uuid.UUID]Identity{}, nil
	}

	linked, err := s.linkedIdentitiesForUsers(ctx, unique)
	if err != nil {
		return nil, err
	}
	if len(linked) == 0 {
		return map[uuid.UUID]Identity{}, nil
	}

	byIssuer := make(map[string][]LinkedIdentity, len(s.issuers))
	for _, row := range linked {
		byIssuer[row.Issuer] = append(byIssuer[row.Issuer], row)
	}

	identities := make(map[uuid.UUID]Identity, len(unique))
	for _, issuer := range s.issuers {
		rows, ok := byIssuer[issuer]
		if !ok {
			continue
		}

		subjects := make([]string, 0, len(rows))
		userIDBySubject := make(map[string]uuid.UUID, len(rows))
		for _, row := range rows {
			subjects = append(subjects, row.Subject)
			userIDBySubject[row.Subject] = row.UserID
		}

		found, err := s.directories[issuer].ListIdentities(ctx, subjects)
		if err != nil {
			return nil, err
		}

		for _, id := range found {
			userID, ok := userIDBySubject[id.Subject]
			if !ok {
				continue
			}
			if _, exists := identities[userID]; exists {
				continue
			}
			identities[userID] = id
		}
	}

	return identities, nil
}

func (s *service) linkedIdentitiesForUsers(ctx context.Context, userIDs []uuid.UUID) ([]LinkedIdentity, error) {
	linked, err := s.linkage.IdentitiesForUsers(ctx, s.issuers, userIDs)
	if err != nil {
		return nil, fmt.Errorf("lookup linked identities: %w", err)
	}

	return linked, nil
}

func uniqueUUIDs(ids []uuid.UUID) []uuid.UUID {
	seen := make(map[uuid.UUID]struct{}, len(ids))
	unique := make([]uuid.UUID, 0, len(ids))

	for _, id := range ids {
		if id == uuid.Nil {
			continue
		}

		if _, ok := seen[id]; ok {
			continue
		}

		seen[id] = struct{}{}
		unique = append(unique, id)
	}

	return unique
}
