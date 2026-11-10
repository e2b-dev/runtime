package featureflags

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldcontext"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFlattenContexts(t *testing.T) {
	t.Parallel()
	one := ldcontext.NewWithKind("one", "one")
	two := ldcontext.NewWithKind("two", "two")
	three := ldcontext.NewWithKind("three", "three")
	four := ldcontext.NewWithKind("four", "four")
	five := ldcontext.NewWithKind("five", "five")
	six := ldcontext.NewWithKind("six", "six")
	seven := ldcontext.NewWithKind("seven", "seven")

	items := []ldcontext.Context{one, two, ldcontext.NewMulti(three, four, ldcontext.NewMulti(five, six)), seven}

	result := flattenContexts(items)

	// we don't care what order the contexts are in, they just all need to be there
	keySet := sliceToSet(result, func(c ldcontext.Context) ldcontext.Kind { return c.Kind() })
	assert.Equal(t, newSet[ldcontext.Kind]("one", "two", "three", "four", "five", "six", "seven"), keySet)
}

func ldValueToText(value ldvalue.Value) string {
	return strings.ReplaceAll(value.String(), "\"", "")
}

func TestMergeContextsSameKind(t *testing.T) {
	t.Parallel()
	kind := ldcontext.Kind("test")
	testKey := "test"
	emptyName := "[none]"
	emptyValue := "null"

	type keyValue struct {
		key   string
		value string
	}

	tests := []struct {
		name              string
		firstContext      ldcontext.Context
		secondContext     ldcontext.Context
		expectedName      string
		expectedKey       string
		expectedKeyValues []keyValue
	}{
		{
			name: "no_name_no_value",
			firstContext: ldcontext.NewBuilder("same").
				Kind(kind).
				Build(),
			secondContext: ldcontext.NewBuilder("same").
				Kind(kind).
				Build(),
			expectedKey:       "same",
			expectedName:      emptyName,
			expectedKeyValues: []keyValue{{key: testKey, value: emptyValue}},
		},
		{
			name: "name_no_value",
			firstContext: ldcontext.NewBuilder("same").
				Kind(kind).
				Name("first").
				Build(),
			secondContext: ldcontext.NewBuilder("same").
				Kind(kind).
				Name("second").
				Build(),
			expectedKey:       "same",
			expectedName:      "second",
			expectedKeyValues: []keyValue{{key: testKey, value: emptyValue}},
		},
		{
			name: "no_name_only_value",
			firstContext: ldcontext.NewBuilder("same").
				Kind(kind).
				SetValue(testKey, ldvalue.String("first")).
				Build(),
			secondContext: ldcontext.NewBuilder("same").
				Kind(kind).
				Build(),
			expectedKey:       "same",
			expectedName:      emptyName,
			expectedKeyValues: []keyValue{{key: testKey, value: "first"}},
		},
		{
			name: "key",
			firstContext: ldcontext.NewBuilder("first").
				Kind(kind).
				Build(),
			secondContext: ldcontext.NewBuilder("second").
				Kind(kind).
				Build(),
			expectedKey:       "second",
			expectedName:      emptyName,
			expectedKeyValues: nil,
		},
		{
			name: "combine_values",
			firstContext: ldcontext.NewBuilder("same").
				Kind(kind).
				SetValue(testKey, ldvalue.String("first")).
				Name("first").
				SetValue("other", ldvalue.String("other")).
				Build(),
			secondContext: ldcontext.NewBuilder("same").
				Kind(kind).
				SetValue(testKey, ldvalue.String("second")).
				Build(),
			expectedKey:       "same",
			expectedName:      "first",
			expectedKeyValues: []keyValue{{key: testKey, value: "second"}, {key: "other", value: "other"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := mergeSameKind(tc.firstContext, tc.secondContext)
			assert.Equal(t, tc.expectedKey, result.Key(), "expected key to match")
			assert.Equal(t, tc.expectedName, result.Name().String(), "expected name to match")
			for _, kv := range tc.expectedKeyValues {
				assert.Equal(t, kv.value, ldValueToText(result.GetValue(kv.key)), "expected value for key %s to match", kv.key)
			}
		})
	}
}

type set[T comparable] map[T]struct{}

func sliceToSet[TIn any, TOut comparable](input []TIn, fn func(TIn) TOut) set[TOut] {
	s := newSet[TOut]()

	for _, item := range input {
		key := fn(item)
		s[key] = struct{}{}
	}

	return s
}

func newSet[T comparable](input ...T) set[T] {
	s := make(set[T])

	for _, item := range input {
		s[item] = struct{}{}
	}

	return s
}

func TestSetContext(t *testing.T) {
	t.Parallel()

	t.Run("empty_contexts_returns_original_context", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		result := AddToContext(ctx)

		assert.Equal(t, ctx, result)
		_, ok := getContext(result)
		assert.False(t, ok, "should not have embedded context")
	})

	t.Run("single_context", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		teamCtx := TeamContext("team-123")

		result := AddToContext(ctx, teamCtx)

		embedded, ok := getContext(result)
		assert.True(t, ok)
		assert.Equal(t, "team-123", embedded.Key())
		assert.Equal(t, TeamKind, embedded.Kind())
	})

	t.Run("multiple_contexts", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		teamCtx := TeamContext("team-123")
		userCtx := UserContext("user-456")

		result := AddToContext(ctx, teamCtx, userCtx)

		embedded, ok := getContext(result)
		assert.True(t, ok)
		assert.True(t, embedded.Multiple())

		// Verify both contexts are present
		keySet := sliceToSet(embedded.GetAllIndividualContexts(nil), func(c ldcontext.Context) ldcontext.Kind { return c.Kind() })
		assert.Equal(t, newSet[ldcontext.Kind](TeamKind, UserKind), keySet)
	})

	t.Run("sequential_calls_merge_contexts", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()

		// First call adds team context
		ctx = AddToContext(ctx, TeamContext("team-123"))

		// Second call adds user context - should merge, not replace
		ctx = AddToContext(ctx, UserContext("user-456"))

		embedded, ok := getContext(ctx)
		assert.True(t, ok)
		assert.True(t, embedded.Multiple())

		// Both contexts should be present
		keySet := sliceToSet(embedded.GetAllIndividualContexts(nil), func(c ldcontext.Context) ldcontext.Kind { return c.Kind() })
		assert.Equal(t, newSet[ldcontext.Kind](TeamKind, UserKind), keySet)
	})

	t.Run("same_kind_second_takes_precedence", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()

		// First call with team-123
		ctx = AddToContext(ctx, TeamContextWithName("team-123", "First Team"))

		// Second call with different team - should override
		ctx = AddToContext(ctx, TeamContextWithName("team-456", "Second Team"))

		embedded, ok := getContext(ctx)
		assert.True(t, ok)
		assert.False(t, embedded.Multiple(), "should have single context after same-kind merge")
		assert.Equal(t, "team-456", embedded.Key())
		assert.Equal(t, "Second Team", embedded.Name().String())
	})

	// Empty-key contexts are IsDefined()==true but Err()!=nil; without
	// filtering they would poison the resulting multi-context.
	t.Run("invalid_contexts_filtered_out", func(t *testing.T) {
		t.Parallel()

		invalidTeam := TeamContext("")
		assert.True(t, invalidTeam.IsDefined())
		require.Error(t, invalidTeam.Err())

		ctx := AddToContext(t.Context(), invalidTeam, UserContext(""), SandboxContext("sandbox-123"))

		embedded, ok := getContext(ctx)
		assert.True(t, ok)
		assert.False(t, embedded.Multiple())
		assert.Equal(t, "sandbox-123", embedded.Key())
		assert.Equal(t, SandboxKind, embedded.Kind())
		require.NoError(t, embedded.Err())
	})
}

type detachTestKey struct{}

func TestDetachContext(t *testing.T) {
	t.Parallel()

	t.Run("keeps the flag contexts and nothing else of the request", func(t *testing.T) {
		t.Parallel()
		team := TeamContext("team-detached")
		request, cancel := context.WithTimeout(t.Context(), time.Hour)
		defer cancel()
		request = context.WithValue(AddToContext(request, team), detachTestKey{}, "request value")

		detached := DetachContext(request)

		got, ok := getContext(detached)
		require.True(t, ok)
		assert.Equal(t, team, got)
		assert.Nil(t, detached.Value(detachTestKey{}), "a value of the request is not carried")
		_, hasDeadline := detached.Deadline()
		assert.False(t, hasDeadline)
		cancel()
		assert.NoError(t, detached.Err(), "the request's cancellation does not reach it")
	})

	t.Run("carries no flag context when the request has none", func(t *testing.T) {
		t.Parallel()
		detached := DetachContext(context.WithValue(t.Context(), detachTestKey{}, "request value"))

		_, ok := getContext(detached)
		assert.False(t, ok)
		assert.Nil(t, detached.Value(detachTestKey{}))
	})
}
