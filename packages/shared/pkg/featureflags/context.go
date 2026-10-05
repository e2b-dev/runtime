package featureflags

import (
	"context"

	"github.com/google/uuid"
	"github.com/launchdarkly/go-sdk-common/v3/ldcontext"
)

type ctxKey struct{}

func AddToContext(ctx context.Context, contexts ...ldcontext.Context) context.Context {
	if len(contexts) == 0 {
		return ctx
	}

	ctx = context.WithValue(ctx, ctxKey{}, mergeContexts(ctx, contexts))

	return ctx
}

func getContext(ctx context.Context) (ldcontext.Context, bool) {
	if val, ok := ctx.Value(ctxKey{}).(ldcontext.Context); ok {
		return val, true
	}

	return ldcontext.Context{}, false
}

func flattenContexts(contexts []ldcontext.Context) []ldcontext.Context {
	contextMap := make(map[ldcontext.Kind]ldcontext.Context)

	work := contexts
	for len(work) != 0 {
		item := work[0]
		work = work[1:]

		if !item.Multiple() {
			// There can be only one context of each kind
			if oldCtx, ok := contextMap[item.Kind()]; ok {
				contextMap[item.Kind()] = mergeSameKind(oldCtx, item)
			} else {
				contextMap[item.Kind()] = item
			}

			continue
		}

		flattened := item.GetAllIndividualContexts(nil)
		work = append(flattened, work...)
	}

	var result []ldcontext.Context
	for _, item := range contextMap {
		result = append(result, item)
	}

	return result
}

// mergeSameKind merges two contexts of the same kind. The second context has precedence.
func mergeSameKind(first ldcontext.Context, second ldcontext.Context) ldcontext.Context {
	builder := ldcontext.NewBuilderFromContext(first)

	// Use the key from the second context
	builder.Key(second.Key())

	if second.Name().IsDefined() {
		builder.Name(second.Name().String())
	}

	for _, attr := range second.GetOptionalAttributeNames(nil) {
		builder.SetValue(attr, second.GetValue(attr))
	}

	for i := range second.PrivateAttributeCount() {
		if ref, ok := second.PrivateAttributeByIndex(i); ok {
			builder.PrivateRef(ref)
		}
	}

	return builder.Build()
}

// removeInvalid drops undefined or invalid (e.g. empty key) contexts that
// would otherwise poison the resulting multi-context.
func removeInvalid(contexts []ldcontext.Context) []ldcontext.Context {
	var result []ldcontext.Context

	for _, item := range contexts {
		if !item.IsDefined() || item.Err() != nil {
			continue
		}

		result = append(result, item)
	}

	return result
}

func mergeContexts(ctx context.Context, contexts []ldcontext.Context) ldcontext.Context {
	if embeddedContext, ok := getContext(ctx); ok {
		// Prepend embedded context so new contexts override old ones
		contexts = append([]ldcontext.Context{embeddedContext}, contexts...)
	}

	contexts = flattenContexts(contexts)

	contexts = removeInvalid(contexts)

	if len(contexts) == 0 {
		return ldcontext.NewWithKind("none", "none")
	}

	if len(contexts) == 1 {
		return contexts[0]
	}

	return ldcontext.NewMulti(contexts...)
}

func ClusterContext(clusterID uuid.UUID) ldcontext.Context {
	return ldcontext.NewWithKind(ClusterKind, clusterID.String())
}

func InstanceGroupContext(instanceGroupName string) ldcontext.Context {
	return ldcontext.NewWithKind(InstanceGroupKind, instanceGroupName)
}

func DeploymentEnvironmentContext(name string) ldcontext.Context {
	return ldcontext.NewWithKind(DeploymentEnvironmentKind, name)
}

func SandboxContext(sandboxID string) ldcontext.Context {
	return ldcontext.NewWithKind(SandboxKind, sandboxID)
}

func TeamContextWithName(teamID, teamName string) ldcontext.Context {
	return ldcontext.NewBuilder(teamID).
		Kind(TeamKind).
		Name(teamName).
		Build()
}

func TeamContext(teamID string) ldcontext.Context {
	return ldcontext.NewBuilder(teamID).
		Kind(TeamKind).
		Build()
}

func TierContext(tierID, tierName string) ldcontext.Context {
	return ldcontext.NewBuilder(tierID).
		Kind(TierKind).
		Name(tierName).
		Build()
}

func UserContext(userID string) ldcontext.Context {
	return ldcontext.NewWithKind(UserKind, userID)
}

func ServiceContext(serviceName string) ldcontext.Context {
	return ldcontext.NewWithKind(ServiceKind, serviceName)
}

func TemplateContext(templateID string) ldcontext.Context {
	return ldcontext.NewWithKind(TemplateKind, templateID)
}

func VolumeContext(volumeName string) ldcontext.Context {
	return ldcontext.NewWithKind(VolumeKind, volumeName)
}

func CompressFileTypeContext(fileType string) ldcontext.Context {
	return ldcontext.NewWithKind(CompressFileTypeKind, fileType)
}

func CompressUseCaseContext(useCase string) ldcontext.Context {
	return ldcontext.NewWithKind(CompressUseCaseKind, useCase)
}

// BatcherContext targets one named ClickHouse writer.
func BatcherContext(name string) ldcontext.Context {
	return ldcontext.NewWithKind(BatcherKind, name)
}
