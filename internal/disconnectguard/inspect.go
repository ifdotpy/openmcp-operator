package disconnectguard

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Resolve verifies the live APIBinding identity before returning its workspace client.
// The boolean is true only when the workspace has a deletion timestamp.
type Resolve func(context.Context, string, string, types.UID) (client.Client, bool, error)

// Inspector checks configured services after Resolve verifies the workspace.
// Resolve can also inspect persisted services for providers removed from configuration.
type Inspector struct {
	Resolve  Resolve
	Services []schema.GroupVersionKind
}

func (i Inspector) Check(ctx context.Context, cluster, name string, uid types.UID) error {
	if i.Resolve == nil {
		return fmt.Errorf("disconnect guard is not configured")
	}
	workspace, deleting, err := i.Resolve(ctx, cluster, name, uid)
	if err != nil {
		return fmt.Errorf("cannot verify the protected APIBinding while the workspace API is unavailable")
	}
	if deleting {
		return nil
	}
	if workspace == nil {
		return fmt.Errorf("cannot inspect the workspace")
	}
	for _, service := range i.Services {
		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(service.GroupVersion().WithKind(service.Kind + "List"))
		// No namespace filter: non-default service orders also prevent disconnect.
		if err := workspace.List(ctx, list, client.Limit(1)); err != nil {
			return fmt.Errorf("cannot verify %s objects; APIBinding deletion is blocked", service.Kind)
		}
		if len(list.Items) > 0 {
			return fmt.Errorf("remove all %s objects before deleting the APIBinding", service.Kind)
		}
	}
	return nil
}
