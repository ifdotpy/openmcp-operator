package disconnectguard

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Resolve verifies the live APIBinding identity before returning its account client.
// The boolean is true only when the PM workspace has a deletion timestamp.
type Resolve func(context.Context, string, string, types.UID) (client.Client, bool, error)

type Inspector struct {
	Resolve  Resolve
	Services []schema.GroupVersionKind
}

func (i Inspector) Check(ctx context.Context, cluster, name string, uid types.UID) error {
	if i.Resolve == nil || len(i.Services) == 0 {
		return fmt.Errorf("disconnect guard is not configured")
	}
	account, deleting, err := i.Resolve(ctx, cluster, name, uid)
	if err != nil {
		return fmt.Errorf("cannot verify the OCP binding. Retry when the account API is available")
	}
	if deleting {
		return nil
	}
	if account == nil {
		return fmt.Errorf("cannot inspect the account")
	}
	for _, service := range i.Services {
		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(service.GroupVersion().WithKind(service.Kind + "List"))
		// No namespace filter: non-default service orders also prevent disconnect.
		if err := account.List(ctx, list, client.Limit(1)); err != nil {
			return fmt.Errorf("cannot verify %s orders. Disconnect is blocked", service.Kind)
		}
		if len(list.Items) > 0 {
			return fmt.Errorf("remove all %s installations before disconnecting OCP", service.Kind)
		}
	}
	return nil
}
