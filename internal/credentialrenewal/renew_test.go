// Copyright 2026 OpenControlPlane contributors.
// SPDX-License-Identifier: Apache-2.0
package credentialrenewal

import (
	"context"
	"fmt"
	"testing"
	"time"

	authv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/clientcmd"
)

func TestReplacementAndFailure(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			now := time.Now().UTC()
			original := []byte(`{"apiVersion":"v1","kind":"Config","current-context":"test","contexts":[{"name":"test","context":{"cluster":"test","user":"test"}}],"clusters":[{"name":"test","cluster":{"server":"https://example.test"}}],"users":[{"name":"test","user":{"token":"old-token"}}]}`)
			host := fake.NewClientset(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "credential", Namespace: "hosting"}, Data: map[string][]byte{"kubeconfig": original}})
			remote := fake.NewClientset()
			requests := 0
			remote.PrependReactor("create", "serviceaccounts", func(action clienttesting.Action) (bool, runtime.Object, error) {
				requests++
				create := action.(clienttesting.CreateAction)
				if action.GetNamespace() != "identity" || action.GetSubresource() != "token" || create.GetObject().(*authv1.TokenRequest).Spec.ExpirationSeconds == nil {
					t.Fatal("wrong token request")
				}
				if fail {
					return true, nil, fmt.Errorf("denied")
				}
				return true, &authv1.TokenRequest{Status: authv1.TokenRequestStatus{Token: "new-token", ExpirationTimestamp: metav1.NewTime(now.Add(24 * time.Hour))}}, nil
			})
			connect := func(cfg *rest.Config) (kubernetes.Interface, error) {
				if cfg.BearerToken != "old-token" || cfg.Insecure {
					t.Fatal("wrong authentication")
				}
				return remote, nil
			}
			target := Target{Secret: "credential", Namespace: "identity", ServiceAccount: "controller", MountPath: "/etc/kcp/provider"}
			err := Renew(context.Background(), host, "hosting", target, connect, now)
			secret, _ := host.CoreV1().Secrets("hosting").Get(context.Background(), "credential", metav1.GetOptions{})
			if fail {
				if err == nil || string(secret.Data["kubeconfig"]) != string(original) || len(secret.Data["token"]) > 0 {
					t.Fatal("failure replaced the credential")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			inline, err := clientcmd.RESTConfigFromKubeConfig(secret.Data["kubeconfig"])
			if err != nil || inline.BearerToken != "new-token" {
				t.Fatal("missing replacement inline token")
			}
			mounted, err := clientcmd.Load(secret.Data["mounted-kubeconfig"])
			if err != nil {
				t.Fatal(err)
			}
			if mounted.AuthInfos["test"].Token != "" || mounted.AuthInfos["test"].TokenFile != "/etc/kcp/provider/token" || string(secret.Data["token"]) != "new-token" {
				t.Fatal("mounted credential cannot reload token")
			}
			if err := Renew(context.Background(), host, "hosting", target, connect, now.Add(time.Hour)); err != nil {
				t.Fatal(err)
			}
			if requests != 1 {
				t.Fatal("renewed before threshold")
			}
		})
	}
}
