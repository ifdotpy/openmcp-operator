package disconnectguard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"testing"

	admission "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

func TestAdmissionBoundary(t *testing.T) {
	for _, tc := range []struct {
		name            string
		operation       admission.Operation
		old             string
		inspectErr      error
		called, allowed bool
	}{
		{"empty workspace", admission.Delete, `{"apiVersion":"apis.kcp.io/v1alpha2","kind":"APIBinding","metadata":{"name":"services","uid":"binding-uid","annotations":{"kcp.io/cluster":"workspace"}}}`, nil, true, true},
		{"orders remain", admission.Delete, `{"apiVersion":"apis.kcp.io/v1alpha2","kind":"APIBinding","metadata":{"name":"services","uid":"binding-uid","annotations":{"kcp.io/cluster":"workspace"}}}`, errors.New("Remove installed services before deleting the APIBinding"), true, false},
		{"API unavailable", admission.Delete, `{"apiVersion":"apis.kcp.io/v1alpha2","kind":"APIBinding","metadata":{"name":"services","uid":"binding-uid","annotations":{"kcp.io/cluster":"workspace"}}}`, errors.New("Cannot verify service orders"), true, false},
		{"missing cluster", admission.Delete, `{"apiVersion":"apis.kcp.io/v1alpha2","kind":"APIBinding","metadata":{"name":"services","uid":"binding-uid"}}`, nil, false, false},
		{"invalid cluster", admission.Delete, `{"apiVersion":"apis.kcp.io/v1alpha2","kind":"APIBinding","metadata":{"name":"services","uid":"binding-uid","annotations":{"kcp.io/cluster":"../other"}}}`, nil, false, false},
		{"wrong name", admission.Delete, `{"apiVersion":"apis.kcp.io/v1alpha2","kind":"APIBinding","metadata":{"name":"other","uid":"binding-uid","annotations":{"kcp.io/cluster":"workspace"}}}`, nil, false, false},
		{"unexpected update", admission.Update, `{}`, nil, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			h := Handler{Inspect: func(_ context.Context, cluster, name string, uid types.UID) error {
				called = true
				if cluster != "workspace" || name != "services" || uid != "binding-uid" {
					t.Fatal("wrong identity")
				}
				return tc.inspectErr
			}}
			in := admission.AdmissionReview{Request: &admission.AdmissionRequest{UID: "request-uid", Name: "services", Operation: tc.operation, Resource: metav1.GroupVersionResource{Group: "apis.kcp.io", Version: "v1alpha2", Resource: "apibindings"}, OldObject: runtime.RawExtension{Raw: []byte(tc.old)}}}
			data, _ := json.Marshal(in)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest("POST", "/disconnect", bytes.NewReader(data)))
			var out admission.AdmissionReview
			if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
				t.Fatal(err)
			}
			if out.Response == nil || out.Response.UID != "request-uid" || out.Response.Allowed != tc.allowed || called != tc.called {
				t.Fatalf("unexpected response: %s", w.Body.String())
			}
		})
	}
}
