// Copyright 2026 OpenControlPlane contributors.
// SPDX-License-Identifier: Apache-2.0

// Package disconnectguard checks service objects before an APIBinding deletion.
package disconnectguard

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	admission "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

// Inspect must verify the binding UID and inspect live orders in all namespaces.
// It may allow deletion when the enclosing workspace is being deleted.
// Any inspection error prevents deletion.
type Inspect func(context.Context, string, string, types.UID) error

type Handler struct{ Inspect Inspect }

func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var review admission.AdmissionReview
	if err := json.NewDecoder(r.Body).Decode(&review); err != nil || review.Request == nil {
		http.Error(w, "Invalid admission review", http.StatusBadRequest)
		return
	}
	req := review.Request
	response := &admission.AdmissionResponse{UID: req.UID}
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	err := h.check(ctx, req)
	response.Allowed = err == nil
	if err != nil {
		response.Result = &metav1.Status{Code: http.StatusForbidden, Reason: metav1.StatusReasonForbidden, Message: err.Error()}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(admission.AdmissionReview{TypeMeta: metav1.TypeMeta{APIVersion: "admission.k8s.io/v1", Kind: "AdmissionReview"}, Response: response})
}

func (h Handler) check(ctx context.Context, req *admission.AdmissionRequest) error {
	if req.Operation != admission.Delete || req.Resource.Group != "apis.kcp.io" || req.Resource.Resource != "apibindings" || req.SubResource != "" {
		return fmt.Errorf("unexpected operation for APIBinding disconnect guard")
	}
	var binding unstructured.Unstructured
	if err := json.Unmarshal(req.OldObject.Raw, &binding); err != nil {
		return fmt.Errorf("cannot read the APIBinding")
	}
	cluster := binding.GetAnnotations()["kcp.io/cluster"]
	if cluster == "" || len(cluster) > 55 || len(validation.IsDNS1123Label(cluster)) > 0 || binding.GetUID() == "" || binding.GetName() != req.Name {
		return fmt.Errorf("API binding identity is missing or invalid")
	}
	if h.Inspect == nil {
		return fmt.Errorf("disconnect guard is unavailable")
	}
	return h.Inspect(ctx, cluster, binding.GetName(), binding.GetUID())
}
