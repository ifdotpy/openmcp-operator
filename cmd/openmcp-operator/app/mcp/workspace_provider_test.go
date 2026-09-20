package mcp

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadWorkspaceProviders(t *testing.T) {
	valid := `[
  {
    "name": "example-provider",
    "image": "registry.example/provider:v1",
    "providerName": "example-config",
    "resource": {"group": "services.example.io", "version": "v1alpha1", "kind": "Example"},
	"roleRules": [{"apiGroups": ["workloads.example.io"], "resources": ["releases"], "verbs": ["get", "list", "watch"]}],
    "clusterRoleRules": [{"apiGroups": ["services.example.io"], "resources": ["providerconfigs"], "verbs": ["get", "list", "watch"]}]
  }
]`
	for _, tc := range []struct {
		name, input, problem string
		count                int
	}{
		{name: "empty path", count: 0},
		{name: "valid", input: valid, count: 1},
		{name: "unknown field", input: strings.Replace(valid, `"image":`, `"unexpected": true, "image":`, 1), problem: "unknown field"},
		{name: "duplicate deployment name", input: valid[:len(valid)-1] + "," + valid[1:], problem: "duplicated"},
		{name: "missing resource kind", input: strings.Replace(valid, `, "kind": "Example"`, "", 1), problem: "incomplete resource GVK"},
		{name: "empty RBAC verbs", input: strings.Replace(valid, `"verbs": ["get", "list", "watch"]`, `"verbs": []`, 1), problem: "must set"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := ""
			if tc.input != "" {
				path = filepath.Join(t.TempDir(), "providers.json")
				if err := os.WriteFile(path, []byte(tc.input), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			providers, err := loadWorkspaceProviders(path)
			if tc.problem != "" {
				if err == nil || !strings.Contains(err.Error(), tc.problem) {
					t.Fatalf("got %v, want error containing %q", err, tc.problem)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(providers) != tc.count {
				t.Fatalf("got %d providers, want %d", len(providers), tc.count)
			}
		})
	}
}
