//go:build !integration

package test

import (
	"os/exec"
	"strings"
	"testing"
)

// Coverage: service-platform service runs from its own directory and image.

func TestV0048ChangeToOneServiceSelectsOnlyItsTests(t *testing.T) {
	// The service's packages import no other service, so a change elsewhere
	// cannot change its build.
	const module = "github.com/pandops/flowai-source-controller"
	out, err := exec.Command("go", "list", "-deps", "-test", module+"/...").CombinedOutput()
	if err != nil {
		t.Fatalf("go list: %v %s", err, out)
	}
	for _, dep := range strings.Fields(string(out)) {
		if dep == module || strings.HasPrefix(dep, module+"/") {
			continue
		}
		if strings.HasPrefix(dep, "github.com/flowai/platform/") || strings.HasPrefix(dep, "github.com/pandops/flowai-") {
			t.Fatalf("source-controller depends on %s", dep)
		}
	}
}
