package examples_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestExamples runs every example and compares its output with
// expected_output.txt, so the documented output cannot rot.
func TestExamples(t *testing.T) {
	dirs, err := filepath.Glob("*/expected_output.txt")
	if err != nil || len(dirs) == 0 {
		t.Fatalf("no examples found: %v", err)
	}
	for _, f := range dirs {
		name := filepath.Dir(f)
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			want, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			out, err := exec.Command("go", "run", "./"+name).CombinedOutput()
			if err != nil {
				t.Fatalf("go run: %v\n%s", err, out)
			}
			norm := func(b []byte) string { return strings.ReplaceAll(string(b), "\r\n", "\n") }
			if norm(out) != norm(want) {
				t.Fatalf("output differs\n--- got\n%s\n--- want\n%s", out, want)
			}
		})
	}
}

// TestReadmeQuickstartInSync fails when the README quick start drifts from the example.
func TestReadmeQuickstartInSync(t *testing.T) {
	src, err := os.ReadFile("quickstart/main.go")
	if err != nil {
		t.Fatal(err)
	}
	readme, err := os.ReadFile("../README.md")
	if err != nil {
		t.Fatal(err)
	}
	norm := func(b []byte) string { return strings.ReplaceAll(string(b), "\r\n", "\n") }
	if !strings.Contains(norm(readme), strings.TrimRight(norm(src), "\n")) {
		t.Fatal("README quick start differs from examples/quickstart/main.go")
	}
}
