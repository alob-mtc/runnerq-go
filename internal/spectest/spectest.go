// Package spectest reads runnerq-spec vector files (the spec/ submodule) for
// tests.
package spectest

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
)

// Case is one vector: Input goes in, Output must come out.
type Case[In, Out any] struct {
	Name   string `json:"name"`
	Input  In     `json:"input"`
	Output Out    `json:"output"`
}

// Path is the path of a spec file, given relative to the spec root.
func Path(rel string) string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..", "spec", filepath.FromSlash(rel))
}

// Read reads a spec file, given relative to the spec root.
func Read(t testing.TB, rel string) []byte {
	t.Helper()
	raw, err := os.ReadFile(Path(rel))
	if err != nil {
		t.Fatalf("%v (is the spec submodule checked out? git submodule update --init)", err)
	}
	return raw
}

// Load reads the cases of a vector file, given relative to the spec root
// (e.g. "vectors/checkpoint_id.json").
func Load[In, Out any](t testing.TB, rel string) []Case[In, Out] {
	t.Helper()
	raw := Read(t, rel)
	var f struct {
		Description string          `json:"description"`
		Cases       []Case[In, Out] `json:"cases"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		t.Fatalf("%s: %v", rel, err)
	}
	if len(f.Cases) == 0 {
		t.Fatalf("%s: no cases", rel)
	}
	return f.Cases
}

// SameJSON reports whether a and b encode the same JSON value.
func SameJSON(a, b []byte) bool {
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}
	return reflect.DeepEqual(x, y)
}
