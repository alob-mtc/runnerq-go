package runnerq

import (
	"encoding/json"
	"testing"
)

type namedHandler struct{ DefaultDeadLetterHandler }

func (namedHandler) Handle(ActivityContext, json.RawMessage) (json.RawMessage, error) {
	return nil, nil
}

func TestNameOfDerivesBareTypeName(t *testing.T) {
	if got := NameOf[namedHandler](); got != "namedHandler" {
		t.Fatalf("NameOf[T] = %q", got)
	}
	if got := NameOf[*namedHandler](); got != "namedHandler" {
		t.Fatalf("NameOf[*T] = %q, want pointer dereferenced", got)
	}
	if got := NameOf[**namedHandler](); got != "namedHandler" {
		t.Fatalf("NameOf[**T] = %q", got)
	}
	mustPanic(t, func() { NameOf[struct{ DefaultDeadLetterHandler }]() })
	mustPanic(t, func() { NameOf[func()]() })
}

func TestRegisterActivityDerivesNameAndRejectsBadInput(t *testing.T) {
	e := NewWorkerEngineWithBackend(newLifecycleBackend(), DefaultWorkerConfig())

	e.RegisterActivity(&namedHandler{})
	if _, ok := e.handlers[NameOf[namedHandler]()]; !ok {
		t.Fatalf("handler not registered under derived name; have %v", e.handlers)
	}
	// Value receivers register under the same name as pointers.
	mustPanic(t, func() { e.RegisterActivity(namedHandler{}) })
	// Duplicate names are a configuration bug, for pinned names too.
	mustPanic(t, func() { e.RegisterActivityWithName("namedHandler", &funcHandler{}) })
	mustPanic(t, func() { e.RegisterActivity(nil) })
	mustPanic(t, func() { e.RegisterActivityWithName("", &funcHandler{}) })
	mustPanic(t, func() { e.RegisterActivityWithName("x", nil) })

	// Unnamed handler types need a pinned name.
	anon := &struct {
		DefaultDeadLetterHandler
		funcHandler
	}{}
	mustPanic(t, func() { e.RegisterActivity(anon) })
	e.RegisterActivityWithName("anon", anon)
	if _, ok := e.handlers["anon"]; !ok {
		t.Fatal("pinned registration of unnamed type failed")
	}
}
