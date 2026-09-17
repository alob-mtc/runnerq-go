package runnerq

import (
	"fmt"
	"reflect"
)

// NameOf returns the activity type derived from T, exactly as RegisterActivity
// derives it from a handler value and ActivityExecutor.Activity derives it
// for a spawn. Use it where an API takes the type as a string:
//
//	engine.RegisterActivity(&ResizeImage{})
//	runnerq.Builder().ActivityTypes([]string{runnerq.NameOf[ResizeImage]()})
//	engine.SignalByKey(ctx, runnerq.NameOf[ResizeImage](), key, "approved", p)
//
// T may be the handler struct or a pointer to it; both yield the same name.
// NameOf panics for types with no name (anonymous structs, unnamed func
// types). It knows nothing about registration: a handler registered under a
// pinned name via RegisterActivityWithName is addressed by that name.
func NameOf[T any]() string {
	name, err := activityTypeOf(reflect.TypeOf((*T)(nil)).Elem())
	if err != nil {
		panic(err)
	}
	return name
}

// activityTypeOf derives an activity type from a Go type: pointers are
// dereferenced and the bare type name is used, without package qualification
// or case transforms. Bare names keep import paths out of the store (and out
// of every persisted activity row) while still surfacing collisions at
// registration.
func activityTypeOf(t reflect.Type) (string, error) {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Name() == "" {
		return "", &WorkerError{
			Kind:    ErrConfiguration,
			Message: fmt.Sprintf("cannot derive an activity type from unnamed type %s; use RegisterActivityWithName", t),
		}
	}
	return t.Name(), nil
}
