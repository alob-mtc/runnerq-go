package runnerq

import (
	"fmt"
	"reflect"
)

// NameOf returns the activity type RegisterActivity and
// ActivityExecutor.Activity derive from T, for APIs that take it as a string:
//
//	engine.RegisterActivity(&ResizeImage{})
//	runnerq.Builder().ActivityTypes([]string{runnerq.NameOf[ResizeImage]()})
//	engine.SignalByKey(ctx, runnerq.NameOf[ResizeImage](), key, "approved", p)
//
// T may be the handler struct or a pointer to it. NameOf panics for unnamed
// types, and knows nothing of names pinned with RegisterActivityWithName.
func NameOf[T any]() string {
	name, err := activityTypeOf(reflect.TypeOf((*T)(nil)).Elem())
	if err != nil {
		panic(err)
	}
	return name
}

// activityTypeOf is the bare name of t after dereferencing pointers. Bare
// names keep import paths out of every stored row; collisions surface at
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
