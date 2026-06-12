package runnerq

import "context"

// handlerScopeKey marks contexts that originate inside processActivity, so
// ActivityFuture.GetResult can tell an in-handler await (which may yield-park
// the activity) from an external caller's await (which must block normally —
// there is no activity row to park).
type handlerScopeKey struct{}

func withHandlerScope(ctx context.Context) context.Context {
	return context.WithValue(ctx, handlerScopeKey{}, struct{}{})
}

func inHandlerScope(ctx context.Context) bool {
	return ctx.Value(handlerScopeKey{}) != nil
}
