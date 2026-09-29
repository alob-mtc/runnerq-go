package runnerq

// attemptQueueKey carries the executing attempt's queue on contexts that
// originate inside processActivity. Its presence is what tells an in-handler
// ActivityFuture.GetResult (which may yield-park the activity, and hands
// rehydrated futures the attempt's identity) from an external caller's await
// (which must block normally — there is no activity row to park).
type attemptQueueKey struct{}
