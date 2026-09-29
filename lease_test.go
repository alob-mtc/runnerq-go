package runnerq

import "testing"

// leaseBackend records the leases an engine pushes into its backend.
type leaseBackend struct {
	*lifecycleBackend
	set []int64
}

func (b *leaseBackend) SetLeaseMS(ms int64) { b.set = append(b.set, ms) }

// The backend's own lease (postgres.WithConfig's) stands unless the worker
// config asks for another.
func TestEngineKeepsBackendLeaseUnlessConfigured(t *testing.T) {
	b := &leaseBackend{lifecycleBackend: newLifecycleBackend()}
	if _, err := Builder().Backend(b).Build(); err != nil {
		t.Fatal(err)
	}
	NewWorkerEngineWithBackend(b, DefaultWorkerConfig())
	if len(b.set) != 0 {
		t.Fatalf("default engines set the backend lease to %v", b.set)
	}
	cfg := DefaultWorkerConfig()
	lease := uint64(5_000)
	cfg.LeaseMS = &lease
	NewWorkerEngineWithBackend(b, cfg)
	if len(b.set) != 1 || b.set[0] != 5_000 {
		t.Fatalf("configured lease: %v", b.set)
	}
}
