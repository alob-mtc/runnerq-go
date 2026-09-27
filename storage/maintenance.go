package storage

// ManagedMaintenanceStorage identifies services that perform lease recovery and
// retention independently of SDK workers. Configure retention on the service;
// engine retention settings are rejected to avoid silently ignoring a policy.
// Existing backends without this capability keep worker-owned maintenance.
type ManagedMaintenanceStorage interface {
	MaintenanceManaged() bool
}
