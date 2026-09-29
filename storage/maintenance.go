package storage

// ManagedMaintenanceStorage marks services that run lease recovery and
// retention themselves. Engine retention settings are then rejected rather
// than silently ignored; configure retention on the service.
type ManagedMaintenanceStorage interface {
	MaintenanceManaged() bool
}
