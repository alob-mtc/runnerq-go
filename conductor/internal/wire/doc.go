//go:generate go run -C ../../../spec ./tools/gen -part conductor -lang go -pkg wire -out ../conductor/internal/wire/wire.go

// Package wire holds the Conductor protocol's types, generated from the
// runnerq-spec submodule's protocol/conductor/conductor.schema.json.
package wire
