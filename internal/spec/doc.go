//go:generate go run -C ../../spec ./tools/gen -lang go -pkg spec -out ../internal/spec/spec.go
//go:generate go run -C ../../spec ./tools/gen -lang go -pkg spec -part schema -out ../internal/spec/schema.go

package spec
