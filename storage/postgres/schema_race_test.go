package postgres

import (
	"context"
	"os"
	"sync"
	"testing"
)

// Many backends initializing concurrently (multiple processes booting, or
// parallel test packages) must not deadlock on the schema DDL: ALTER TABLE
// takes ACCESS EXCLUSIVE before evaluating IF NOT EXISTS, so unserialized
// concurrent inits take conflicting locks in different orders.
func TestConcurrentSchemaInitDoesNotDeadlock(t *testing.T) {
	dsn := os.Getenv("RUNNERQ_TEST_DSN")
	if dsn == "" {
		t.Skip("RUNNERQ_TEST_DSN not set; skipping integration test")
	}
	const n = 10
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b, err := WithConfig(context.Background(), dsn, "t_schema_race", 30_000, 2)
			if err != nil {
				errs <- err
				return
			}
			b.Close()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent init: %v", err)
	}
}
