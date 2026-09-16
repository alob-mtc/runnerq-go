package runnerq

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alob-mtc/runnerq-go/storage"
	"github.com/alob-mtc/runnerq-go/storage/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Runs only as the child of the process-death test below. The checkpoint has
// committed when READY is printed; the handler then cannot acknowledge.
func TestRecoveryWorkerProcessHelper(t *testing.T) {
	queue := os.Getenv("RUNNERQ_RECOVERY_HELPER_QUEUE")
	if queue == "" {
		t.Skip("subprocess helper")
	}
	b, err := postgres.New(context.Background(), contractDSN(t), queue)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	e, err := Builder().Backend(b).QueueName(queue).MaxWorkers(1).Build()
	if err != nil {
		t.Fatal(err)
	}
	e.RegisterActivityWithName("resilient", &funcHandler{fn: func(c ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		_, err := c.Run("charge", func() (json.RawMessage, error) { return json.RawMessage(`{"charged":true}`), nil })
		if err != nil {
			return nil, err
		}
		fmt.Println("RUNNERQ_CHECKPOINT_READY")
		<-c.Ctx.Done()
		return nil, c.Ctx.Err()
	}})
	if err := e.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// CONTRACT 3: kill a real worker after its checkpoint commits, prove another
// claim occurs, and prove recovery replays that checkpoint. This does not claim
// exactly-once effects if a process dies BEFORE committing its checkpoint.
func TestContract_SideEffectExactlyOnceAcrossCrashRecovery(t *testing.T) {
	dsn := contractDSN(t)
	queue := contractQueue()
	ctx := context.Background()
	b := contractBackend(t, dsn, queue)
	e, err := Builder().Backend(b).QueueName(queue).MaxWorkers(1).Build()
	if err != nil {
		t.Fatal(err)
	}
	var rerun atomic.Int32
	e.RegisterActivityWithName("resilient", &funcHandler{fn: func(c ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		return c.Run("charge", func() (json.RawMessage, error) { rerun.Add(1); return json.RawMessage(`"unexpected reexecution"`), nil })
	}})
	fut, err := e.GetActivityExecutor().Activity("resilient").Payload(json.RawMessage(`{}`)).Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	child := exec.Command(os.Args[0], "-test.run=^TestRecoveryWorkerProcessHelper$", "-test.timeout=45s")
	child.Env = append(os.Environ(), "RUNNERQ_RECOVERY_HELPER_QUEUE="+queue)
	stdout, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	child.Stderr = os.Stderr
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		if !waited {
			_ = child.Process.Kill()
			_ = child.Wait()
		}
	}()
	ready := make(chan bool, 1)
	go func() {
		scan := bufio.NewScanner(stdout)
		for scan.Scan() {
			if scan.Text() == "RUNNERQ_CHECKPOINT_READY" {
				ready <- true
				return
			}
		}
		ready <- false
	}()
	select {
	case ok := <-ready:
		if !ok {
			t.Fatal("worker exited before checkpoint")
		}
	case <-time.After(20 * time.Second):
		t.Fatal("worker never checkpointed")
	}
	snap, err := b.GetActivity(ctx, fut.ActivityID())
	if err != nil || snap == nil || snap.CurrentWorkerID == nil {
		t.Fatalf("original claim: %v %v", snap, err)
	}
	oldWorker := *snap.CurrentWorkerID
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = child.Wait()
	waited = true
	// Advance only the test row's lease; no real-time 310-second sleep or
	// assumption that the configured backend lease beats the timeout floor.
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `UPDATE runnerq_activities SET lease_deadline_ms=(EXTRACT(EPOCH FROM NOW())*1000)::bigint-1000 WHERE id=$1`, fut.ActivityID()); err != nil {
		t.Fatal(err)
	}
	if n, err := b.RequeueExpired(ctx, 10); err != nil || n != 1 {
		t.Fatalf("must recover one expired claim: n=%d err=%v", n, err)
	}
	startEngine(t, e)
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	out, err := fut.GetResult(waitCtx)
	if err != nil || string(out) != `{"charged": true}` && string(out) != `{"charged":true}` {
		t.Fatalf("recovered output=%s err=%v", out, err)
	}
	snap, err = b.GetActivity(ctx, fut.ActivityID())
	if err != nil || snap.RetryCount != 1 || snap.LastWorkerID == nil || *snap.LastWorkerID == oldWorker {
		t.Fatalf("second claim not proven: %v %v", snap, err)
	}
	if rerun.Load() != 0 {
		t.Fatal("checkpointed function ran again")
	}
	if err := b.AckSuccess(ctx, fut.ActivityID(), json.RawMessage(`"stale"`), oldWorker); err == nil {
		t.Fatal("dead worker's acknowledgement accepted")
	}
}

type lostCompletionReplyBackend struct {
	*postgres.PostgresBackend
	replies atomic.Int32
}

func (b *lostCompletionReplyBackend) AckSuccess(ctx context.Context, id uuid.UUID, result json.RawMessage, worker string) error {
	if err := b.PostgresBackend.AckSuccess(ctx, id, result, worker); err != nil {
		return err
	}
	if b.replies.Add(1) == 1 {
		return storage.NewUnavailableError("injected lost commit reply")
	}
	return nil
}
func TestEngineReconcilesLostPostgresCompletionReply(t *testing.T) {
	dsn := contractDSN(t)
	queue := contractQueue()
	b := &lostCompletionReplyBackend{PostgresBackend: contractBackend(t, dsn, queue)}
	e, err := Builder().Backend(b).QueueName(queue).MaxWorkers(1).Build()
	if err != nil {
		t.Fatal(err)
	}
	var runs atomic.Int32
	e.RegisterActivityWithName("receipt", &funcHandler{fn: func(ActivityContext, json.RawMessage) (json.RawMessage, error) {
		runs.Add(1)
		return json.RawMessage(`"original"`), nil
	}})
	startEngine(t, e)
	f, err := e.GetActivityExecutor().Activity("receipt").Payload(json.RawMessage(`{}`)).MaxRetries(1).Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if out, err := f.GetResult(ctx); err != nil || string(out) != `"original"` {
		t.Fatalf("output=%s err=%v", out, err)
	}
	e.Stop() // drain the retained completion, including reconciliation
	deadline := time.Now().Add(3 * time.Second)
	for b.replies.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if runs.Load() != 1 || b.replies.Load() != 2 {
		t.Fatalf("handler=%d ack=%d", runs.Load(), b.replies.Load())
	}
}

func TestRehydratedFutureRegistersIndependentConsumers(t *testing.T) {
	rig := newStepsRig(t, func(e *WorkerEngine) {
		e.RegisterActivityWithName("producer", &funcHandler{fn: func(c ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
			select {
			case <-time.After(3 * time.Second):
				return json.RawMessage(`"shared"`), nil
			case <-c.Ctx.Done():
				return nil, c.Ctx.Err()
			}
		}})
		e.RegisterActivityWithName("consumer", &funcHandler{fn: func(c ActivityContext, p json.RawMessage) (json.RawMessage, error) {
			var id uuid.UUID
			if err := json.Unmarshal(p, &id); err != nil {
				return nil, err
			}
			return FutureFor(e.backend, id).GetResult(c.Ctx)
		}})
	})
	f, err := rig.engine.GetActivityExecutor().Activity("producer").Payload(json.RawMessage(`{}`)).Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(f.ActivityID())
	var consumers []*ActivityFuture
	for range 2 {
		c, err := rig.engine.GetActivityExecutor().Activity("consumer").Payload(payload).Execute(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		consumers = append(consumers, c)
	}
	for _, c := range consumers {
		if result := rig.await(t, c.ActivityID(), 10*time.Second); string(result.Data) != `"shared"` {
			t.Fatalf("consumer output=%s", result.Data)
		}
	}
}
