# 01 — Hello, Workflow

The smallest useful RunnerQ program: a durable two-step workflow.

## What this shows

A workflow is just an activity handler. Inside it, each `ctx.Run(name, fn)` is
a **step** whose result is checkpointed in Postgres. Steps run at most once —
if the process restarted between them, the first would be skipped and the
second resumed. (Example 02 proves that; here we just run it through.)

## Run it

```bash
docker compose up -d        # from the examples/ directory, if not already up
go run .
```

## Expected output

```
starting signup workflow...
  ▶ creating account for ada@example.com
  ▶ sending welcome email to ada@example.com
✓ signup complete: {"email":"ada@example.com","user_id":"u_1001"}
```

## The key idea

```go
func (h *SignupWorkflow) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
    user, err := ctx.Run("create-account", func() (json.RawMessage, error) {
        return createAccount(payload)   // checkpointed: runs at most once
    })
    if err != nil { return nil, err }

    _, err = ctx.Run("send-welcome", func() (json.RawMessage, error) {
        return sendWelcome(user)        // only after the account is durable
    })
    return user, err
}
```

Next: [02 — Crash & Resume](../02-crash-and-resume/) shows why the checkpoints matter.
