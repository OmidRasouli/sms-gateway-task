# SMS Gateway — Final Day Task List

**Timeline:** today (final day of a 7-day challenge; most of it is already spent).
**Goal:** ship a working SMS Gateway (REST API + async worker) that lets businesses
top up a prepaid balance, send Normal and Express SMS, and pull reports — plus a
system architecture design document.

## Deliverables (per challenge brief)

- [ ] System architecture design document — `docs/architecture.md`
- [ ] Link to the implemented project on GitHub/GitLab

## Already done (do not rebuild — verify and extend only)

- Go module scaffolded: `cmd/{api,worker}`, `internal/{config,domain,handler/http,
  operator,queue,repository/postgres,service,worker}`, `migrations/`
- Endpoints: `POST /api/v1/messages`, `GET /api/v1/messages/:userID`,
  `GET /api/v1/messages/:userID/:id`, `GET /api/v1/balance/:userID`,
  `POST /api/v1/balance/charge`
- Atomic balance deduction: `UPDATE balances SET amount = amount - $1 WHERE
  user_id = $2 AND amount >= $1`, in the same DB transaction as message insert
- Asynq priority queues: `express` (weight 6), `normal` (weight 3)
- Idempotent worker handler (checks `status == pending` before acting)
- Mock operator adapter (`internal/operator/mock.go`)
- `docker-compose.yml`: postgres, redis, migrate, api, worker, asynqmon
- Migrations: `balances`, `messages`, `balance_transactions`

## Explicitly out of scope — do not build these

The brief explicitly excludes some of this; the rest is just more than a single day
allows. Note anything relevant as a "known limitation" in the architecture doc
instead of building it.

- Authentication / user management system (**explicitly excluded by the brief**)
- Real telecom operator integration (mock only)
- Multi-part / concatenated SMS handling
- GUI
- Per-user rate limiting
- Sharded per-customer queues or weighted fair dequeue (queue-level `express`/
  `normal` priority weighting already satisfies the SLA requirement)
- Refund / ledger system on permanent send failure
- Reconciliation sweep for messages stuck pre-enqueue
- Kubernetes manifests
- Fairness demo script / load test

## Today's priority order (stop at any point — earlier items matter more)

1. [ ] Confirm the scaffolded code is actually committed to the repo, not just
       sitting in a downloaded zip
2. [ ] `go mod tidy`, fix compile errors, commit `go.sum`
3. [ ] `docker compose up --build` — confirm postgres, redis, migrate, api,
       worker, asynqmon all come up cleanly end-to-end
4. [ ] **Concurrency test for the balance debit** (the single most important
       correctness property in the system): fire N goroutines at `DeductTx` for
       a user whose balance covers only 1, assert exactly 1 succeeds and the
       balance never goes negative
5. [ ] Add `guaranteed_delivery_seconds` to the `POST /messages` response when
       `type == express` — satisfies the brief's explicit requirement that the
       guaranteed delivery time be provided to the customer
6. [ ] Basic input validation: phone number format, non-empty text, max length
7. [ ] Consistent JSON error shape across all handlers
8. [ ] `GET /healthz` that also checks DB + Redis connectivity
9. [ ] Write `docs/architecture.md` (outline below)
10. [ ] Update `README.md` with setup/run/curl examples
11. [ ] Push to GitHub/GitLab, confirm the deliverable link works

## `docs/architecture.md` outline

- System overview + diagram (`docs/architecture.drawio`)
- Component responsibilities: API, PostgreSQL, Asynq/Redis, Worker, Operator adapter
- Decision log: atomic `UPDATE...WHERE` vs `SELECT FOR UPDATE`; Asynq vs Kafka/
  RabbitMQ; sync-write/async-deliver split; Gin; pgx over an ORM
- Balance consistency guarantee — one paragraph, referencing the concurrency test
- Express SLA mechanism — queue-weight based, not per-message reservation
- Scale reasoning: ~100M/day ≈ ~1,157 msg/s average; this implementation
  demonstrates the pattern (atomic deduction, async delivery, priority queueing);
  true 100M/day production load would need connection pool tuning, horizontal
  worker scaling, and likely balance-write sharding
- Known limitations: no refund on permanent failure, no reconciliation sweep for
  crashed-before-enqueue messages, no per-customer rate limiting beyond queue
  priority weighting

## Stretch (only if everything above is done with time to spare)

- [ ] Document the running Asynqmon dashboard in the README (already up via
      docker-compose — just needs a screenshot/link)
- [ ] One more unit test: worker handler idempotency on retry