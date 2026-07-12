# SMS Gateway

A prepaid SMS gateway built in Go. Businesses top up a balance, then submit Normal or Express SMS messages via a REST API. Messages are delivered asynchronously through Kafka priority queues while balances are deducted atomically in the same database transaction.

## Architecture

```
Client
  │  HTTP
  ▼
API (Gin)
  ├── Atomic balance deduction  ─┐
  └── Message insert (pending)  ─┴─▶ PostgreSQL
                                         │
                  Kafka publish ◀────────┘
                       │
            ┌──────────┴──────────┐
         sms.express           sms.normal
            └──────────┬──────────┘
                       ▼
                   Worker
                       ├── Operator (mock)
                       ├── Update status → sent / failed
                       └── On permanent failure → reverse balance deduction
```

**Components:**

| Component | Responsibility |
|-----------|---------------|
| **API** | Validates requests, atomically deducts balance + inserts message, publishes to Kafka |
| **PostgreSQL** | Source of truth for balances, messages, transactions, and pricing |
| **Kafka** | Two priority topics: `sms.express` and `sms.normal` (+ DLQ variants) |
| **Worker** | Idempotent consumer — delivers to mock operator, updates status, reverses balance on permanent failure |
| **Operator adapter** | Swappable interface; ships with a mock that simulates ~1% permanent and ~2% transient failures |

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) + [Docker Compose](https://docs.docker.com/compose/)
- Go 1.23+ (only needed for running tests or the service locally without Docker)

## Quick Start

```bash
# Clone and start all services (postgres, kafka, migrate, api, worker)
git clone https://github.com/OmidRasouli/sms-gateway-task.git
cd sms-gateway-task
make up
```

The API will be available at `http://localhost:8080`.

To stop and remove volumes:

```bash
make down
```

## Running Locally (without Docker)

Start PostgreSQL and Kafka separately (or use `docker compose up postgres kafka`), then:

```bash
make run-api     # starts the HTTP API on :8080
make run-worker  # starts the Kafka consumer in a separate terminal
```

Apply migrations first if needed:

```bash
make migrate-up
```

## Configuration

All configuration is via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8080` | HTTP listen port |
| `DATABASE_URL` | *(required)* | PostgreSQL DSN |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated Kafka broker addresses |
| `EXPRESS_CONCURRENCY` | `15` | Worker goroutines consuming `sms.express` |
| `NORMAL_CONCURRENCY` | `10` | Worker goroutines consuming `sms.normal` |
| `PRICE_CACHE_REFRESH_INTERVAL` | `5m` | How often the in-memory price cache is refreshed from the DB |
| `MAX_RETRY_ATTEMPTS` | `3` | Maximum Kafka consumer retry attempts before a message is sent to the DLQ |

## API Reference

All endpoints accept and return `application/json`. The caller identifies itself via the `X-User-ID` header (a UUID). Authentication is **explicitly out of scope** for this challenge.

### Health Check

```
GET /healthz
```

Returns `200 OK` when the service is up.

---

### Send a Message

```
POST /api/v1/messages
X-User-ID: <uuid>
```

**Request body:**

```json
{
  "phone_number": "+14155552671",
  "text": "Hello!",
  "type": "normal"
}
```

`type` is either `normal` or `express`.

**Response `202 Accepted`:**

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "user_id": "...",
  "phone_number": "+14155552671",
  "text": "Hello!",
  "type": "normal",
  "price": 10,
  "status": "pending",
  "created_at": "2026-07-12T10:00:00Z"
}
```

**Error responses:**

| Status | Condition |
|--------|-----------|
| `400` | Missing/invalid `X-User-ID`, bad JSON, invalid phone or text |
| `402` | Insufficient balance |
| `500` | Internal error |

---

### List Messages

```
GET /api/v1/messages/:userID
```

Returns the last 50 messages for the user.

---

### Get a Single Message

```
GET /api/v1/messages/:userID/:id
```

---

### Get Balance

```
GET /api/v1/balance/:userID
```

**Response `200 OK`:**

```json
{ "user_id": "...", "amount": 990 }
```

---

### Get Transaction History

```
GET /api/v1/transactions/:userID?limit=20
```

Returns balance transactions (debits and credits). `limit` defaults to 20, max 100.

---

### Top Up Balance

```
POST /api/v1/balance/charge
```

**Request body:**

```json
{ "user_id": "...", "amount": 1000 }
```

**Response `200 OK`:**

```json
{ "user_id": "...", "new_balance": 1990 }
```

## Example cURL Workflow

```bash
USER_ID="550e8400-e29b-41d4-a716-446655440000"

# 1. Top up balance
curl -s -X POST http://localhost:8080/api/v1/balance/charge \
  -H "Content-Type: application/json" \
  -d "{\"user_id\": \"$USER_ID\", \"amount\": 1000}" | jq

# 2. Send a normal SMS
curl -s -X POST http://localhost:8080/api/v1/messages \
  -H "Content-Type: application/json" \
  -H "X-User-ID: $USER_ID" \
  -d '{"phone_number": "+14155552671", "text": "Hello!", "type": "normal"}' | jq

# 3. Send an express SMS
curl -s -X POST http://localhost:8080/api/v1/messages \
  -H "Content-Type: application/json" \
  -H "X-User-ID: $USER_ID" \
  -d '{"phone_number": "+14155552671", "text": "Urgent!", "type": "express"}' | jq

# 4. Check balance
curl -s http://localhost:8080/api/v1/balance/$USER_ID | jq

# 5. List messages
curl -s http://localhost:8080/api/v1/messages/$USER_ID | jq
```

## Pricing

Prices are stored in the `message_pricing` table and cached in memory (refreshed every 5 minutes).

| Type | Default Price (units) |
|------|-----------------------|
| `normal` | 10 |
| `express` | 25 |

## Database Schema

| Table | Purpose |
|-------|---------|
| `balances` | One row per user; `amount >= 0` enforced at the DB level |
| `messages` | All submitted messages with current status |
| `balance_transactions` | Immutable ledger of every debit and credit |
| `message_pricing` | Configurable price per message type |

## Testing

```bash
# Unit tests
make test

# Integration tests (requires a running PostgreSQL)
make test-integration
```

## Key Design Decisions

**Atomic balance deduction** — A single `UPDATE balances SET amount = amount - $1 WHERE user_id = $2 AND amount >= $1` runs in the same transaction as the message insert. This makes it impossible for the balance to go negative, even under high concurrency, without needing advisory locks or `SELECT FOR UPDATE`.

**Kafka over Asynq/Redis** — Kafka's consumer-group offset model gives durable, ordered, replayable delivery. The two topics (`sms.express`, `sms.normal`) are consumed with different concurrency levels to implement priority dispatch.

**Idempotent worker** — The handler checks `status == pending` before acting. A Kafka redelivery after a crash cannot double-send or double-deduct because the balance was deducted exactly once (in the API transaction) before the message was ever published.

**Balance reversal on permanent failure** — When the operator returns a permanent error (e.g. invalid destination), the worker reverses the deduction via `ReverseDeductTx`, which is also idempotent (guarded by a unique constraint on `(message_id, tx_type)` in `balance_transactions`).

## Known Limitations

- Mock operator only — no real telecom integration.
- No per-user rate limiting beyond queue priority weighting.
- No refund sweep for messages that crash after balance deduction but before Kafka publish (Outbox Pattern candidate fix).
