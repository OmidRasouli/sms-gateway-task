.PHONY: up down migrate-up migrate-down run-api run-worker tidy test test-integration

up:
	docker compose up --build

down:
	docker compose down -v

migrate-up:
	migrate -path migrations -database "postgres://sms:sms@localhost:5432/sms_gateway?sslmode=disable" up

migrate-down:
	migrate -path migrations -database "postgres://sms:sms@localhost:5432/sms_gateway?sslmode=disable" down

run-api:
	DATABASE_URL="postgres://sms:sms@localhost:5432/sms_gateway?sslmode=disable" KAFKA_BROKERS=localhost:9092 PORT=8080 go run ./cmd/api

run-worker:
	DATABASE_URL="postgres://sms:sms@localhost:5432/sms_gateway?sslmode=disable" KAFKA_BROKERS=localhost:9092 go run ./cmd/worker

tidy:
	go mod tidy

test:
	go test ./internal/...

test-integration:
	TEST_DB_DSN="postgres://sms:sms@localhost:5432/sms_gateway?sslmode=disable" \
	go test -tags integration -v -count=1 ./internal/repository/postgres/
