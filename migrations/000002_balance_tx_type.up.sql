ALTER TABLE balance_transactions
    ADD COLUMN type TEXT NOT NULL DEFAULT 'deduct' CHECK (type IN ('deduct', 'reverse'));

-- Remove the default after backfilling so new rows must supply an explicit value.
ALTER TABLE balance_transactions
    ALTER COLUMN type DROP DEFAULT;

-- Idempotency constraint: one deduction and one reversal per message at most.
-- message_id can be NULL (e.g. top-up rows), so the constraint only applies
-- when message_id IS NOT NULL — partial unique index handles this correctly.
CREATE UNIQUE INDEX uq_balance_transactions_message_type
    ON balance_transactions (message_id, type)
    WHERE message_id IS NOT NULL;
