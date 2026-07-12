DROP INDEX IF EXISTS uq_balance_transactions_message_type;

ALTER TABLE balance_transactions DROP COLUMN IF EXISTS type;
