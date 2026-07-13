ALTER TABLE balance_transactions
    DROP CONSTRAINT IF EXISTS balance_transactions_type_check;

ALTER TABLE balance_transactions
    ADD CONSTRAINT balance_transactions_type_check
    CHECK (type IN ('deduct', 'reverse'));
