CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE balances (
    user_id UUID PRIMARY KEY,
    amount BIGINT NOT NULL DEFAULT 0 CHECK (amount >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE messages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    phone_number VARCHAR(20) NOT NULL,
    text TEXT NOT NULL,
    message_type VARCHAR(10) NOT NULL CHECK (message_type IN ('normal','express')),
    price BIGINT NOT NULL,
    status VARCHAR(10) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','sent','failed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at TIMESTAMPTZ
);

CREATE INDEX idx_messages_user_id_created_at ON messages (user_id, created_at DESC);

CREATE TABLE balance_transactions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    amount BIGINT NOT NULL,
    message_id UUID REFERENCES messages(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
