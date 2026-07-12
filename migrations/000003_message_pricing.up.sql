CREATE TABLE message_pricing (
    message_type TEXT PRIMARY KEY,   -- 'express' | 'normal'
    price        BIGINT NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO message_pricing (message_type, price) VALUES
    ('normal',  10),
    ('express', 25);
