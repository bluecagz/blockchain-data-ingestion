-- 0001_initial.sql
CREATE TABLE blocks (
    id SERIAL PRIMARY KEY,
    block_number BIGINT NOT NULL,
    hash TEXT NOT NULL,
    parent_hash TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    miner TEXT NOT NULL,
    difficulty TEXT NOT NULL,
    total_difficulty TEXT NOT NULL,
    gas_used BIGINT NOT NULL,
    gas_limit BIGINT NOT NULL,
    size BIGINT NOT NULL,
    receipts_root TEXT NOT NULL,
    transactions JSONB NOT NULL,
    tx_count BIGINT NOT NULL,
    UNIQUE (block_number, hash)
);

CREATE TABLE transactions (
    id SERIAL PRIMARY KEY,
    chain TEXT NOT NULL,
    block_number BIGINT NOT NULL,
    tx_hash TEXT NOT NULL,
    from_address TEXT NOT NULL,
    to_address TEXT,
    value TEXT NOT NULL,
    gas_price TEXT NOT NULL,
    gas TEXT NOT NULL,
    input TEXT NOT NULL,
    nonce BIGINT NOT NULL,
    FOREIGN KEY (block_number) REFERENCES blocks (block_number)
);
