CREATE SEQUENCE IF NOT EXISTS gid;

CREATE TABLE IF NOT EXISTS rune_entry
(
    rune_id      VARCHAR(24) NOT NULL PRIMARY KEY,
    etching      CHAR(64)    NOT NULL,
    number       UINTEGER    NOT NULL,
    rune         VARCHAR(64) NOT NULL,
    spaced_rune  VARCHAR(64) NOT NULL,
    symbol       CHAR(1)     NOT NULL,
    divisibility UTINYINT    NOT NULL,
    premine      UHUGEINT    NOT NULL DEFAULT 0,
    amount       UHUGEINT,
    cap          UHUGEINT,
    start_height UINTEGER,
    end_height   UINTEGER,
    start_offset UINTEGER,
    end_offset   UINTEGER,
    timestamp    UINTEGER    NOT NULL,
    mints        UHUGEINT    NOT NULL DEFAULT 0,
    turbo        BOOLEAN     NOT NULL DEFAULT false,
    burned       UHUGEINT    NOT NULL DEFAULT 0
);

CREATE INDEX idx_rune ON rune_entry (rune);
CREATE INDEX idx_spaced_rune ON rune_entry (spaced_rune);

CREATE TABLE IF NOT EXISTS rune_balance
(
    id           UBIGINT PRIMARY KEY,
    height       UINTEGER    NOT NULL,
    index        UINTEGER    NOT NULL,
    txid         CHAR(64)    NOT NULL,
    vout         UINTEGER,
    value        UINTEGER,
    rune_id      VARCHAR(24) NOT NULL,
    rune_amount  UHUGEINT    NOT NULL,
    address      VARCHAR(64) NOT NULL,
    timestamp    UINTEGER    NOT NULL,
    op           UTINYINT    NOT NULL,
    spent_height UINTEGER    NOT NULL DEFAULT 0,
    spent_txid   CHAR(64),
    spent_vin    UINTEGER,
    UNIQUE (txid, vout, rune_id)
);

CREATE INDEX idx_address ON rune_balance (address);