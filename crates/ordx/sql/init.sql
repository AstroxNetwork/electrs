CREATE TABLE IF NOT EXISTS rune_entry
(
    rune_id      TEXT    NOT NULL PRIMARY KEY,
    etching      TEXT    NOT NULL,
    number       INTEGER NOT NULL,
    rune         TEXT    NOT NULL,
    spaced_rune  TEXT    NOT NULL,
    symbol       TEXT,
    divisibility INTEGER NOT NULL,
    premine      TEXT    NOT NULL DEFAULT '0',
    amount       TEXT,
    cap          TEXT,
    start_height INTEGER,
    end_height   INTEGER,
    start_offset INTEGER,
    end_offset   INTEGER,
    turbo        BOOLEAN NOT NULL DEFAULT false,
    fairmint     BOOLEAN NOT NULL DEFAULT false,
    height       INTEGER NOT NULL,
    ts           INTEGER NOT NULL,
    mints        TEXT    NOT NULL DEFAULT '0',
    burned       TEXT    NOT NULL DEFAULT '0',
    mintable     BOOLEAN NOT NULL DEFAULT false,
    holders      INTEGER NOT NULL DEFAULT 0,
    transactions INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_rune ON rune_entry (rune);
CREATE INDEX IF NOT EXISTS idx_spaced_rune ON rune_entry (spaced_rune);
CREATE INDEX IF NOT EXISTS idx_etching ON rune_entry (etching);
CREATE INDEX IF NOT EXISTS idx_fairmint ON rune_entry (fairmint);

CREATE TABLE IF NOT EXISTS rune_balance
(
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    txid         TEXT    NOT NULL,
    vout         INTEGER NOT NULL,
    value        INTEGER NOT NULL,
    rune_id      TEXT    NOT NULL,
    rune_amount  TEXT    NOT NULL,
    address      TEXT    NOT NULL,
    premine      BOOLEAN NOT NULL DEFAULT false,
    mint         BOOLEAN NOT NULL DEFAULT false,
    burn         BOOLEAN NOT NULL DEFAULT false,
    cenotaph     BOOLEAN NOT NULL DEFAULT false,
    transfer     BOOLEAN NOT NULL DEFAULT false,
    height       INTEGER NOT NULL,
    idx          INTEGER NOT NULL,
    ts           INTEGER NOT NULL,
    spent_height INTEGER NOT NULL DEFAULT 0,
    spent_txid   TEXT,
    spent_vin    INTEGER,
    spent_ts     INTEGER
);

CREATE INDEX IF NOT EXISTS idx_address ON rune_balance (address);
CREATE INDEX IF NOT EXISTS idx_spent_height ON rune_balance (spent_height);
CREATE INDEX IF NOT EXISTS idx_spent_txid ON rune_balance (spent_txid);
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_txid_vout_rune_id ON rune_balance (txid, vout, rune_id);