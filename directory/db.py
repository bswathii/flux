"""
db.py — PostgreSQL (Railway) + SQLite (local) storage layer
Automatically uses PostgreSQL when DATABASE_URL is set, else SQLite.
"""
import hashlib
import json
import os
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional

DATABASE_URL = os.environ.get("DATABASE_URL", "")
USE_PG       = bool(DATABASE_URL)
DB_PATH      = os.environ.get("DB_PATH", "companion.db")

# ── Connection ─────────────────────────────────────────────────────────────

_db_lock = threading.Lock()
_conn    = None


def _get_conn():
    global _conn
    if _conn is not None:
        return _conn
    with _db_lock:
        if _conn is not None:
            return _conn
        if USE_PG:
            import psycopg2
            import psycopg2.extras
            _conn = psycopg2.connect(DATABASE_URL)
            _conn.autocommit = False
        else:
            import sqlite3
            _conn = sqlite3.connect(DB_PATH, check_same_thread=False)
            _conn.row_factory = sqlite3.Row
            _conn.execute("PRAGMA journal_mode=WAL")
            _conn.execute("PRAGMA foreign_keys=ON")
            _conn.execute("PRAGMA synchronous=NORMAL")
    return _conn


@contextmanager
def tx():
    conn = _get_conn()
    with _db_lock:
        try:
            if USE_PG:
                import psycopg2.extras
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    yield cur
                conn.commit()
            else:
                yield conn
                conn.commit()
        except Exception:
            conn.rollback()
            raise


def _q(sql: str) -> str:
    """Convert SQLite ? placeholders to PostgreSQL %s."""
    if USE_PG:
        return sql.replace("?", "%s")
    return sql


def _row(r) -> Optional[dict]:
    if r is None:
        return None
    if USE_PG:
        return dict(r)
    import sqlite3
    if isinstance(r, sqlite3.Row):
        return dict(r)
    return dict(r)


def _rows(rs) -> list[dict]:
    return [_row(r) for r in rs] if rs else []


# ── Init ───────────────────────────────────────────────────────────────────

def init_db() -> None:
    conn = _get_conn()
    with _db_lock:
        try:
            if USE_PG:
                import psycopg2.extras
                with conn.cursor() as c:
                    c.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        user_id         TEXT PRIMARY KEY,
                        chat_id         BIGINT,
                        first_name      TEXT,
                        monthly_income  REAL DEFAULT 60000,
                        monthly_budget  REAL DEFAULT 20000,
                        savings_goal    REAL DEFAULT 15000,
                        cat_budgets     TEXT DEFAULT '{}',
                        onboarding_step TEXT DEFAULT 'new',
                        created_at      TIMESTAMPTZ DEFAULT NOW(),
                        last_active     TIMESTAMPTZ DEFAULT NOW()
                    )""")
                    c.execute("""
                    CREATE TABLE IF NOT EXISTS transactions (
                        id          TEXT PRIMARY KEY,
                        user_id     TEXT NOT NULL,
                        ts          TEXT NOT NULL,
                        txn_type    TEXT NOT NULL,
                        amount      REAL,
                        currency    TEXT DEFAULT 'INR',
                        merchant    TEXT,
                        category    TEXT,
                        balance     REAL,
                        account     TEXT,
                        confidence  REAL,
                        note        TEXT,
                        deleted     INTEGER DEFAULT 0,
                        FOREIGN KEY(user_id) REFERENCES users(user_id)
                    )""")
                    c.execute("""
                    CREATE TABLE IF NOT EXISTS sms_seen (
                        hash        TEXT PRIMARY KEY,
                        user_id     TEXT,
                        seen_at     TIMESTAMPTZ DEFAULT NOW()
                    )""")
                    c.execute("""
                    CREATE TABLE IF NOT EXISTS pair_codes (
                        code        TEXT PRIMARY KEY,
                        user_id     TEXT NOT NULL,
                        created_at  TIMESTAMPTZ DEFAULT NOW(),
                        used        INTEGER DEFAULT 0
                    )""")
                    c.execute("CREATE INDEX IF NOT EXISTS idx_txn_user_ts    ON transactions(user_id, ts)")
                    c.execute("CREATE INDEX IF NOT EXISTS idx_txn_user_month ON transactions(user_id, substr(ts,1,7))")
                conn.commit()
            else:
                conn.executescript("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id         TEXT PRIMARY KEY,
                    chat_id         INTEGER,
                    first_name      TEXT,
                    monthly_income  REAL DEFAULT 60000,
                    monthly_budget  REAL DEFAULT 20000,
                    savings_goal    REAL DEFAULT 15000,
                    cat_budgets     TEXT DEFAULT '{}',
                    onboarding_step TEXT DEFAULT 'new',
                    created_at      TEXT DEFAULT (datetime('now')),
                    last_active     TEXT DEFAULT (datetime('now'))
                );
                CREATE TABLE IF NOT EXISTS transactions (
                    id          TEXT PRIMARY KEY,
                    user_id     TEXT NOT NULL,
                    ts          TEXT NOT NULL,
                    txn_type    TEXT NOT NULL,
                    amount      REAL,
                    currency    TEXT DEFAULT 'INR',
                    merchant    TEXT,
                    category    TEXT,
                    balance     REAL,
                    account     TEXT,
                    confidence  REAL,
                    note        TEXT,
                    deleted     INTEGER DEFAULT 0,
                    FOREIGN KEY(user_id) REFERENCES users(user_id)
                );
                CREATE TABLE IF NOT EXISTS sms_seen (
                    hash        TEXT PRIMARY KEY,
                    user_id     TEXT,
                    seen_at     TEXT DEFAULT (datetime('now'))
                );
                CREATE TABLE IF NOT EXISTS pair_codes (
                    code        TEXT PRIMARY KEY,
                    user_id     TEXT NOT NULL,
                    created_at  TEXT DEFAULT (datetime('now')),
                    used        INTEGER DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS idx_txn_user_ts    ON transactions(user_id, ts);
                CREATE INDEX IF NOT EXISTS idx_txn_user_month ON transactions(user_id, substr(ts,1,7));
                """)
        except Exception:
            conn.rollback()
            raise


# ── Users ──────────────────────────────────────────────────────────────────

def upsert_user(user_id: str, chat_id: int, first_name: str = "") -> None:
    with tx() as c:
        if USE_PG:
            c.execute(_q("""
                INSERT INTO users(user_id, chat_id, first_name)
                VALUES(%s,%s,%s)
                ON CONFLICT(user_id) DO UPDATE SET
                    chat_id=EXCLUDED.chat_id,
                    first_name=EXCLUDED.first_name,
                    last_active=NOW()
            """), (user_id, chat_id, first_name))
        else:
            c.execute(_q("""
                INSERT INTO users(user_id, chat_id, first_name)
                VALUES(?,?,?)
                ON CONFLICT(user_id) DO UPDATE SET
                    chat_id=excluded.chat_id,
                    first_name=excluded.first_name,
                    last_active=datetime('now')
            """), (user_id, chat_id, first_name))


def get_user(user_id: str) -> Optional[dict]:
    with tx() as c:
        c.execute(_q("SELECT * FROM users WHERE user_id=?"), (user_id,))
        row = c.fetchone() if USE_PG else c.execute(_q("SELECT * FROM users WHERE user_id=?"), (user_id,)).fetchone()
        return _row(row) if row else None


def update_profile(user_id: str, income: float, budget: float, savings: float,
                   cat_budgets: dict | None = None) -> None:
    with tx() as c:
        c.execute(_q("""
            UPDATE users SET
                monthly_income=?, monthly_budget=?, savings_goal=?,
                cat_budgets=?, onboarding_step='done'
            WHERE user_id=?
        """), (income, budget, savings, json.dumps(cat_budgets or {}), user_id))


def set_onboarding_step(user_id: str, step: str) -> None:
    with tx() as c:
        c.execute(_q("UPDATE users SET onboarding_step=? WHERE user_id=?"), (step, user_id))


def get_chat_id(user_id: str) -> Optional[int]:
    with tx() as c:
        c.execute(_q("SELECT chat_id FROM users WHERE user_id=?"), (user_id,))
        row = c.fetchone() if USE_PG else c.execute(_q("SELECT chat_id FROM users WHERE user_id=?"), (user_id,)).fetchone()
        if not row:
            return None
        return _row(row)["chat_id"]


# ── Device pairing ─────────────────────────────────────────────────────────

def create_pair_code(user_id: str, code: str) -> None:
    with tx() as c:
        c.execute(_q("UPDATE pair_codes SET used=1 WHERE user_id=? AND used=0"), (user_id,))
        if USE_PG:
            c.execute(_q("""
                INSERT INTO pair_codes(code, user_id) VALUES(%s,%s)
                ON CONFLICT(code) DO UPDATE SET user_id=EXCLUDED.user_id, used=0, created_at=NOW()
            """), (code.upper(), user_id))
        else:
            c.execute(_q("INSERT OR REPLACE INTO pair_codes(code, user_id) VALUES(?,?)"),
                      (code.upper(), user_id))


def claim_pair_code(code: str) -> Optional[str]:
    with tx() as c:
        c.execute(_q("SELECT user_id, created_at FROM pair_codes WHERE code=? AND used=0"), (code.upper(),))
        row = c.fetchone() if USE_PG else c.execute(_q("SELECT user_id, created_at FROM pair_codes WHERE code=? AND used=0"), (code.upper(),)).fetchone()
        if not row:
            return None
        row = _row(row)
        created = row["created_at"]
        if isinstance(created, str):
            created = datetime.fromisoformat(created).replace(tzinfo=timezone.utc)
        elif created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        if (datetime.now(tz=timezone.utc) - created).total_seconds() > 600:
            return None
        c.execute(_q("UPDATE pair_codes SET used=1 WHERE code=?"), (code.upper(),))
        return row["user_id"]


# ── Dedup ──────────────────────────────────────────────────────────────────

def is_seen(user_id: str, sender: str, body: str) -> bool:
    h = hashlib.sha256(f"{user_id}|{sender}|{body}".encode()).hexdigest()
    with tx() as c:
        c.execute(_q("SELECT 1 FROM sms_seen WHERE hash=?"), (h,))
        row = c.fetchone() if USE_PG else c.execute(_q("SELECT 1 FROM sms_seen WHERE hash=?"), (h,)).fetchone()
        if row:
            return True
        if USE_PG:
            c.execute(_q("INSERT INTO sms_seen(hash,user_id) VALUES(%s,%s) ON CONFLICT DO NOTHING"), (h, user_id))
        else:
            c.execute(_q("INSERT OR IGNORE INTO sms_seen(hash,user_id) VALUES(?,?)"), (h, user_id))
    return False


# ── Transactions ───────────────────────────────────────────────────────────

def save_txn(user_id: str, txn_id: str, ts: str, txn_type: str,
             amount: Optional[float], currency: str, merchant: Optional[str],
             category: Optional[str], balance: Optional[float],
             account: Optional[str], confidence: float) -> None:
    with tx() as c:
        if USE_PG:
            c.execute(_q("""
                INSERT INTO transactions
                (id,user_id,ts,txn_type,amount,currency,merchant,category,balance,account,confidence)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT(id) DO NOTHING
            """), (txn_id, user_id, ts, txn_type, amount, currency,
                   merchant, category, balance, account, confidence))
        else:
            c.execute(_q("""
                INSERT OR IGNORE INTO transactions
                (id,user_id,ts,txn_type,amount,currency,merchant,category,balance,account,confidence)
                VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """), (txn_id, user_id, ts, txn_type, amount, currency,
                   merchant, category, balance, account, confidence))


def get_month_snapshot(user_id: str, month: str) -> dict:
    with tx() as c:
        c.execute(_q("""
            SELECT category, SUM(amount) as total
            FROM transactions
            WHERE user_id=? AND substr(ts,1,7)=? AND txn_type='debit' AND deleted=0
            GROUP BY category
        """), (user_id, month))
        rows = c.fetchall() if USE_PG else c.execute(_q("""
            SELECT category, SUM(amount) as total
            FROM transactions
            WHERE user_id=? AND substr(ts,1,7)=? AND txn_type='debit' AND deleted=0
            GROUP BY category
        """), (user_id, month)).fetchall()

        cat_spent: dict[str, float] = {}
        total = 0.0
        for r in _rows(rows):
            amt = r["total"] or 0.0
            total += amt
            cat = r["category"] or "other"
            cat_spent[cat] = cat_spent.get(cat, 0.0) + amt

        c.execute(_q("""
            SELECT COUNT(*) as n FROM transactions
            WHERE user_id=? AND substr(ts,1,7)=? AND deleted=0
        """), (user_id, month))
        count_row = c.fetchone() if USE_PG else c.execute(_q("""
            SELECT COUNT(*) as n FROM transactions
            WHERE user_id=? AND substr(ts,1,7)=? AND deleted=0
        """), (user_id, month)).fetchone()
        count = _row(count_row)["n"] if count_row else 0

        c.execute(_q("""
            SELECT balance FROM transactions
            WHERE user_id=? AND balance IS NOT NULL
            ORDER BY ts DESC LIMIT 1
        """), (user_id,))
        last_bal = c.fetchone() if USE_PG else c.execute(_q("""
            SELECT balance FROM transactions
            WHERE user_id=? AND balance IS NOT NULL
            ORDER BY ts DESC LIMIT 1
        """), (user_id,)).fetchone()

        return {
            "spent_this_month": total,
            "category_spent":   cat_spent,
            "txn_count":        count,
            "last_balance":     _row(last_bal)["balance"] if last_bal else None,
        }


def get_recent_txns(user_id: str, limit: int = 7) -> list[dict]:
    with tx() as c:
        c.execute(_q("""
            SELECT id,ts,txn_type,amount,merchant,category,account
            FROM transactions
            WHERE user_id=? AND deleted=0
            ORDER BY ts DESC LIMIT ?
        """), (user_id, limit))
        rows = c.fetchall() if USE_PG else c.execute(_q("""
            SELECT id,ts,txn_type,amount,merchant,category,account
            FROM transactions
            WHERE user_id=? AND deleted=0
            ORDER BY ts DESC LIMIT ?
        """), (user_id, limit)).fetchall()
        return _rows(rows)


def delete_txn(txn_id: str, user_id: str) -> bool:
    with tx() as c:
        c.execute(_q("""
            UPDATE transactions SET deleted=1
            WHERE id=? AND user_id=?
        """), (txn_id, user_id))
        return (c.rowcount or 0) > 0


def get_category_trend(user_id: str, category: str, months: int = 3) -> list[dict]:
    with tx() as c:
        c.execute(_q("""
            SELECT substr(ts,1,7) as month, SUM(amount) as total
            FROM transactions
            WHERE user_id=? AND category=? AND txn_type='debit' AND deleted=0
            GROUP BY month ORDER BY month DESC LIMIT ?
        """), (user_id, category, months))
        rows = c.fetchall() if USE_PG else c.execute(_q("""
            SELECT substr(ts,1,7) as month, SUM(amount) as total
            FROM transactions
            WHERE user_id=? AND category=? AND txn_type='debit' AND deleted=0
            GROUP BY month ORDER BY month DESC LIMIT ?
        """), (user_id, category, months)).fetchall()
        return _rows(rows)