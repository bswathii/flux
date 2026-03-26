"""
sms_intelligence.py — SMS ingest + extraction + device pairing
POST /ingest/sms   ← Android SmsReceiver (HMAC-signed)
POST /pair         ← Android app claims pairing code
GET  /health
"""
from __future__ import annotations
from dotenv import load_dotenv
load_dotenv()
import hashlib
import hmac
import os
import re
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx
from fastapi import BackgroundTasks, FastAPI, HTTPException, Header, Request
from pydantic import BaseModel

import db
from sms_parser import parse as parse_sms

# ── Config ─────────────────────────────────────────────────────────────────

SMS_API_SECRET   = os.environ["SMS_API_SECRET"]
TELEGRAM_TOKEN   = os.environ["TELEGRAM_BOT_TOKEN"]


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    yield


app = FastAPI(title="Financial Companion", lifespan=lifespan)

# ── Rate limiting ───────────────────────────────────────────────────────────

_rate: dict[str, list[float]] = defaultdict(list)


def _allow(user_id: str, limit: int = 60) -> bool:
    now = time.time()
    _rate[user_id] = [t for t in _rate[user_id] if now - t < 3600]
    if len(_rate[user_id]) >= limit:
        return False
    _rate[user_id].append(now)
    return True


# ── OTP patterns ────────────────────────────────────────────────────────────

_OTP_RE = [
    re.compile(r"(?i)\b(otp|one.?time.?pass|verification.?code|auth.?code)\b"),
    re.compile(r"(?i)\b\d{4,8}\s+is\s+(your|the)\s+(otp|code|pin)\b"),
    re.compile(r"(?i)do not share.*\b\d{4,8}\b"),
    re.compile(r"(?i)\b\d{4,8}\b.{0,30}valid for \d+\s*min"),
]


def _is_otp(body: str) -> bool:
    return any(p.search(body) for p in _OTP_RE)


# SMS extraction handled by sms_parser.py — zero cost, no LLM


# ── Telegram push ───────────────────────────────────────────────────────────

async def push_telegram(user_id: str, text: str, parse_mode: str = "Markdown") -> None:
    chat_id = db.get_chat_id(user_id)
    if not chat_id:
        return
    async with httpx.AsyncClient(timeout=5) as client:
        await client.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": parse_mode},
        )


def _txn_id(user_id: str, ts_ms: int, amount) -> str:
    return hashlib.sha256(f"{user_id}{ts_ms}{amount}".encode()).hexdigest()[:16]


def _month() -> str:
    n = datetime.now(tz=timezone.utc)
    return f"{n.year}-{n.month:02d}"


async def _process_and_notify(user_id: str, sender: str, body: str, ts_ms: int) -> None:
    parsed = parse_sms(sender, body)
    if not parsed.is_financial or parsed.txn_type not in ("debit", "credit"):
        return

    ts     = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()
    txn_id = _txn_id(user_id, ts_ms, parsed.amount)

    db.save_txn(
        user_id=user_id, txn_id=txn_id, ts=ts,
        txn_type=parsed.txn_type,
        amount=parsed.amount, currency=parsed.currency,
        merchant=parsed.merchant, category=parsed.category,
        balance=parsed.balance, account=parsed.account_last4,
        confidence=1.0,
    )

    txn_type = parsed.txn_type
    amount   = parsed.amount
    merchant = parsed.merchant
    category = parsed.category
    balance  = parsed.balance

    snap   = db.get_month_snapshot(user_id, _month())
    user   = db.get_user(user_id)
    budget = (user or {}).get("monthly_budget", 20000)
    spent  = snap["spent_this_month"]
    pct    = round((spent / budget) * 100) if budget else 0

    if txn_type == "debit" and amount:
        cat_tag  = f"  #{category}" if category else ""
        mer_tag  = f" · {merchant}" if merchant else ""
        bal_line = f"\n💳 Bal ₹{balance:,.0f}" if balance else ""
        pulse    = "🔴" if pct >= 90 else ("🟡" if pct >= 70 else "🟢")
        text = (
            f"↓ *₹{amount:,.0f}*{mer_tag}{cat_tag}\n"
            f"{pulse} ₹{spent:,.0f} of ₹{budget:,.0f} spent  ({pct}%)"
            f"{bal_line}"
        )
        await push_telegram(user_id, text)

    elif txn_type == "credit" and amount:
        text = f"↑ *₹{amount:,.0f}* received"
        if merchant:
            text += f" from {merchant}"
        await push_telegram(user_id, text)


# ── Request models ──────────────────────────────────────────────────────────

class SmsPayload(BaseModel):
    user_id:   str
    sender:    str
    body:      str
    timestamp: int


class PairRequest(BaseModel):
    code:       str   # 6-char code the user sees in Telegram
    device_id:  str   # stable Android device identifier


# ── Endpoints ───────────────────────────────────────────────────────────────

@app.post("/ingest/sms")
async def ingest_sms(
    request:          Request,
    background_tasks: BackgroundTasks,
    x_signature:      str = Header(...),
    x_user_id:        str = Header(...),
):
    raw = await request.body()

    expected = hmac.HMAC(SMS_API_SECRET.encode(), raw, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, x_signature):
        raise HTTPException(401, "Invalid signature")

    payload = SmsPayload.model_validate_json(raw)
    if payload.user_id != x_user_id:
        raise HTTPException(400, "user_id mismatch")

    if not _allow(payload.user_id):
        raise HTTPException(429, "Rate limit exceeded")

    if _is_otp(payload.body):
        return {"status": "otp_filtered"}

    if db.is_seen(payload.user_id, payload.sender, payload.body):
        return {"status": "duplicate"}

    background_tasks.add_task(
        _process_and_notify,
        payload.user_id, payload.sender, payload.body, payload.timestamp,
    )
    return {"status": "queued"}


@app.post("/pair")
async def pair_device(req: PairRequest):
    """
    Android app POSTs the 6-char code from Telegram + its device_id.
    Returns user_id + a device-specific api_secret on success.
    The api_secret is used to sign future /ingest/sms requests.
    """
    user_id = db.claim_pair_code(req.code)
    if not user_id:
        raise HTTPException(400, "Invalid or expired code. Run /pair in Telegram to get a new one.")

    # Issue a device-specific secret — derived from master secret + user_id + device_id
    # This means we can revoke a single device without affecting others
    device_secret = hmac.HMAC(
        SMS_API_SECRET.encode(),
        f"{user_id}:{req.device_id}".encode(),
        hashlib.sha256,
    ).hexdigest()

    return {
        "user_id":    user_id,
        "api_secret": device_secret,
        "status":     "paired",
    }


@app.get("/health")
def health():
    return {"status": "ok", "ts": int(time.time())}


# ── Snapshot accessor (used by telegram_bot.py) ─────────────────────────────

def get_snapshot(user_id: str) -> dict:
    return db.get_month_snapshot(user_id, _month())