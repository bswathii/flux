"""
telegram_bot.py — Financial Companion Telegram bot
Every user-facing interaction lives here.
"""
from __future__ import annotations

from dotenv import load_dotenv
load_dotenv()


import asyncio
import calendar
import os
import re
import secrets
from datetime import datetime, time, timezone

from telegram import (
    InlineKeyboardButton, InlineKeyboardMarkup,
    KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove, Update,
)
from telegram.ext import (
    Application, CallbackQueryHandler, CommandHandler,
    ContextTypes, ConversationHandler, MessageHandler, filters,
)

from directory import db
from directory.sms_intelligence import get_snapshot, push_telegram

TELEGRAM_TOKEN   = os.environ["TELEGRAM_BOT_TOKEN"]

# ── Onboarding states ───────────────────────────────────────────────────────
ASK_INCOME, ASK_BUDGET, ASK_SAVINGS = range(3)

# ── Category emoji ──────────────────────────────────────────────────────────
_CAT_EMOJI = {
    "food": "🍱", "transport": "🚇", "shopping": "🛍",
    "utilities": "💡", "entertainment": "🎬", "health": "💊",
    "transfer": "↔️", "atm": "🏧", "emi": "🏠",
    "subscription": "📱", "other": "📦",
}

def cat_icon(cat: str | None) -> str:
    return _CAT_EMOJI.get(cat or "other", "📦")

def health_bar(pct: float) -> str:
    filled = min(10, round(pct / 10))
    return "█" * filled + "░" * (10 - filled)

def _month() -> str:
    n = datetime.now(tz=timezone.utc)
    return f"{n.year}-{n.month:02d}"

def _month_label() -> str:
    return datetime.now(tz=timezone.utc).strftime("%B")


# ── NL brain — Gemini Flash free tier + rule-based fallback ─────────────────
#
# Google Gemini Flash: 1,000,000 tokens/day FREE, no credit card.
# At 10 users × 3 queries/day ≈ 30 requests/day — uses ~0.2% of free limit.
# If GEMINI_API_KEY is not set or Gemini fails, falls back to _rule_reply()
# transparently. Users never see an error either way.
#
# Get a free key: https://aistudio.google.com/app/apikey

# NEW
from google import genai

_GEMINI_KEY   = os.environ.get("GEMINI_API_KEY", "")
_GEMINI_MODEL = "gemini-2.0-flash"
_gemini_client: genai.Client | None = None

def _get_gemini() -> "genai.Client | None":
    global _gemini_client
    if not _GEMINI_KEY:
        return None
    if _gemini_client is None:
        _gemini_client = genai.Client(api_key=_GEMINI_KEY)
    return _gemini_client


_BRAIN_SYSTEM = """\
You are a sharp, warm financial companion on Telegram. Answer directly — no preamble, no "Certainly!".

Rules:
- Under 5 lines. Direct.
- Amounts: ₹1,200 format always.
- Buy question → verdict + one number that justifies it.
- Category question → number + one-line observation.
- Unclear → acknowledge warmly, suggest one specific question they can ask.
- Never say "I'm an AI" or mention your limitations.
- Tone: smart friend who knows your finances, not a bank helpdesk.

User's financial context:
{context}"""


def _build_context(user_id: str) -> str:
    user      = db.get_user(user_id) or {}
    snap      = get_snapshot(user_id)
    budget    = user.get("monthly_budget", 20000)
    spent     = snap["spent_this_month"]
    remaining = max(0, budget - spent)
    pct       = round((spent / budget) * 100) if budget else 0

    cats     = snap.get("category_spent", {})
    top_cats = sorted(cats.items(), key=lambda x: -x[1])[:5]
    cat_str  = "\n".join(f"  {c}: ₹{a:,.0f}" for c, a in top_cats) or "  (none yet)"

    recent   = db.get_recent_txns(user_id, 5)
    rec_str  = "\n".join(
        f"  {r['merchant'] or 'Unknown'} ₹{r['amount']:,.0f} [{r['category'] or 'other'}]"
        for r in recent if r.get("amount")
    ) or "  (none yet)"

    return (
        f"Name: {user.get('first_name') or 'User'}\n"
        f"Month: {_month_label()}\n"
        f"Budget: ₹{budget:,.0f} | Spent: ₹{spent:,.0f} ({pct}%) | Left: ₹{remaining:,.0f}\n"
        f"Savings goal: ₹{user.get('savings_goal', 15000):,.0f}/mo\n"
        f"Top spending:\n{cat_str}\n"
        f"Recent transactions:\n{rec_str}"
    )


def _rule_reply(user_id: str, message: str) -> str:
    """
    Fast rule-based responder. Handles ~85% of real queries with zero cost.
    Called when Gemini is unavailable or GEMINI_API_KEY is not set.
    """
    user      = db.get_user(user_id) or {}
    snap      = get_snapshot(user_id)
    lower     = message.lower()
    budget    = user.get("monthly_budget", 20000)
    spent     = snap["spent_this_month"]
    remaining = max(0, budget - spent)
    pct       = round((spent / budget) * 100) if budget else 0
    cats      = snap.get("category_spent", {})

    # Category spend query
    for cat in ["food", "transport", "shopping", "utilities",
                "entertainment", "health", "subscription", "emi", "atm", "transfer"]:
        if cat in lower or cat.rstrip("s") in lower:
            amt = cats.get(cat, 0)
            if amt:
                share = round(amt / spent * 100) if spent else 0
                return f"{cat_icon(cat)} ₹{amt:,.0f} on {cat} this month ({share}% of spending)."
            return f"Nothing logged under {cat} yet this month."

    # Remaining / left
    if any(w in lower for w in ["left", "remaining", "headroom", "how much more"]):
        mood = "🔴" if pct >= 90 else ("🟡" if pct >= 70 else "🟢")
        return f"{mood} ₹{remaining:,.0f} left of ₹{budget:,.0f} ({pct}% used this month)."

    # Total spent
    if any(w in lower for w in ["spent", "total", "spending", "how much"]):
        return f"₹{spent:,.0f} spent this {_month_label()} out of ₹{budget:,.0f} ({pct}%)."

    # Buy decision
    price_match = re.search(r"(?:₹|rs\.?|inr)\s*([0-9,]+)", lower)
    if price_match and any(w in lower for w in ["buy", "should i", "worth", "afford", "purchase"]):
        price  = float(price_match.group(1).replace(",", ""))
        impact = round(price / remaining * 100) if remaining > 0 else 100
        if impact <= 25:
            return f"✅ Go for it — ₹{price:,.0f} is {impact}% of your ₹{remaining:,.0f} left."
        elif impact <= 60:
            return f"⚠️ Tight — ₹{price:,.0f} takes {impact}% of your ₹{remaining:,.0f} left."
        return f"❌ Skip — ₹{price:,.0f} is {impact}% of your ₹{remaining:,.0f} remaining."

    # Last transaction
    if any(w in lower for w in ["last", "latest", "recent", "what did i"]):
        txns = db.get_recent_txns(user_id, 1)
        if txns:
            t = txns[0]
            return (
                f"Last: {t.get('merchant') or 'Unknown'} "
                f"₹{t.get('amount') or 0:,.0f} [{t.get('category') or 'other'}] "
                f"on {str(t.get('ts', ''))[:10]}"
            )
        return "No transactions logged yet."

    # Savings
    if any(w in lower for w in ["saving", "save", "goal"]):
        goal   = user.get("savings_goal", 15000)
        income = user.get("monthly_income", 60000)
        track  = "✅ yes" if (income - spent) >= goal else "❌ spending too fast"
        return f"Goal: ₹{goal:,.0f}/mo · On track: {track} · Projected savings: ₹{max(0, income-spent):,.0f}"

    # Default
    mood    = "🔴 tight" if pct >= 90 else ("🟡 watch it" if pct >= 70 else "🟢 on track")
    top     = sorted(cats.items(), key=lambda x: -x[1])[:2]
    top_str = ", ".join(f"{c} ₹{a:,.0f}" for c, a in top) if top else "none yet"
    return (
        f"{mood} — ₹{spent:,.0f} of ₹{budget:,.0f} ({pct}%)\n"
        f"Top: {top_str}\n"
        f"Ask: 'how much on food?', 'should I buy X for ₹Y?', or /history"
    )


def nl_reply(user_id: str, message: str) -> str:
    model = _get_gemini()
    print(f"DEBUG: Gemini client = {model}")  # None means Gemini not working
    print(f"DEBUG: context = {_build_context(user_id)}")  # see what data exists

    if model is not None:
        try:
            context = _build_context(user_id)
            prompt = _BRAIN_SYSTEM.format(context=context) + f"\n\nUser: {message}"
            resp = model.models.generate_content(
                model=_GEMINI_MODEL,
                contents=prompt,
            )
            text = resp.text.strip()
            print(f"DEBUG: Gemini replied = {text}")
            if text:
                return text
        except Exception as e:
            print(f"DEBUG: Gemini error = {e}")  # see the actual error

    return _rule_reply(user_id, message)



# ── Onboarding ──────────────────────────────────────────────────────────────

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    uid  = str(update.effective_user.id)
    name = update.effective_user.first_name or "there"
    db.upsert_user(uid, update.effective_chat.id, name)

    user = db.get_user(uid)
    if user and user.get("onboarding_step") == "done":
        snap      = get_snapshot(uid)
        spent     = snap["spent_this_month"]
        budget    = user["monthly_budget"]
        remaining = max(0, budget - spent)
        pct       = round((spent / budget) * 100) if budget else 0
        await update.message.reply_text(
            f"👋 Welcome back, {name}!\n\n"
            f"{health_bar(pct)} {pct}% of budget used\n"
            f"₹{remaining:,.0f} left this {_month_label()}\n\n"
            "Just talk to me — ask anything about your spending.",
            reply_markup=_main_keyboard(),
        )
        return ConversationHandler.END

    await update.message.reply_text(
        f"Hey {name}! 👋 I'm your financial companion.\n\n"
        "I'll track every bank transaction automatically — zero manual logging.\n"
        "Quick setup — 3 questions.\n\n"
        "What's your monthly take-home income? (e.g. 65000)",
        reply_markup=ReplyKeyboardRemove(),
    )
    db.set_onboarding_step(uid, "ask_income")
    return ASK_INCOME


async def _onboard_income(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        income = float(re.sub(r"[^\d.]", "", update.message.text))
        if income <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("Just the number — like 65000")
        return ASK_INCOME

    ctx.user_data["income"] = income
    await update.message.reply_text(
        f"₹{income:,.0f}/mo 💰\n\n"
        "How much do you want to *allow yourself to spend* each month?\n"
        "(Savings = income minus this. e.g. 25000)",
        parse_mode="Markdown",
    )
    return ASK_BUDGET


async def _onboard_budget(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        budget = float(re.sub(r"[^\d.]", "", update.message.text))
        if budget <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("Just a number — like 25000")
        return ASK_BUDGET

    income = ctx.user_data.get("income", 60000)
    if budget > income:
        await update.message.reply_text(
            f"That's more than your income (₹{income:,.0f}). Try a lower number."
        )
        return ASK_BUDGET

    ctx.user_data["budget"] = budget
    implied = income - budget
    await update.message.reply_text(
        f"₹{budget:,.0f}/mo budget 👍\n\n"
        f"That leaves ₹{implied:,.0f} for savings.\n"
        f"What's your savings *goal*? (press send to use ₹{implied:,.0f})",
        parse_mode="Markdown",
    )
    return ASK_SAVINGS


async def _onboard_savings(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    uid    = str(update.effective_user.id)
    income = ctx.user_data.get("income", 60000)
    budget = ctx.user_data.get("budget", 20000)
    raw    = update.message.text.strip()

    try:
        savings = float(re.sub(r"[^\d.]", "", raw)) if raw else income - budget
    except ValueError:
        savings = income - budget

    db.update_profile(uid, income, budget, savings)
    name = update.effective_user.first_name or "you"
    await update.message.reply_text(
        f"All set, {name}! 🎉\n\n"
        f"  Income:       ₹{income:,.0f}/mo\n"
        f"  Budget:       ₹{budget:,.0f}/mo\n"
        f"  Savings goal: ₹{savings:,.0f}/mo\n\n"
        "Install the SMS bridge app on your Android — it'll track every bank "
        "transaction automatically.\n\n"
        "Use /pair to link your phone to this account.",
        reply_markup=_main_keyboard(),
    )
    return ConversationHandler.END


async def _onboard_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Cancelled. Run /start when ready.")
    return ConversationHandler.END


# ── /pair ────────────────────────────────────────────────────────────────────

async def cmd_pair(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Generate a 6-char code the user enters in the Android app to link it."""
    uid  = str(update.effective_user.id)
    user = db.get_user(uid)
    if not user:
        await update.message.reply_text("Run /start first.")
        return

    code = secrets.token_hex(3).upper()   # e.g. "A3F91C"
    db.create_pair_code(uid, code)

    await update.message.reply_text(
        f"Your pairing code:\n\n"
        f"`{code}`\n\n"
        "Enter this in the Financial Companion Android app.\n"
        "Code expires in 10 minutes.",
        parse_mode="Markdown",
    )


# ── Keyboard ─────────────────────────────────────────────────────────────────

def _main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("📊 Status"), KeyboardButton("📋 This month")],
            [KeyboardButton("🕐 History"), KeyboardButton("💡 Should I buy?")],
        ],
        resize_keyboard=True,
        input_field_placeholder="Ask me anything...",
    )


# ── /status ──────────────────────────────────────────────────────────────────

async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    uid  = str(update.effective_user.id)
    user = db.get_user(uid)
    if not user:
        await update.message.reply_text("Run /start first.")
        return

    snap      = get_snapshot(uid)
    budget    = user["monthly_budget"]
    spent     = snap["spent_this_month"]
    remaining = max(0, budget - spent)
    pct       = round((spent / budget) * 100) if budget else 0
    bar       = health_bar(pct)
    mood      = "🔴 Tight" if pct >= 90 else ("🟡 Watch it" if pct >= 70 else "🟢 On track")

    text = (
        f"*{_month_label()} snapshot*\n\n"
        f"{bar} {pct}%\n"
        f"{mood}\n\n"
        f"Spent:   ₹{spent:,.0f}\n"
        f"Left:    ₹{remaining:,.0f}\n"
        f"Budget:  ₹{budget:,.0f}\n"
    )

    day = datetime.now(tz=timezone.utc).day
    if day > 0:
        projected = round((spent / day) * 30)
        if projected > budget:
            text += f"\n⚡ On pace for ₹{projected:,.0f} — ₹{projected - budget:,.0f} over budget.\n"

    if snap["category_spent"]:
        text += "\n*Top categories:*\n"
        for cat, amt in sorted(snap["category_spent"].items(), key=lambda x: -x[1])[:4]:
            text += f"{cat_icon(cat)} {cat.capitalize():<12} ₹{amt:,.0f}\n"

    if snap.get("last_balance"):
        text += f"\n💳 Last known balance: ₹{snap['last_balance']:,.0f}"

    await update.message.reply_text(text, parse_mode="Markdown",
                                    reply_markup=_main_keyboard())


# ── /report ──────────────────────────────────────────────────────────────────

async def cmd_report(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    uid  = str(update.effective_user.id)
    user = db.get_user(uid)
    snap = get_snapshot(uid)
    cats = snap.get("category_spent", {})

    if not cats:
        await update.message.reply_text(
            "No transactions yet this month.\n"
            "Make sure the SMS bridge is running on your phone.",
            reply_markup=_main_keyboard(),
        )
        return

    budget = (user or {}).get("monthly_budget", 20000)
    total  = snap["spent_this_month"]
    lines  = [f"*{_month_label()} breakdown*\n"]

    for cat, amt in sorted(cats.items(), key=lambda x: -x[1]):
        pct = round((amt / total) * 100) if total else 0
        bar = "█" * max(1, pct // 10) + "░" * (10 - max(1, pct // 10))
        lines.append(f"{cat_icon(cat)} {cat.capitalize():<12} {bar} ₹{amt:,.0f} ({pct}%)")

    lines.append(f"\nTotal:  ₹{total:,.0f}  /  ₹{budget:,.0f}")
    lines.append(f"Left:   ₹{max(0, budget - total):,.0f}")

    keyboard = [
        [InlineKeyboardButton(
            f"{cat_icon(cat)} {cat.capitalize()} trend",
            callback_data=f"trend:{cat}",
        )]
        for cat, _ in sorted(cats.items(), key=lambda x: -x[1])[:4]
    ]
    await update.message.reply_text(
        "\n".join(lines),
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
    )


# ── /history ─────────────────────────────────────────────────────────────────

async def cmd_history(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    uid  = str(update.effective_user.id)
    txns = db.get_recent_txns(uid, 7)

    if not txns:
        await update.message.reply_text("No transactions logged yet.",
                                        reply_markup=_main_keyboard())
        return

    lines = [f"*Last {len(txns)} transactions*\n"]
    for t in txns:
        date  = t["ts"][:10]
        amt   = t.get("amount") or 0
        mer   = t.get("merchant") or "Unknown"
        cat   = t.get("category") or "other"
        arrow = "↓" if t["txn_type"] == "debit" else "↑"
        lines.append(
            f"{arrow} *₹{amt:,.0f}* {mer}  {cat_icon(cat)}\n"
            f"   `{date}` · `{t['id']}`"
        )

    lines.append("\nTo remove an entry: /delete `<id>`")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown",
                                    reply_markup=_main_keyboard())


# ── /delete ───────────────────────────────────────────────────────────────────

async def cmd_delete(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    uid  = str(update.effective_user.id)
    args = ctx.args
    if not args:
        await update.message.reply_text("Usage: /delete <id>\nGet IDs from /history")
        return

    ok = db.delete_txn(args[0].strip(), uid)
    if ok:
        await update.message.reply_text(f"Removed `{args[0]}`.", parse_mode="Markdown")
    else:
        await update.message.reply_text("Not found. Check /history for valid IDs.")


# ── Inline callbacks ──────────────────────────────────────────────────────────

async def on_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    uid  = str(query.from_user.id)
    data = query.data or ""

    if data.startswith("trend:"):
        cat    = data.split(":", 1)[1]
        months = db.get_category_trend(uid, cat, 3)
        if not months:
            await query.edit_message_text(f"No {cat} data yet.")
            return

        max_val = max(r["total"] for r in months)
        lines   = [f"*{cat_icon(cat)} {cat.capitalize()} — last 3 months*\n"]
        for m in months:
            bar = "█" * max(1, round((m["total"] / max_val) * 10))
            lines.append(f"{m['month']}  {bar}  ₹{m['total']:,.0f}")

        avg = sum(r["total"] for r in months) / len(months)
        lines.append(f"\nAvg: ₹{avg:,.0f}/mo")
        await query.edit_message_text("\n".join(lines), parse_mode="Markdown")


# ── Free-text handler ─────────────────────────────────────────────────────────

_SHORTCUTS = {
    "📊 status":       "give me a status update",
    "📋 this month":   "show me this month's breakdown",
    "🕐 history":      "__history__",
    "💡 should i buy?":"I want to buy something, help me decide",
}


async def handle_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    uid  = str(update.effective_user.id)
    text = update.message.text.strip()
    lower = text.lower()

    if lower in _SHORTCUTS:
        mapped = _SHORTCUTS[lower]
        if mapped == "__history__":
            await cmd_history(update, ctx)
            return
        text = mapped

    user = db.get_user(uid)
    if not user:
        await update.message.reply_text("Run /start to set up your account.")
        return

    await ctx.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")

    try:
        reply = nl_reply(uid, text)
    except Exception:
        reply = _rule_reply(uid, text)

    await update.message.reply_text(reply, parse_mode="Markdown",
                                    reply_markup=_main_keyboard())


# ── Proactive nudges ──────────────────────────────────────────────────────────

async def _weekly_nudge(context: ContextTypes.DEFAULT_TYPE) -> None:
    with db.tx() as c:
        users = c.execute(
            "SELECT user_id FROM users WHERE chat_id IS NOT NULL AND onboarding_step='done'"
        ).fetchall()

    for row in users:
        uid  = row["user_id"]
        user = db.get_user(uid)
        snap = get_snapshot(uid)
        budget    = user.get("monthly_budget", 20000)
        spent     = snap["spent_this_month"]
        pct       = round((spent / budget) * 100) if budget else 0
        remaining = max(0, budget - spent)
        name      = user.get("first_name") or "hey"

        day = datetime.now(tz=timezone.utc).day
        pace_line = ""
        if day > 0:
            projected = round((spent / day) * 30)
            if projected > budget * 1.1:
                pace_line = f"\n⚡ On pace for ₹{projected:,.0f} — ₹{projected - budget:,.0f} over."

        top_cat = max(snap["category_spent"].items(), key=lambda x: x[1]) \
                  if snap["category_spent"] else None
        cat_line = f"\nBiggest: {cat_icon(top_cat[0])} {top_cat[0]} ₹{top_cat[1]:,.0f}" \
                   if top_cat else ""

        if pct >= 80:
            opener = f"⚠️ {name}, {pct}% of budget used."
        elif pct <= 30:
            opener = f"💪 {name}, great week — only {pct}% used."
        else:
            opener = f"📊 Weekly check-in, {name}."

        text = (
            f"{opener}\n\n"
            f"Spent: ₹{spent:,.0f}  ({pct}%)\n"
            f"Left:  ₹{remaining:,.0f}"
            f"{pace_line}{cat_line}"
        )
        try:
            await push_telegram(uid, text)
        except Exception:
            pass
        await asyncio.sleep(0.05)


async def _month_end_check(context: ContextTypes.DEFAULT_TYPE) -> None:
    today    = datetime.now(tz=timezone.utc)
    last_day = calendar.monthrange(today.year, today.month)[1]
    if today.day != last_day:
        return

    with db.tx() as c:
        users = c.execute(
            "SELECT user_id FROM users WHERE chat_id IS NOT NULL AND onboarding_step='done'"
        ).fetchall()

    month = _month()
    for row in users:
        uid  = row["user_id"]
        user = db.get_user(uid)
        snap = db.get_month_snapshot(uid, month)
        budget = user.get("monthly_budget", 20000)
        income = user.get("monthly_income", 60000)
        spent  = snap["spent_this_month"]
        saved  = income - spent
        goal   = user.get("savings_goal", 15000)
        name   = user.get("first_name") or "hey"

        if saved >= goal and spent <= budget:
            mood = f"🎉 Great month, {name}!"
        elif spent <= budget:
            mood = f"👍 Under budget, {name}."
        else:
            mood = f"📉 Over budget this month, {name}."

        text = (
            f"{mood}\n\n"
            f"*{_month_label()} wrap-up*\n"
            f"Spent:  ₹{spent:,.0f} / ₹{budget:,.0f}\n"
            f"Saved:  ₹{saved:,.0f} (goal ₹{goal:,.0f})\n\n"
            "New month starts fresh tomorrow 🌅"
        )
        try:
            await push_telegram(uid, text)
        except Exception:
            pass
        await asyncio.sleep(0.05)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    db.init_db()
    application = Application.builder().token(TELEGRAM_TOKEN).build()

    onboarding = ConversationHandler(
        entry_points=[CommandHandler("start", cmd_start)],
        states={
            ASK_INCOME:  [MessageHandler(filters.TEXT & ~filters.COMMAND, _onboard_income)],
            ASK_BUDGET:  [MessageHandler(filters.TEXT & ~filters.COMMAND, _onboard_budget)],
            ASK_SAVINGS: [MessageHandler(filters.TEXT & ~filters.COMMAND, _onboard_savings)],
        },
        fallbacks=[CommandHandler("cancel", _onboard_cancel)],
        allow_reentry=True,
    )

    application.add_handler(onboarding)
    application.add_handler(CommandHandler("pair",    cmd_pair))
    application.add_handler(CommandHandler("status",  cmd_status))
    application.add_handler(CommandHandler("report",  cmd_report))
    application.add_handler(CommandHandler("history", cmd_history))
    application.add_handler(CommandHandler("delete",  cmd_delete))
    application.add_handler(CallbackQueryHandler(on_callback))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    jq = application.job_queue
    # Weekly nudge — Mondays 9am IST = 03:30 UTC
    jq.run_daily(_weekly_nudge,     time=time(3, 30),  days=(0,), name="weekly_nudge")
    # Month-end summary — checked daily at 7pm IST = 13:30 UTC
    jq.run_daily(_month_end_check,  time=time(13, 30), name="month_end")

    print("Bot polling...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()