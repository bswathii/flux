# Financial Companion

Zero-effort expense tracking via SMS. Every bank transaction auto-logged.
Ask anything about your spending in plain English.

## Stack
- FastAPI backend (SMS ingest, device pairing)
- python-telegram-bot (Telegram interface)
- SQLite/WAL (persistent storage)
- Google Gemini Flash free tier (NL queries — 1M tokens/day free)
- Pure regex SMS parser (zero LLM cost for extraction)
- Android SMS bridge (Kotlin, WorkManager)

## Cost
$0/month for up to ~500 users.

## Setup

```bash
cp .env.example .env        # fill in 3 values
pip install -r requirements.txt
python -m backend.telegram_bot &
uvicorn backend.sms_intelligence:app --reload
```

## Deploy
Push to GitHub → connect to Railway → add env vars → done.
See launch checklist in conversation.

## Project structure
```
financial_companion/
├── backend/
│   ├── __init__.py
│   ├── db.py                 # SQLite layer
│   ├── sms_parser.py         # Zero-cost regex SMS extraction
│   ├── sms_intelligence.py   # FastAPI: /ingest/sms /pair /health
│   ├── telegram_bot.py       # Full bot UX
│   └── alternatives_engine.py
├── android_app/
│   └── SmsReceiver.kt        # Android SMS bridge
├── requirements.txt
├── Procfile                  # Railway process config
├── railway.json
├── .env.example
└── PRIVACY.md
```