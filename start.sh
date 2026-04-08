#!/bin/bash
# Start both processes — Railway runs this as the start command
python -m backend.telegram_bot &
uvicorn backend.sms_intelligence:app --host 0.0.0.0 --port $PORT