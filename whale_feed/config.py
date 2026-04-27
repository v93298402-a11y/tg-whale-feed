"""Default Telegram API credentials for the Telethon client.

These are the public Telegram Desktop credentials — safe to use without
registration. Override via API_ID / API_HASH env vars if you have your own.
"""

from __future__ import annotations

import os

# Telegram Desktop (open-source) credentials.
DEFAULT_API_ID = 2040
DEFAULT_API_HASH = "b18441a1ff607e10a989891a5462e627"


def get_api_id() -> int:
    raw = os.getenv("API_ID", "").strip()
    if raw:
        try:
            return int(raw)
        except ValueError:
            pass
    return DEFAULT_API_ID


def get_api_hash() -> str:
    return os.getenv("API_HASH", "").strip() or DEFAULT_API_HASH


def get_session_name() -> str:
    return os.getenv("SESSION_NAME", "whale_feed").strip() or "whale_feed"
