"""Auto-fetch auth tokens for third-party marketplaces via Telegram WebView."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from urllib.parse import parse_qs, unquote, urlparse

import httpx
from telethon import functions, types

if TYPE_CHECKING:
    from telethon import TelegramClient

logger = logging.getLogger(__name__)

_PORTALS_BOT = "portals"
_PORTALS_URL = "https://portal-market.com/"

_MRKT_BOT = "mrkt"
_MRKT_AUTH_URL = "https://api.tgmrkt.io/api/v1/auth"


def _extract_init_data(url: str) -> str:
    """Extract tgWebAppData from the WebView result URL fragment."""
    parsed = urlparse(url)
    fragment = parsed.fragment
    qs = parse_qs(fragment)
    raw = qs.get("tgWebAppData", [""])[0]
    return unquote(raw) if raw else ""


async def get_portals_token(client: TelegramClient) -> str:
    """Get Portals auth token via RequestWebView."""
    try:
        bot = await client.get_input_entity(_PORTALS_BOT)
        result = await client(
            functions.messages.RequestWebViewRequest(
                peer=bot,
                bot=bot,
                platform="android",
                url=_PORTALS_URL,
            )
        )
        init_data = _extract_init_data(result.url)
        if init_data:
            token = f"tma {init_data}"
            logger.info("Portals auth token obtained (%d chars)", len(token))
            return token
        logger.warning("Failed to extract Portals init data from URL")
    except Exception:
        logger.exception("Failed to get Portals auth token")
    return ""


async def get_mrkt_token(client: TelegramClient) -> str:
    """Get MRKT auth token via RequestAppWebView + /api/v1/auth exchange."""
    try:
        bot_entity = await client.get_input_entity(_MRKT_BOT)
        bot_user = types.InputUser(user_id=bot_entity.user_id, access_hash=bot_entity.access_hash)
        bot_app = types.InputBotAppShortName(bot_id=bot_user, short_name="app")
        result = await client(
            functions.messages.RequestAppWebViewRequest(
                peer=bot_entity,
                app=bot_app,
                platform="android",
            )
        )
        init_data = _extract_init_data(result.url)
        if not init_data:
            logger.warning("Failed to extract MRKT init data from URL")
            return ""

        async with httpx.AsyncClient() as http:
            resp = await http.post(
                _MRKT_AUTH_URL,
                json={"data": init_data},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            token = data.get("token", "")
            if token:
                logger.info("MRKT auth token obtained: %s...%s", token[:8], token[-4:])
                return token
            logger.warning("MRKT auth response missing token: %s", data)
    except Exception:
        logger.exception("Failed to get MRKT auth token")
    return ""
