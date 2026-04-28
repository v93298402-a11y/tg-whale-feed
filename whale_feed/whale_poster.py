"""Post whale-feed sale events to a Telegram channel via Bot API.

Supports multiple sources (Telegram Resale, Getgems, Fragment, …). All
sources hand a :class:`~whale_feed.whale_types.WhaleSale` to the poster. The
poster:

  * deduplicates across sources (same sale reported by 2+ sources gets one
    post — first one wins) using a TTL cache;
  * caches TON/USD rate from CoinGecko (refreshed every 5 min);
  * throttles outbound channel messages to ≤ 1 per ``min_interval_sec``.

Format mimics @giftwhalefeed:

    🎉 GIFT SOLD!

    🎁 <b><a href="https://t.me/nft/<slug>"><Collection> #<num></a></b>
    ├ Model: <model>
    ├ Backdrop: <backdrop>
    ├ Symbol: <symbol>
    ├ Price: 156 TON (~$200.00)
    └ Sold on <Source>

The ``<a>`` on the gift title gives Telegram a URL to expand into
the link-preview block (showing the gift image) without us needing
a separate raw URL line. The source label is plain text by default,
but for marketplaces we have referral links to (Portals, Tonnel) the
source name is also rendered as an ``<a>`` to the operator's referral
deep link, so every "Sold on Portals/Tonnel" line earns affiliate
credit when readers click through.
"""

from __future__ import annotations

import asyncio
import html as _html
import logging
import time

import httpx
from telegram.constants import ParseMode

from whale_feed.whale_types import DedupCache, WhaleSale

logger = logging.getLogger(__name__)


# Cache TON/USD rate so we don't hit CoinGecko on every post.
_USD_CACHE: dict[str, float | int] = {"price": 0.0, "ts": 0}
_USD_TTL_SEC = 300  # refresh every 5 minutes
_COINGECKO_URL = (
    "https://api.coingecko.com/api/v3/simple/price"
    "?ids=the-open-network&vs_currencies=usd"
)


async def _ton_usd_rate() -> float:
    """Return cached TON/USD rate; refresh every _USD_TTL_SEC seconds."""
    now = time.time()
    if (
        _USD_CACHE["price"]
        and now - float(_USD_CACHE["ts"]) < _USD_TTL_SEC
    ):
        return float(_USD_CACHE["price"])
    try:
        async with httpx.AsyncClient(timeout=5.0) as http:
            resp = await http.get(_COINGECKO_URL)
            resp.raise_for_status()
            data = resp.json()
            price = float(data["the-open-network"]["usd"])
    except Exception:
        logger.warning("Failed to fetch TON/USD rate; using last known")
        return float(_USD_CACHE["price"])  # may be 0.0 on first failure
    _USD_CACHE["price"] = price
    _USD_CACHE["ts"] = now
    return price


def _esc(s: str) -> str:
    """HTML-escape user-controlled text for parse_mode=HTML."""
    return _html.escape(s, quote=True)


# Per-source referral deep links. Each entry maps the WhaleSale.source
# label exactly as the source modules emit it (case-sensitive) to the
# affiliate URL we wrap the source name in. Sources not listed here
# render as plain text, matching the previous behaviour.
_SOURCE_REFERRAL_LINKS: dict[str, str] = {
    "Portals": (
        "https://t.me/portals/market"
        "?startapp=zulgx8-ref_gameCDbI-to_games"
    ),
    "Tonnel": (
        "https://t.me/tonnel_network_bot/gifts"
        "?startapp=ref_993435816"
    ),
    "MRKT": "https://t.me/mrkt/app?startapp=993435816",
}


def _source_html(source: str) -> str:
    """Render the source label, hyperlinking to a referral URL if known."""
    label = _esc(source)
    url = _SOURCE_REFERRAL_LINKS.get(source)
    if not url:
        return label
    return f'<a href="{_esc(url)}">{label}</a>'


def _fmt_ton(amount: float) -> str:
    """Format a TON amount the way marketplaces show it.

    Drops trailing zeros and the decimal point if the amount is a whole
    number (688.0 -> "688"). Keeps up to 2 decimal places otherwise
    (234.567 -> "234.57", 234.5 -> "234.5").
    """
    rounded = round(amount, 2)
    if rounded == int(rounded):
        return str(int(rounded))
    return f"{rounded:g}"


def _format_post(sale: WhaleSale, usd_rate: float) -> str:
    """Return the channel-message body for a sale (HTML parse_mode)."""
    if sale.collection_title and sale.num is not None:
        title = f"{sale.collection_title} #{sale.num}"
    else:
        title = sale.title

    link = sale.link
    title_esc = _esc(title)
    if link:
        title_html = f'<a href="{_esc(link)}">{title_esc}</a>'
    else:
        title_html = title_esc

    lines: list[str] = []
    lines.append("🎉 GIFT SOLD!")
    lines.append("")
    lines.append(f"🎁 <b>{title_html}</b>")
    if sale.model:
        lines.append(f"├ Model: {_esc(sale.model)}")
    if sale.backdrop:
        lines.append(f"├ Backdrop: {_esc(sale.backdrop)}")
    if sale.symbol:
        lines.append(f"├ Symbol: {_esc(sale.symbol)}")

    price_str = _fmt_ton(sale.price_ton)
    if usd_rate > 0:
        usd = sale.price_ton * usd_rate
        price_line = f"├ Price: {price_str} TON (~${usd:.2f})"
    else:
        price_line = f"├ Price: {price_str} TON"
    lines.append(price_line)

    source_html = _source_html(sale.source)
    if sale.is_auction:
        # Marker so readers can tell at a glance the price came from a
        # winning bid rather than a fixed-price listing.
        source_html = f"{source_html} (Auction)"
    lines.append(f"└ Sold on {source_html}")

    return "\n".join(lines)


def create_whale_poster(
    bot_token: str,
    channel: str,
    min_interval_sec: float = 1.1,
    dedup_ttl_sec: float = 1800.0,
):
    """Return an async ``post(sale)`` callback that publishes to a channel.

    Posts are throttled to at most 1 per ``min_interval_sec`` to stay within
    Telegram's bot anti-spam limits for channels.

    Cross-source dedup: if the same sale (same dedup key) was already posted
    in the last ``dedup_ttl_sec`` seconds, the duplicate is silently dropped.
    """
    from telegram import Bot

    bot = Bot(token=bot_token)
    lock = asyncio.Lock()
    state = {"last_sent": 0.0}
    dedup = DedupCache(ttl_sec=dedup_ttl_sec)

    async def post(sale: WhaleSale) -> None:
        key = sale.dedup_key

        # Fast-path outside the lock to avoid unnecessary work.
        if dedup.seen(key):
            logger.info(
                "Skipping duplicate %s sale: %s @ %.2f TON (key=%s)",
                sale.source,
                sale.title,
                sale.price_ton,
                key,
            )
            return

        usd = await _ton_usd_rate()
        text = _format_post(sale, usd)

        async with lock:
            # Re-check after acquiring the lock: another coroutine may
            # have posted the same sale while we were awaiting the USD
            # rate or waiting for the lock.
            if dedup.seen(key):
                logger.info(
                    "Skipping duplicate %s sale (caught inside lock): "
                    "%s @ %.2f TON (key=%s)",
                    sale.source,
                    sale.title,
                    sale.price_ton,
                    key,
                )
                return

            now = asyncio.get_event_loop().time()
            wait = min_interval_sec - (now - state["last_sent"])
            if wait > 0:
                await asyncio.sleep(wait)
            try:
                await bot.send_message(
                    chat_id=channel,
                    text=text,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=False,
                )
                dedup.mark(key)
                logger.info(
                    "Posted %s sale: %s @ %.2f TON",
                    sale.source,
                    sale.title,
                    sale.price_ton,
                )
            except Exception:
                logger.exception(
                    "Failed to post %s sale: %s @ %.2f TON",
                    sale.source,
                    sale.title,
                    sale.price_ton,
                )
            finally:
                state["last_sent"] = asyncio.get_event_loop().time()

    return post
