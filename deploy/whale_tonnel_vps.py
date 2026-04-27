"""Whale-feed source for Tonnel Marketplace gift sales.

Tonnel publishes every sale to its official Telegram channel
``@GiftNotification`` (Tonnel Marketplace Sales) — linked from
``@tonnel_network``. We subscribe to that channel via Telethon and
parse each post; sales at or above ``threshold_ton`` are forwarded to
``on_sold``.

Tonnel's post format is intentionally minimal — title, number, and
price, with NO attribute information::

    Tonnel Marketplace Sales
    Gift Sold (Internal Purchase)

    Swag Bag #4824 🎒

    Price: 5.788  💎
    Cashback Earned: 0.164 💎

    [forwarded NFT preview]
    [inline buttons: View Gift, View MarketPlace]

Tonnel also posts:

* ``Offer Accepted (Internal Purchase)`` — same shape, treated as a
  sale (the seller accepted a buyer's standing offer).
* ``Auction Finished!`` — auction-style sale, less common, treated as
  a sale when a price is parseable.
* Pinned single-line variant for whales (everything on one line).

Because attributes are missing from the message body, every sale is
enriched via :func:`whale_feed.whale_enrich.enrich_attributes` before
emission, which fetches Model / Symbol / Backdrop from Telegram via
``payments.GetUniqueStarGift``.
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from telethon import events
from telethon.errors import FloodWaitError

from sniper.whale_enrich import enrich_attributes
from sniper.whale_types import WhaleSale, derive_slug

if TYPE_CHECKING:
    from telethon import TelegramClient

logger = logging.getLogger(__name__)


GIFT_THRESHOLD_TON = 100.0
TONNEL_CHANNEL = "GiftNotification"
CATCHUP_INTERVAL_SEC = 120.0  # periodic sweep every 2 minutes
CATCHUP_LIMIT = 50  # how many recent messages to re-check each sweep
SEEN_IDS_MAX = 2000  # cap on seen message ID set to bound memory

# Any of these phrases indicates a sale we want to forward.
_SALE_ANCHORS = (
    "Gift Sold",
    "Offer Accepted",
    "Auction Finished",
)

# "Title #4824" with an optional emoji suffix. Title is letters / digits
# / spaces / hyphens / apostrophes / dots. Stops at first '#'.
# Accept typographic apostrophes (U+2019, U+2018, U+02BC) in addition
# to ASCII '\''; see whale_mrkt for full rationale.
_TITLE_RE = re.compile(
    r"([A-Za-z][A-Za-z0-9 .'’‘ʼ\-]*?)\s*#\s*(\d+)",
)
# Match either:
#   * "Price: 5.788" — used by Gift Sold / Offer Accepted posts.
#   * "Highest Bid: 202.65 TON" — used by Auction Finished posts.
# Tonnel sometimes appends a 💎 emoji after the number rather than a
# "TON" unit text, so we don't require a unit suffix. Cashback and
# similar amount lines are stripped before parsing in :func:`parse_sale_message`.
_PRICE_RE = re.compile(
    r"(?:Highest\s*Bid|Price)\s*:?\s*([\d]+(?:[.,]\d+)?)",
    re.IGNORECASE,
)


_stats: dict[str, int] = {
    "messages_seen": 0,
    "sales_parsed": 0,
    "whales_emitted": 0,
    "parse_errors": 0,
    "below_threshold": 0,
    "enrich_attempts": 0,
    "catchup_sweeps": 0,
    "catchup_recovered": 0,
}


def get_stats() -> dict[str, int]:
    return dict(_stats)


def _has_sale_anchor(text: str) -> bool:
    return any(anchor in text for anchor in _SALE_ANCHORS)


def parse_sale_message(text: str) -> WhaleSale | None:
    """Parse one Tonnel notification post into a :class:`WhaleSale`.

    Returns ``None`` if the message isn't a recognised sale post or
    is missing required fields (title or price).
    """
    if not text or not _has_sale_anchor(text):
        return None

    is_auction = "Auction Finished" in text

    # Cashback line repeats the word "Earned" — strip it from the body
    # used for price parsing so we don't accidentally pick up cashback
    # as the sale price (the regex below picks the FIRST "Price:" hit
    # but better to be defensive against future format changes).
    body = re.sub(r"Cashback Earned\s*:.*", "", text, flags=re.IGNORECASE)

    title_match = _TITLE_RE.search(body)
    if not title_match:
        return None
    title = title_match.group(1).strip()
    try:
        num = int(title_match.group(2))
    except ValueError:
        return None
    full_title = f"{title} #{num}"
    slug, collection_title, _ = derive_slug(full_title)

    price_match = _PRICE_RE.search(body)
    if not price_match:
        return None
    raw_price = price_match.group(1).replace(",", ".")
    try:
        price_ton = float(raw_price)
    except ValueError:
        return None
    if price_ton <= 0:
        return None

    return WhaleSale(
        source="Tonnel",
        title=full_title,
        price_ton=price_ton,
        collection_title=collection_title or title,
        num=num,
        slug=slug,
        # Attributes intentionally None — Tonnel posts don't include
        # them. They're filled in by enrich_attributes() before emit.
        model=None,
        symbol=None,
        backdrop=None,
        nft_address=None,
        seller_address=None,
        buyer_address=None,
        is_auction=is_auction,
    )


async def _process_tonnel_message(
    client: TelegramClient,
    text: str,
    msg_id: int,
    threshold_ton: float,
    on_sold: Callable[[WhaleSale], Awaitable[None]],
) -> bool:
    """Parse and forward one Tonnel sale message.  Returns True if emitted."""
    try:
        sale = parse_sale_message(text)
    except Exception:
        _stats["parse_errors"] += 1
        logger.exception("Tonnel: parser crashed on message %s", msg_id)
        return False
    if sale is None:
        return False
    _stats["sales_parsed"] += 1
    if sale.price_ton < threshold_ton:
        _stats["below_threshold"] += 1
        return False

    _stats["enrich_attempts"] += 1
    try:
        await enrich_attributes(client, sale)
    except Exception:
        logger.exception("Tonnel: enrich raised, posting partial sale")

    _stats["whales_emitted"] += 1
    logger.info(
        "Tonnel whale: %s @ %.2f TON (model=%s sym=%s bd=%s)",
        sale.title,
        sale.price_ton,
        sale.model,
        sale.symbol,
        sale.backdrop,
    )
    try:
        await on_sold(sale)
    except Exception:
        logger.exception("Tonnel: on_sold callback raised")
        return False
    return True


async def _catchup_loop(
    client: TelegramClient,
    entity: object,
    on_sold: Callable[[WhaleSale], Awaitable[None]],
    threshold_ton: float,
    seen_ids: set[int],
) -> None:
    """Periodic sweep: re-read recent messages and process any missed ones."""
    while True:
        await asyncio.sleep(CATCHUP_INTERVAL_SEC)
        try:
            _stats["catchup_sweeps"] += 1
            async for msg in client.iter_messages(entity, limit=CATCHUP_LIMIT):
                if msg.id in seen_ids:
                    continue
                seen_ids.add(msg.id)
                text = msg.message or ""
                if not text:
                    continue
                if not _has_sale_anchor(text):
                    continue
                emitted = await _process_tonnel_message(
                    client, text, msg.id, threshold_ton, on_sold,
                )
                if emitted:
                    _stats["catchup_recovered"] += 1
                    logger.info(
                        "Tonnel catch-up recovered message %d", msg.id,
                    )
        except FloodWaitError as e:
            logger.warning(
                "Tonnel catch-up: FloodWait %ds; skipping this sweep",
                e.seconds,
            )
            await asyncio.sleep(e.seconds)
        except Exception:
            logger.exception("Tonnel catch-up sweep failed")
        finally:
            if len(seen_ids) > SEEN_IDS_MAX:
                cutoff = sorted(seen_ids)[-SEEN_IDS_MAX // 2 :]
                seen_ids.clear()
                seen_ids.update(cutoff)


async def run_tonnel_feed(
    client: TelegramClient,
    on_sold: Callable[[WhaleSale], Awaitable[None]],
    threshold_ton: float = GIFT_THRESHOLD_TON,
    channel: str = TONNEL_CHANNEL,
) -> None:
    """Subscribe to ``@GiftNotification`` via Telethon and forward whales.

    The Telethon client must already be connected. The bound account
    must be subscribed to the channel (it's public; one-time join from
    any client is enough).
    """
    try:
        entity = await client.get_entity(channel)
    except FloodWaitError as e:
        logger.warning(
            "Tonnel: FloodWait %ds resolving @%s; aborting source.",
            e.seconds,
            channel,
        )
        return
    except Exception:
        logger.exception(
            "Tonnel: failed to resolve @%s — is the account subscribed?",
            channel,
        )
        return

    logger.info(
        "Tonnel feed starting: channel=@%s threshold=%.1f TON",
        channel,
        threshold_ton,
    )

    seen_ids: set[int] = set()

    # Seed seen_ids with recent messages so the catch-up loop doesn't
    # replay historical posts on startup.
    try:
        async for msg in client.iter_messages(entity, limit=CATCHUP_LIMIT):
            seen_ids.add(msg.id)
    except Exception:
        logger.warning("Tonnel: failed to seed seen_ids; catch-up may replay some posts")

    @client.on(events.NewMessage(chats=entity))
    async def _on_new_post(event):  # noqa: ANN001
        _stats["messages_seen"] += 1
        seen_ids.add(event.message.id)
        text = event.message.message or ""
        await _process_tonnel_message(
            client, text, event.message.id, threshold_ton, on_sold,
        )

    # Start catch-up loop in background.
    asyncio.create_task(_catchup_loop(
        client, entity, on_sold, threshold_ton, seen_ids,
    ))
    logger.info("Tonnel catch-up loop started (every %.0fs, last %d msgs)",
                CATCHUP_INTERVAL_SEC, CATCHUP_LIMIT)

    # Keep the wrapping task alive forever — see whale_mrkt for rationale.
    await asyncio.Event().wait()
