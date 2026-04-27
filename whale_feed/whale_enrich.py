"""Backfill missing model / symbol / backdrop on a :class:`WhaleSale`.

Some whale-feed sources (notably the Tonnel marketplace channel
``@GiftNotification``) post just the title, number, and price — they
do NOT include attribute information in the message body. Posting such
a sale verbatim would yield a channel post missing Model / Symbol /
Backdrop, which the user has explicitly rejected.

This module looks up the gift's current attributes via Telethon's
``payments.GetUniqueStarGift`` call, the same primitive the
Telegram-Resale source uses. The lookup is fast (one round-trip) and
works for every TG-native gift, regardless of which marketplace the
sale happened on.

Failures are non-fatal: if the gift cannot be resolved (bad slug,
FloodWait, etc.) the sale is forwarded as-is with whatever attributes
were already populated.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from telethon import functions, types
from telethon.errors import FloodWaitError

from whale_feed.whale_types import WhaleSale

if TYPE_CHECKING:
    from telethon import TelegramClient

logger = logging.getLogger(__name__)


def _extract_attribute(
    gift: types.StarGiftUnique,
    attr_type: type,
) -> str | None:
    for attr in gift.attributes:
        if isinstance(attr, attr_type):
            return attr.name
    return None


async def enrich_attributes(
    client: TelegramClient,
    sale: WhaleSale,
    timeout_sec: float = 10.0,
) -> WhaleSale:
    """Fill in missing model/symbol/backdrop from Telegram, in place.

    Returns ``sale`` (mutated). If all three attributes are already
    populated this function is a no-op. If the slug is missing the
    function logs and returns the sale unchanged — there's nothing to
    look up against.
    """
    if sale.model and sale.symbol and sale.backdrop:
        return sale
    if not sale.slug:
        return sale

    try:
        result = await asyncio.wait_for(
            client(functions.payments.GetUniqueStarGiftRequest(slug=sale.slug)),
            timeout=timeout_sec,
        )
    except FloodWaitError as e:
        logger.warning(
            "enrich: FloodWait %ds resolving %s — leaving attrs blank.",
            e.seconds,
            sale.slug,
        )
        return sale
    except asyncio.TimeoutError:
        logger.warning("enrich: timeout resolving %s", sale.slug)
        return sale
    except Exception:
        logger.exception("enrich: failed to resolve %s", sale.slug)
        return sale

    gift = getattr(result, "gift", None)
    if not isinstance(gift, types.StarGiftUnique):
        logger.debug(
            "enrich: %s resolved but not a StarGiftUnique (got %s)",
            sale.slug,
            type(gift).__name__,
        )
        return sale

    if not sale.model:
        sale.model = _extract_attribute(gift, types.StarGiftAttributeModel)
    if not sale.backdrop:
        sale.backdrop = _extract_attribute(gift, types.StarGiftAttributeBackdrop)
    if not sale.symbol:
        sale.symbol = _extract_attribute(gift, types.StarGiftAttributePattern)

    return sale
