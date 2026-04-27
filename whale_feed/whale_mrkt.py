"""Whale-feed source for MRKT (tgmrkt.io) Telegram-gift sales.

This source is a *Telethon channel scraper*.

We initially scraped MRKT's own ``@mrktnotification`` channel, but in
practice that channel turned out to be unreliable — it dropped many
real sales (e.g. Vintage Cigar #9740 sold on MRKT for 158 TON appeared
in the community aggregator ``@giftwhalefeed`` but never in
``@mrktnotification``) and occasionally reposted week-old sales,
causing them to be re-emitted. We do not have a clean signal for
filtering those replays.

The community aggregator ``@giftwhalefeed`` (12k+ subscribers, see
https://t.me/giftwhalefeed) operates a much more reliable scraper:
every whale sale across MRKT, Tonnel, Telegram Resale, Getgems,
Portals, Fragment is posted within seconds. Their post format is
stable and includes Model / Backdrop / Symbol / Price / source tag.

We listen to ``@giftwhalefeed`` via Telethon and emit only the posts
tagged ``Sold on MRKT``. Other source tags are ignored — those
marketplaces have their own working sources in this project.

Post format::

    🎉 GIFT SOLD!

    🟫 Vintage Cigar #9740
    ├ Model: Short Fuse
    ├ Backdrop: Neon Blue
    ├ Symbol: Candle
    ├ Price: 158.0 TON (~$206.98)
    └ Sold on MRKT

If ``@giftwhalefeed`` ever degrades, set the env var
``WHALE_MRKT_CHANNEL`` to point at a different channel (e.g. back to
``mrktnotification``) — the parser is forgiving enough to handle both
formats: it anchors on ``GIFT SOLD`` / ``Gift Sold`` and the
``Sold on MRKT`` filter is a no-op for the MRKT-only channel.
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from telethon import events
from telethon.errors import FloodWaitError

from whale_feed.whale_enrich import enrich_attributes
from whale_feed.whale_types import WhaleSale, derive_slug

if TYPE_CHECKING:
    from telethon import TelegramClient

logger = logging.getLogger(__name__)


GIFT_THRESHOLD_TON = 100.0
MRKT_CHANNEL = "giftwhalefeed"

# Either of the two anchors works:
#   * "GIFT SOLD!"  — @giftwhalefeed format (uppercase)
#   * "Gift Sold"   — @mrktnotification format (titlecase)
_ANCHOR_RE = re.compile(r"GIFT\s+SOLD", re.IGNORECASE)
# Source tag identifying which marketplace executed the sale. We only
# emit when this matches MRKT. Tolerate "Sold on MRKT" / "Sold on Mrkt".
_SOURCE_TAG_RE = re.compile(r"Sold\s+on\s+MRKT\b", re.IGNORECASE)

# "Title #12345" — accepts letters, digits, spaces, hyphens, apostrophes,
# dots in the title. Matches first occurrence after the anchor.
# Accept typographic apostrophes (U+2019, U+2018, U+02BC) in addition
# to ASCII '\''; without them "Durov’s Cap" parses as just "s Cap".
_TITLE_RE = re.compile(
    r"([A-Za-z][A-Za-z0-9 .'’‘ʼ\-]*?)\s*#\s*(\d+)",
)
# "Price: 158.0 TON (~$206.98)" / "Price: 142.97 TON" / "😋Price:142.97 TON".
# Tolerates missing/extra whitespace and stray emoji or pipes preceding
# "Price". Optional USD parenthetical is ignored.
_PRICE_RE = re.compile(
    r"Price\s*:?\s*([\d]+(?:[.,]\d+)?)\s*TON",
    re.IGNORECASE,
)
# Each attribute may be prefixed by "├" / "└" / "-" / "•" depending on
# whose channel the post came from. The body capture stops at the next
# attribute keyword, the next box-drawing prefix, or end of line.
_ATTR_BOUNDARY = (
    r"(?=\s*(?:[├└\-]\s*(?:Model|Symbol|Backdrop|Price|Sold)|"
    r"Price|Sold\s+on|😋|\n|$))"
)
_MODEL_RE = re.compile(
    rf"Model\s*:\s*(.+?){_ATTR_BOUNDARY}",
    re.IGNORECASE | re.DOTALL,
)
_SYMBOL_RE = re.compile(
    rf"Symbol\s*:\s*(.+?){_ATTR_BOUNDARY}",
    re.IGNORECASE | re.DOTALL,
)
_BACKDROP_RE = re.compile(
    rf"Backdrop\s*:\s*(.+?){_ATTR_BOUNDARY}",
    re.IGNORECASE | re.DOTALL,
)
_RARITY_PAREN_RE = re.compile(r"\s*\([^)]*\)\s*$")


_stats: dict[str, int] = {
    "messages_seen": 0,
    "sales_parsed": 0,
    "whales_emitted": 0,
    "parse_errors": 0,
    "below_threshold": 0,
    "wrong_source": 0,
}


def get_stats() -> dict[str, int]:
    return dict(_stats)


def _clean_attr(s: str | None) -> str | None:
    """Tidy an attribute capture: drop trailing rarity, dashes, emoji."""
    if s is None:
        return None
    s = s.strip()
    s = _RARITY_PAREN_RE.sub("", s)
    s = s.strip(" -·.,;:├└|")
    return s or None


def parse_sale_message(text: str) -> WhaleSale | None:
    """Parse one ``@giftwhalefeed`` post tagged ``Sold on MRKT``.

    Returns ``None`` if the message is not a "GIFT SOLD" post or if
    its source tag is anything other than MRKT. Other markets are
    handled by their own modules in this project, so we deliberately
    drop their posts here even though they appear in the same channel.
    """
    if not text:
        return None
    anchor_match = _ANCHOR_RE.search(text)
    if anchor_match is None:
        return None
    if _SOURCE_TAG_RE.search(text) is None:
        return None

    # Trim everything before the anchor so a stray "#12345" earlier in
    # the message can't fool _TITLE_RE.
    body = text[anchor_match.end():]

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

    model_match = _MODEL_RE.search(body)
    symbol_match = _SYMBOL_RE.search(body)
    backdrop_match = _BACKDROP_RE.search(body)

    return WhaleSale(
        source="MRKT",
        title=full_title,
        price_ton=price_ton,
        collection_title=collection_title or title,
        num=num,
        slug=slug,
        model=_clean_attr(model_match.group(1)) if model_match else None,
        symbol=_clean_attr(symbol_match.group(1)) if symbol_match else None,
        backdrop=_clean_attr(backdrop_match.group(1)) if backdrop_match else None,
        nft_address=None,
        seller_address=None,
        buyer_address=None,
    )


async def run_mrkt_feed(
    client: TelegramClient,
    on_sold: Callable[[WhaleSale], Awaitable[None]],
    threshold_ton: float = GIFT_THRESHOLD_TON,
    channel: str = MRKT_CHANNEL,
) -> None:
    """Subscribe to ``@giftwhalefeed`` via Telethon and forward MRKT sales.

    Runs forever. Resolves the channel entity once on startup, then
    registers a NewMessage handler scoped to that channel. The handler
    parses each post; only posts tagged ``Sold on MRKT`` and at or
    above ``threshold_ton`` are forwarded to ``on_sold``.

    The Telethon client must already be connected and authorised
    (i.e. the same client used by the rest of whale-feed). If the
    bound account isn't a member of the channel, this function logs
    a warning and returns — the channel is public, so a one-time
    join from any Telegram client is sufficient.
    """
    try:
        entity = await client.get_entity(channel)
    except FloodWaitError as e:
        logger.warning(
            "MRKT: FloodWait %ds resolving @%s; aborting source.",
            e.seconds,
            channel,
        )
        return
    except Exception:
        logger.exception(
            "MRKT: failed to resolve @%s — is the account subscribed?",
            channel,
        )
        return

    logger.info(
        "MRKT feed starting: channel=@%s threshold=%.1f TON (filter: 'Sold on MRKT')",
        channel,
        threshold_ton,
    )

    @client.on(events.NewMessage(chats=entity))
    async def _on_new_post(event):  # noqa: ANN001
        _stats["messages_seen"] += 1
        text = event.message.message or ""

        # Cheap pre-filter: skip posts that aren't tagged for MRKT
        # without running the full parser. The aggregator channel
        # mixes posts from several marketplaces.
        if _SOURCE_TAG_RE.search(text) is None:
            _stats["wrong_source"] += 1
            return

        try:
            sale = parse_sale_message(text)
        except Exception:
            _stats["parse_errors"] += 1
            logger.exception("MRKT: parser crashed on message %s", event.id)
            return
        if sale is None:
            return
        _stats["sales_parsed"] += 1
        if sale.price_ton < threshold_ton:
            _stats["below_threshold"] += 1
            return

        # Backfill any missing attributes — @giftwhalefeed already
        # carries Model/Symbol/Backdrop in their posts, but enrich is
        # cheap insurance against format drift or omitted Symbol.
        if not (sale.model and sale.symbol and sale.backdrop):
            try:
                await enrich_attributes(client, sale)
            except Exception:
                logger.exception("MRKT: enrich raised, posting partial sale")

        _stats["whales_emitted"] += 1
        logger.info(
            "MRKT whale: %s @ %.2f TON (model=%s sym=%s bd=%s)",
            sale.title,
            sale.price_ton,
            sale.model,
            sale.symbol,
            sale.backdrop,
        )
        try:
            await on_sold(sale)
        except Exception:
            logger.exception("MRKT: on_sold callback raised")

    # Keep the wrapping task alive forever — the registered handler
    # runs on the client's main loop regardless of this coroutine's
    # state. Using an Event that's never set is cleaner than calling
    # run_until_disconnected from each source independently.
    await asyncio.Event().wait()
