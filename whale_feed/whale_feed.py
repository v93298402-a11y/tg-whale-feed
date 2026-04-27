"""Monitor Telegram Resale for sales >= threshold TON and post to a channel.

Architecture:

1. Periodically enumerate all gift collections (payments.GetStarGifts).
2. For each collection, fetch the top N most recently re-priced/listed
   resale offers (default sort = unixtime desc when price last changed).
3. Track listings whose price >= WHALE_THRESHOLD_TON.
4. When a tracked listing disappears from the top N for several cycles,
   call payments.GetUniqueStarGift(slug) to verify the current owner.
   If the owner address differs from the seller we recorded, the gift
   was sold — emit a "sold" event.

This produces high-precision "whale" sale signals without blockchain
monitoring, at the cost of missing sales of listings that drift out of
the recency-sorted top N before we ever observe them.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from telethon import functions, types
from telethon.errors import BadRequestError, FloodWaitError

from whale_feed.whale_types import WhaleSale

if TYPE_CHECKING:
    from telethon import TelegramClient

logger = logging.getLogger(__name__)

_NANOTON = 1_000_000_000
WHALE_THRESHOLD_TON = 100.0
COLLECTION_GAP_SEC = 5.0
CYCLE_INTERVAL_SEC = 60.0
SHALLOW_LIMIT = 100
MISSING_CYCLES_BEFORE_VERIFY = 1
OWNER_CHECK_GAP_SEC = 1.0


@dataclass
class TrackedListing:
    slug: str
    gift_id: int
    collection_title: str
    num: int
    price_ton: float
    seller_address: str | None
    seller_name: str | None
    seller_peer: object | None  # types.Peer; kept as object to avoid import cycle
    model: str | None
    backdrop: str | None
    symbol: str | None
    misses: int = 0


# slug -> TrackedListing
_tracked: dict[str, TrackedListing] = {}

_stats = {
    "cycles": 0,
    "listings_seen": 0,
    "tracked_now": 0,
    "verified_sold": 0,
    "verified_delisted": 0,
    "verify_errors": 0,
    "flood_waits": 0,
}


def get_stats() -> dict[str, int]:
    """Return a snapshot of current whale-feed counters."""
    snap = dict(_stats)
    snap["tracked_now"] = len(_tracked)
    return snap


def _extract_ton_price(gift: types.StarGiftUnique) -> float | None:
    """Return TON price for a resale listing, or None if not in TON."""
    if not gift.resell_amount:
        return None
    for amt in gift.resell_amount:
        if isinstance(amt, types.StarsTonAmount):
            return amt.amount / _NANOTON
    return None


def _extract_attribute(
    gift: types.StarGiftUnique,
    attr_type: type,
) -> str | None:
    for attr in gift.attributes:
        if isinstance(attr, attr_type):
            return attr.name
    return None


def _build_tracked(
    gift: types.StarGiftUnique,
    collection_title: str,
    price_ton: float,
) -> TrackedListing:
    return TrackedListing(
        slug=gift.slug,
        gift_id=gift.gift_id,
        collection_title=collection_title,
        num=gift.num,
        price_ton=price_ton,
        seller_address=gift.owner_address,
        seller_name=gift.owner_name,
        seller_peer=gift.owner_id,
        model=_extract_attribute(gift, types.StarGiftAttributeModel),
        backdrop=_extract_attribute(gift, types.StarGiftAttributeBackdrop),
        symbol=_extract_attribute(gift, types.StarGiftAttributePattern),
    )


async def _list_collections(client: TelegramClient) -> list[types.StarGift]:
    """Return all StarGift collections that have a resale market."""
    try:
        result = await client(functions.payments.GetStarGiftsRequest(hash=0))
    except FloodWaitError as e:
        _stats["flood_waits"] += 1
        logger.warning("FloodWait %ds on GetStarGifts; sleeping…", e.seconds)
        await asyncio.sleep(e.seconds + 1)
        return []
    except Exception:
        logger.exception("Failed to fetch gift collections")
        return []

    if isinstance(result, types.payments.StarGiftsNotModified):
        return []

    collections: list[types.StarGift] = []
    for g in result.gifts:
        if not isinstance(g, types.StarGift):
            continue
        # Include collections with any resale availability — that's where
        # whale-priced gifts live.
        if g.availability_resale and g.availability_resale > 0:
            collections.append(g)
    return collections


async def _scan_collection(
    client: TelegramClient,
    collection: types.StarGift,
    seen_this_cycle: set[str],
) -> None:
    """Fetch top SHALLOW_LIMIT listings of `collection` and update `_tracked`."""
    try:
        result = await client(
            functions.payments.GetResaleStarGiftsRequest(
                gift_id=collection.id,
                offset="",
                limit=SHALLOW_LIMIT,
                # No sort_by_price / sort_by_num: default sort is
                # unixtime (desc) of last price change → newest first.
            )
        )
    except FloodWaitError as e:
        _stats["flood_waits"] += 1
        logger.warning(
            "FloodWait %ds on collection %s; sleeping…",
            e.seconds,
            collection.title,
        )
        await asyncio.sleep(e.seconds + 1)
        return
    except BadRequestError:
        logger.exception("Bad request scanning collection %s", collection.title)
        return
    except Exception:
        logger.exception("Error scanning collection %s", collection.title)
        return

    if not hasattr(result, "gifts") or not result.gifts:
        return

    for gift in result.gifts:
        if not isinstance(gift, types.StarGiftUnique):
            continue
        _stats["listings_seen"] += 1

        price_ton = _extract_ton_price(gift)
        if price_ton is None or price_ton < WHALE_THRESHOLD_TON:
            continue

        slug = gift.slug
        seen_this_cycle.add(slug)

        existing = _tracked.get(slug)
        if existing is None:
            _tracked[slug] = _build_tracked(gift, collection.title, price_ton)
        else:
            # Listing still present — refresh price & seller in case of repricing.
            existing.price_ton = price_ton
            existing.seller_address = gift.owner_address
            existing.seller_name = gift.owner_name
            existing.seller_peer = gift.owner_id
            existing.misses = 0


async def _verify_sold(
    client: TelegramClient,
    listing: TrackedListing,
) -> tuple[bool, types.StarGiftUnique | None]:
    """Re-fetch a single gift by slug and decide whether it was sold.

    Returns (sold, current_gift). `sold` is True iff the gift's owner
    address has changed compared to what we recorded.
    """
    try:
        result = await client(
            functions.payments.GetUniqueStarGiftRequest(slug=listing.slug)
        )
    except FloodWaitError as e:
        _stats["flood_waits"] += 1
        logger.warning("FloodWait %ds verifying slug=%s", e.seconds, listing.slug)
        await asyncio.sleep(e.seconds + 1)
        return (False, None)
    except Exception:
        _stats["verify_errors"] += 1
        logger.exception("Verify failed for slug=%s", listing.slug)
        return (False, None)

    gift = getattr(result, "gift", None)
    if not isinstance(gift, types.StarGiftUnique):
        return (False, None)

    # Compare owner identity. A StarGiftUnique exposes both ``owner_id``
    # (a Telegram Peer — set when the gift is held by a TG user, which
    # is the common case) and ``owner_address`` (a TON wallet address,
    # populated only when the gift was transferred onto a wallet).
    # We must check BOTH; previously only ``owner_address`` was tracked,
    # which left ``prev=None`` for the vast majority of gifts and
    # caused every disappearance to register as "delisted" instead of
    # "sold". A change in either identity field is conclusive evidence
    # of a sale (the old owner no longer holds the gift).
    prev_peer = listing.seller_peer
    cur_peer = gift.owner_id
    prev_addr = listing.seller_address
    cur_addr = gift.owner_address
    peer_changed = (
        prev_peer is not None and cur_peer is not None and prev_peer != cur_peer
    )
    addr_changed = (
        prev_addr is not None and cur_addr is not None and prev_addr != cur_addr
    )
    # Cross-type ownership change (peer → wallet or wallet → peer) also
    # counts as a sale: the gift moved between two distinct holders.
    cross_type = (prev_peer is not None and cur_peer is None and cur_addr) or (
        prev_addr is not None and cur_addr is None and cur_peer is not None
    )
    if peer_changed or addr_changed or cross_type:
        return (True, gift)

    if not gift.resell_amount:
        # Not currently listed; if owner unchanged, the seller delisted.
        return (False, gift)
    return (False, gift)


def _to_whale_sale(listing: TrackedListing) -> WhaleSale:
    """Convert an internal Telegram-Resale tracked listing to a WhaleSale."""
    return WhaleSale(
        source="Telegram",
        title=f"{listing.collection_title} #{listing.num}",
        price_ton=listing.price_ton,
        collection_title=listing.collection_title,
        num=listing.num,
        slug=listing.slug,
        model=listing.model,
        backdrop=listing.backdrop,
        symbol=listing.symbol,
        seller_address=listing.seller_address,
    )


async def _process_disappearances(
    client: TelegramClient,
    seen_this_cycle: set[str],
    on_sold: Callable[[WhaleSale], Awaitable[None]],
) -> None:
    """For listings that didn't show up this cycle, decide sold vs delisted."""
    to_verify: list[TrackedListing] = []
    for slug, listing in list(_tracked.items()):
        if slug in seen_this_cycle:
            continue
        listing.misses += 1
        if listing.misses >= MISSING_CYCLES_BEFORE_VERIFY:
            to_verify.append(listing)

    for i, listing in enumerate(to_verify):
        if i > 0:
            await asyncio.sleep(OWNER_CHECK_GAP_SEC)
        sold, gift = await _verify_sold(client, listing)
        if sold:
            _stats["verified_sold"] += 1
            logger.info(
                "TG-Resale SOLD: %s #%d @ %.2f TON (slug=%s); "
                "owner peer %s -> %s, addr %s -> %s",
                listing.collection_title,
                listing.num,
                listing.price_ton,
                listing.slug,
                listing.seller_peer,
                gift.owner_id if gift is not None else None,
                listing.seller_address,
                gift.owner_address if gift is not None else None,
            )
            try:
                await on_sold(_to_whale_sale(listing))
            except Exception:
                logger.exception("on_sold callback failed for slug=%s", listing.slug)
        else:
            _stats["verified_delisted"] += 1
            # One-off DEBUG so we can confirm verify is reaching this branch
            # at all — if we never see "TG-Resale SOLD" entries but plenty
            # of "delisted" entries, the verify logic is wrongly classifying
            # sales as delistings (likely a Peer-equality bug).
            if _stats["verified_delisted"] <= 5 or _stats["verified_delisted"] % 50 == 0:
                logger.info(
                    "TG-Resale delisted: %s #%d @ %.2f TON (slug=%s); "
                    "owner peer %s == %s, addr %s == %s, currently_listed=%s",
                    listing.collection_title,
                    listing.num,
                    listing.price_ton,
                    listing.slug,
                    listing.seller_peer,
                    gift.owner_id if gift is not None else None,
                    listing.seller_address,
                    gift.owner_address if gift is not None else None,
                    bool(gift.resell_amount) if gift is not None else None,
                )
        _tracked.pop(listing.slug, None)


async def run_whale_feed(
    client: TelegramClient,
    on_sold: Callable[[WhaleSale], Awaitable[None]],
    threshold_ton: float = WHALE_THRESHOLD_TON,
    cycle_interval_sec: float = CYCLE_INTERVAL_SEC,
    collection_gap_sec: float = COLLECTION_GAP_SEC,
) -> None:
    """Run the whale-feed monitoring loop until cancelled."""
    global WHALE_THRESHOLD_TON
    WHALE_THRESHOLD_TON = threshold_ton

    logger.info(
        "Whale feed starting: threshold=%.1f TON, cycle=%.0fs, per-coll gap=%.1fs",
        threshold_ton,
        cycle_interval_sec,
        collection_gap_sec,
    )

    while True:
        _stats["cycles"] += 1
        cycle_start = asyncio.get_event_loop().time()

        collections = await _list_collections(client)
        if not collections:
            logger.warning("No collections returned this cycle; sleeping.")
            await asyncio.sleep(cycle_interval_sec)
            continue

        seen: set[str] = set()
        for i, coll in enumerate(collections):
            if i > 0:
                await asyncio.sleep(collection_gap_sec)
            await _scan_collection(client, coll, seen)

        await _process_disappearances(client, seen, on_sold)

        elapsed = asyncio.get_event_loop().time() - cycle_start
        sleep_for = max(1.0, cycle_interval_sec - elapsed)
        _stats["tracked_now"] = len(_tracked)
        logger.info(
            "Whale feed cycle done in %.1fs — tracking %d listings, "
            "stats=%s; sleeping %.1fs",
            elapsed,
            len(_tracked),
            _stats,
            sleep_for,
        )
        await asyncio.sleep(sleep_for)
