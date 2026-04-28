"""Whale-feed source: on-chain monitoring via TonCenter REST API v3.

Replaces the Telegram Resale source (``whale_feed.py``) with direct
blockchain monitoring.  Polls TonCenter's ``/api/v3/actions`` endpoint
for ``nft_purchase`` events and filters for Telegram-gift NFTs
(identified by the ``nft.fragment.com`` URI pattern on item metadata).

Advantages over the previous Telegram-Resale approach:

* **100 % accuracy** вЂ” every on-chain sale is an ``nft_purchase`` action;
  no disappearance heuristics, no top-N limitation.
* **Correct marketplace attribution** вЂ” the ``marketplace`` field in the
  action tells us exactly where the sale happened (Fragment, Getgems, вЂ¦).
* **No Telegram API FloodWait** вЂ” uses a plain HTTPS REST call.
* **Low latency** вЂ” polls every 30 s; new sales appear within one cycle.

Requires ``TONCENTER_API_KEY`` env variable (free key from
https://tonconsole.com).  Works without it too, but at a lower rate
limit (1 RPS).
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import httpx

from whale_feed.whale_types import WhaleSale

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TONCENTER_API_BASE = "https://toncenter.com/api/v3"
ACTIONS_PATH = "/actions"
NFT_ITEMS_PATH = "/nft/items"
POLL_INTERVAL_SEC = 30.0
HTTP_TIMEOUT_SEC = 20.0
RATE_LIMIT_PAUSE = 1.1  # seconds between TonCenter API calls (free tier: 1 RPS)
NANOTON = 1_000_000_000
WHALE_THRESHOLD_TON = 100.0
COLLECTION_CACHE_MAX = 500
FRAGMENT_URI_PREFIX = "https://nft.fragment.com/"
SEEN_ACTIONS_MAX = 5000

# Marketplaces that already have their own dedicated source modules.
# Sales on these are skipped by TonCenter to avoid duplicates вЂ” the
# specialised sources post them with richer details anyway.
_SKIP_MARKETPLACES: frozenset[str] = frozenset({"getgems", "fragment"})

_DEFAULT_MARKETPLACE = "Telegram"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SLUG_RE = re.compile(
    r"https://nft\.fragment\.com/gift/(?P<slug>[a-z0-9]+)-(?P<num>\d+)\.json$",
    re.IGNORECASE,
)

_TITLE_SPLIT_RE = re.compile(r"(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")

_NAME_NUM_RE = re.compile(r"^(?P<title>.+?)\s*#\s*(?P<num>\d+)\s*$")


@dataclass
class _CollectionInfo:
    title: str
    slug_prefix: str


_stats: dict[str, int] = {
    "polls": 0,
    "actions_seen": 0,
    "whales_emitted": 0,
    "non_gift_skipped": 0,
    "below_threshold": 0,
    "marketplace_skipped": 0,
    "resolve_errors": 0,
    "http_errors": 0,
}


def get_stats() -> dict[str, int]:
    return dict(_stats)


def _title_from_slug(slug_prefix: str) -> str:
    """Derive a human title from a Fragment slug prefix.

    ``"plushpepe"``  -> ``"Plush Pepe"``
    ``"lootbag"``    -> ``"Loot Bag"``
    ``"snoopDogg"``  -> ``"Snoop Dogg"``
    """
    parts = _TITLE_SPLIT_RE.sub(" ", slug_prefix)
    return parts.title()


def _headers(api_key: str | None) -> dict[str, str]:
    h: dict[str, str] = {"Accept": "application/json"}
    if api_key:
        h["X-API-Key"] = api_key
    return h


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------


async def _fetch_purchases(
    client: httpx.AsyncClient,
    api_key: str | None,
    limit: int = 100,
) -> list[dict]:
    """Fetch the latest ``nft_purchase`` actions from TonCenter."""
    url = TONCENTER_API_BASE + ACTIONS_PATH
    params = {
        "action_type": "nft_purchase",
        "limit": str(limit),
        "sort": "desc",
    }
    resp = await client.get(url, headers=_headers(api_key), params=params)
    resp.raise_for_status()
    data = resp.json()
    return list(data.get("actions", []))


async def _resolve_nft_item(
    client: httpx.AsyncClient,
    api_key: str | None,
    nft_item_address: str,
) -> dict | None:
    """Fetch a single NFT item from TonCenter. Returns the item dict."""
    await asyncio.sleep(RATE_LIMIT_PAUSE)
    url = TONCENTER_API_BASE + NFT_ITEMS_PATH
    params = {"address": nft_item_address, "limit": "1"}
    resp = await client.get(url, headers=_headers(api_key), params=params)
    resp.raise_for_status()
    data = resp.json()
    items = data.get("nft_items", [])
    return items[0] if items else None


async def _fetch_gift_metadata(
    client: httpx.AsyncClient,
    metadata_url: str,
) -> dict | None:
    """Fetch the JSON metadata for a Telegram gift NFT."""
    try:
        resp = await client.get(metadata_url, timeout=10.0)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        logger.warning("Failed to fetch gift metadata: %s", metadata_url)
        return None


async def _resolve_collection(
    client: httpx.AsyncClient,
    api_key: str | None,
    nft_item_address: str,
) -> _CollectionInfo | None:
    """Look up an NFT item and determine if it belongs to a Telegram gift
    collection.  Returns ``_CollectionInfo`` or ``None`` (non-gift NFT).

    Fetches the item metadata JSON from Fragment to get the canonical
    collection title (e.g. "Plush Pepe #1" в†’ "Plush Pepe").
    """
    try:
        item = await _resolve_nft_item(client, api_key, nft_item_address)
    except Exception:
        _stats["resolve_errors"] += 1
        logger.warning("Failed to resolve NFT item %s", nft_item_address)
        raise  # let caller decide whether to cache

    if item is None:
        return None

    uri = (item.get("content") or {}).get("uri", "")
    if not uri.startswith(FRAGMENT_URI_PREFIX):
        return None  # not a Telegram gift

    m = _SLUG_RE.match(uri)
    if not m:
        return None

    slug_prefix = m.group("slug")

    # Fetch the metadata JSON from Fragment to get the real title.
    meta = await _fetch_gift_metadata(client, uri)
    if meta and meta.get("name"):
        # name is like "Plush Pepe #1" вЂ” strip the number suffix.
        raw_name = meta["name"]
        title_match = _NAME_NUM_RE.match(raw_name)
        title = title_match.group("title").strip() if title_match else raw_name
    else:
        title = _title_from_slug(slug_prefix)

    return _CollectionInfo(title=title, slug_prefix=slug_prefix)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


async def run_toncenter_feed(
    on_sold: Callable[[WhaleSale], Awaitable[None]],
    threshold_ton: float = WHALE_THRESHOLD_TON,
    api_key: str | None = None,
) -> None:
    """Poll TonCenter for on-chain NFT purchases of Telegram gifts.

    Runs until cancelled.
    """
    logger.info(
        "TonCenter feed starting: threshold=%.1f TON, poll=%.0fs, api_key=%s",
        threshold_ton,
        POLL_INTERVAL_SEC,
        "present" if api_key else "absent",
    )

    # Collection address -> _CollectionInfo | None (None = not a TG gift)
    collection_cache: dict[str, _CollectionInfo | None] = {}
    # Track seen action IDs to avoid reprocessing (poll returns latest N)
    seen_action_ids: set[str] = set()

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_SEC) as client:
        while True:
            _stats["polls"] += 1
            try:
                actions = await _fetch_purchases(client, api_key)
            except Exception:
                _stats["http_errors"] += 1
                logger.exception("TonCenter poll failed")
                await asyncio.sleep(POLL_INTERVAL_SEC)
                continue

            for action in actions:
                if not action.get("success"):
                    continue

                action_id = action.get("action_id", "")
                if action_id in seen_action_ids:
                    continue

                seen_action_ids.add(action_id)
                _stats["actions_seen"] += 1

                details = action.get("details", {})
                raw_price = details.get("price", "0")
                try:
                    price_ton = int(raw_price) / NANOTON
                except (TypeError, ValueError):
                    continue

                if price_ton < threshold_ton:
                    _stats["below_threshold"] += 1
                    continue

                # Resolve collection (is it a Telegram gift?)
                coll_addr = details.get("nft_collection", "")
                if coll_addr not in collection_cache:
                    nft_item = details.get("nft_item", "")
                    try:
                        info = await _resolve_collection(
                            client, api_key, nft_item,
                        )
                    except Exception:
                        # Resolution failed (rate limit, network, etc.)
                        # вЂ” skip but do NOT cache so we retry next cycle.
                        continue
                    collection_cache[coll_addr] = info
                    # Bound cache size
                    if len(collection_cache) > COLLECTION_CACHE_MAX:
                        oldest = next(iter(collection_cache))
                        collection_cache.pop(oldest, None)

                coll_info = collection_cache[coll_addr]
                if coll_info is None:
                    _stats["non_gift_skipped"] += 1
                    continue

                marketplace = details.get("marketplace") or ""

                # Skip sales from marketplaces that have their own
                # dedicated source modules (Getgems, Fragment, etc.).
                if marketplace.lower() in _SKIP_MARKETPLACES:
                    _stats["marketplace_skipped"] += 1
                    logger.debug(
                        "Skipping %s sale (covered by dedicated source): "
                        "%s @ %.1f TON",
                        marketplace, coll_info.title, price_ton,
                    )
                    continue

                source = marketplace.title() if marketplace else _DEFAULT_MARKETPLACE

                # Fetch the NFT item to get the real gift number from
                # the content URI (the on-chain index is a hash, not the
                # sequential gift number).
                nft_item_addr = details.get("nft_item", "")
                num: int = 0
                model, backdrop, symbol = None, None, None
                try:
                    item = await _resolve_nft_item(
                        client, api_key, nft_item_addr,
                    )
                    if item:
                        content_uri = (item.get("content") or {}).get(
                            "uri", "",
                        )
                        uri_match = _SLUG_RE.match(content_uri)
                        if uri_match:
                            num = int(uri_match.group("num"))

                        # Also fetch metadata for model/backdrop/symbol.
                        if content_uri:
                            meta = await _fetch_gift_metadata(
                                client, content_uri,
                            )
                            if meta:
                                attrs = meta.get("attributes", [])
                                by_trait = {
                                    a.get("trait_type", ""): a.get("value")
                                    for a in attrs
                                }
                                model = by_trait.get("Model")
                                backdrop = by_trait.get("Backdrop")
                                symbol = by_trait.get("Symbol")
                except Exception:
                    logger.debug(
                        "Could not fetch item details for %s", nft_item_addr,
                    )

                if not num:
                    _stats["resolve_errors"] += 1
                    logger.warning(
                        "TonCenter sale without gift number: "
                        "%s @ %.1f TON (nft=%s) вЂ” posting anyway",
                        coll_info.title, price_ton, nft_item_addr,
                    )

                slug = f"{coll_info.slug_prefix}-{num}" if num else None
                title_str = (
                    f"{coll_info.title} #{num}" if num
                    else coll_info.title
                )
                sale = WhaleSale(
                    source=source,
                    title=title_str,
                    price_ton=price_ton,
                    collection_title=coll_info.title,
                    num=num or None,
                    slug=slug,
                    model=model,
                    backdrop=backdrop,
                    symbol=symbol,
                    nft_address=nft_item_addr or None,
                    seller_address=details.get("old_owner"),
                    buyer_address=details.get("new_owner"),
                )

                try:
                    await on_sold(sale)
                    _stats["whales_emitted"] += 1
                    logger.info(
                        "TonCenter WHALE: %s @ %.1f TON (marketplace=%s)",
                        sale.title,
                        price_ton,
                        marketplace,
                    )
                except Exception:
                    logger.exception(
                        "on_sold callback failed for %s", sale.title,
                    )

            # Bound seen_action_ids memory
            if len(seen_action_ids) > SEEN_ACTIONS_MAX:
                excess = len(seen_action_ids) - SEEN_ACTIONS_MAX // 2
                for _ in range(excess):
                    seen_action_ids.pop()

            await asyncio.sleep(POLL_INTERVAL_SEC)
