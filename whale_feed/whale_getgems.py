"""Whale-feed source for Getgems Telegram-gift sales.

Polls the Getgems public REST API for the *Telegram Gifts* category and
forwards every confirmed sale at or above ``threshold_ton`` to the supplied
``on_sold`` callback.

API reference (read-only):
    https://github.com/getgems-io/nft-contracts/blob/main/docs/read-api-en.md

Rate limit: 400 requests / 5 min per IP. We poll once a minute (~5/5min),
well below the limit.

Authentication: the caller must provide an API key obtained at
https://getgems.io/public-api (TON Connect). The key is passed as the raw
``Authorization`` header value (no `Bearer` prefix).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

import httpx

from whale_feed.whale_types import WhaleSale, derive_slug

logger = logging.getLogger(__name__)


GETGEMS_API_BASE = "https://api.getgems.io/public-api"
GIFTS_HISTORY_PATH = "/v1/nfts/history/gifts"
NFT_DETAIL_PATH = "/v1/nft"  # GET /v1/nft/<address> for traits
GIFT_THRESHOLD_TON = 100.0
POLL_INTERVAL_SEC = 60.0
HTTP_TIMEOUT_SEC = 15.0
SEEN_LT_BUFFER = 500  # cap on per-source dedup set
# Getgems' history endpoint returns *on-chain* TON sales — the same sale
# is visible regardless of which UI (Fragment, Getgems, MRKT, …) was
# used. To get a useful "Sold on …" label, we delay Getgems posts by
# this many seconds so Fragment (and other UI-specific sources, when
# we add them) can win the dedup race when they've also seen the sale.
# After the delay, if no other source posted, Getgems falls back to
# emitting with its own label.
POST_DELAY_SEC = 180.0


_stats = {
    "polls": 0,
    "items_seen": 0,
    "sales_seen": 0,
    "sales_over_threshold": 0,
    "errors": 0,
    "attribute_fetches": 0,
    "attribute_fetch_errors": 0,
}


def get_stats() -> dict[str, int]:
    return dict(_stats)


async def _fetch_recent_sold(
    client: httpx.AsyncClient,
    api_key: str,
) -> list[dict]:
    """Return the most recent ``sold`` entries from the gifts history endpoint."""
    url = GETGEMS_API_BASE + GIFTS_HISTORY_PATH
    headers = {
        "Authorization": api_key,
        "Accept": "application/json",
    }
    params = {"types": "sold"}
    resp = await client.get(url, headers=headers, params=params)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("success"):
        raise RuntimeError(f"Getgems API returned success=false: {data!r}")
    return list(data.get("response", {}).get("items", []))


async def _fetch_nft_traits(
    client: httpx.AsyncClient,
    api_key: str,
    address: str,
) -> tuple[str | None, str | None, str | None]:
    """Fetch ``(model, backdrop, symbol)`` for an NFT from Getgems.

    Returns ``(None, None, None)`` on any failure — the post can still
    go out without traits, just with less detail.
    """
    url = f"{GETGEMS_API_BASE}{NFT_DETAIL_PATH}/{address}"
    headers = {"Authorization": api_key, "Accept": "application/json"}
    try:
        resp = await client.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        _stats["attribute_fetch_errors"] += 1
        logger.warning("Failed to fetch traits for %s", address)
        return None, None, None

    _stats["attribute_fetches"] += 1
    attrs = data.get("response", {}).get("attributes") or []
    by_trait = {a.get("traitType"): a.get("value") for a in attrs}
    return (
        by_trait.get("Model"),
        by_trait.get("Backdrop"),
        by_trait.get("Symbol"),
    )


def _to_sale(item: dict) -> WhaleSale | None:
    """Convert a Getgems history item to a :class:`WhaleSale`.

    Returns ``None`` if the item isn't a TON sale we can post.
    """
    td = item.get("typeData", {}) or {}
    if td.get("type") != "sold":
        return None
    if td.get("currency") != "TON":
        return None
    raw_price = td.get("price")
    if raw_price is None:
        return None
    try:
        price_ton = float(raw_price)
    except (TypeError, ValueError):
        return None
    if price_ton <= 0:
        return None

    name = item.get("name") or ""
    slug, collection_title, num = derive_slug(name)
    return WhaleSale(
        source="Getgems",
        title=name or "Unknown gift",
        price_ton=price_ton,
        collection_title=collection_title,
        num=num,
        slug=slug,
        nft_address=item.get("address"),
        seller_address=td.get("oldOwner"),
        buyer_address=td.get("newOwner"),
    )


async def _handle_whale_sale(
    client: httpx.AsyncClient,
    api_key: str,
    sale: WhaleSale,
    on_sold: Callable[[WhaleSale], Awaitable[None]],
) -> None:
    """Enrich + delay-post a single Getgems whale sale.

    Steps:
    1. Fetch NFT traits (Model/Backdrop/Symbol) — Getgems history doesn't
       include them, but the per-NFT endpoint does.
    2. Sleep ``POST_DELAY_SEC`` so Fragment (or any other UI-specific
       source) can win the dedup race with a more accurate ``source``
       label when they've also seen the sale.
    3. Call ``on_sold`` — the poster's TTL dedup cache silently drops
       this if another source already posted the same sale.
    """
    if sale.nft_address:
        model, backdrop, symbol = await _fetch_nft_traits(
            client, api_key, sale.nft_address
        )
        sale = WhaleSale(
            source=sale.source,
            title=sale.title,
            price_ton=sale.price_ton,
            collection_title=sale.collection_title,
            num=sale.num,
            slug=sale.slug,
            model=model or sale.model,
            backdrop=backdrop or sale.backdrop,
            symbol=symbol or sale.symbol,
            nft_address=sale.nft_address,
            seller_address=sale.seller_address,
            buyer_address=sale.buyer_address,
            marketplace_url=sale.marketplace_url,
        )

    if POST_DELAY_SEC > 0:
        await asyncio.sleep(POST_DELAY_SEC)

    try:
        await on_sold(sale)
    except Exception:
        logger.exception(
            "Getgems on_sold callback failed for %s",
            sale.title,
        )


async def run_getgems_feed(
    api_key: str,
    on_sold: Callable[[WhaleSale], Awaitable[None]],
    threshold_ton: float = GIFT_THRESHOLD_TON,
    poll_interval_sec: float = POLL_INTERVAL_SEC,
) -> None:
    """Main loop: poll Getgems gift sales and forward whales to ``on_sold``.

    Per-source dedup is done by Getgems' ``lt`` (logical time) per item.
    Cross-source dedup (same sale visible on Telegram Resale + Getgems +
    Fragment) is handled by the poster.
    """
    if not api_key:
        raise ValueError("Getgems API key is required")

    seen_lts: set[str] = set()
    bootstrap_done = False

    logger.info(
        "Getgems feed starting: threshold=%.1f TON, poll=%.1fs",
        threshold_ton,
        poll_interval_sec,
    )

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_SEC) as client:
        while True:
            try:
                items = await _fetch_recent_sold(client, api_key)
                _stats["polls"] += 1
                _stats["items_seen"] += len(items)

                # API returns newest-first. Walk forward, stop at first
                # already-seen lt, then process collected ones in
                # chronological order.
                fresh: list[dict] = []
                for it in items:
                    lt = it.get("lt")
                    if not lt:
                        continue
                    if lt in seen_lts:
                        break
                    fresh.append(it)

                if not bootstrap_done:
                    # First poll: seed dedup with whatever the API shows so we
                    # don't post a burst of historical sales on startup.
                    for it in fresh:
                        lt = it.get("lt")
                        if lt:
                            seen_lts.add(lt)
                    bootstrap_done = True
                    logger.info(
                        "Getgems bootstrap: marked %d existing sold items as seen.",
                        len(fresh),
                    )
                else:
                    for it in reversed(fresh):
                        lt = it.get("lt")
                        if lt:
                            seen_lts.add(lt)
                        sale = _to_sale(it)
                        if sale is None:
                            continue
                        _stats["sales_seen"] += 1
                        if sale.price_ton < threshold_ton:
                            continue
                        _stats["sales_over_threshold"] += 1
                        # Schedule each whale sale on its own task so the
                        # POST_DELAY_SEC head-start doesn't block polling.
                        asyncio.create_task(
                            _handle_whale_sale(client, api_key, sale, on_sold)
                        )

                # Bound dedup memory.
                if len(seen_lts) > SEEN_LT_BUFFER:
                    # Keep most recent half (set ordering isn't reliable, but
                    # this is just a soft cap; we'll rebuild from the next polls).
                    seen_lts = set(list(seen_lts)[-SEEN_LT_BUFFER // 2 :])

            except httpx.HTTPStatusError as e:
                _stats["errors"] += 1
                logger.warning(
                    "Getgems HTTP %s; backing off",
                    e.response.status_code,
                )
                await asyncio.sleep(poll_interval_sec * 2)
                continue
            except Exception as e:
                _stats["errors"] += 1
                logger.warning("Getgems poll error: %s", e)
                await asyncio.sleep(poll_interval_sec * 2)
                continue

            await asyncio.sleep(poll_interval_sec)
