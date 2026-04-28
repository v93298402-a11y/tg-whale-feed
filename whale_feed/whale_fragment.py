"""Whale-feed source for Fragment (fragment.com) Telegram-gift sales.

Fragment doesn't publish a public API for sales history. We poll the
HTML page ``https://fragment.com/gifts?filter=sold&sort=listed&view=list``
every ``POLL_INTERVAL_SEC`` seconds, parse the table of recently sold
gifts, and forward every sale at or above ``threshold_ton`` to the
``on_sold`` callback.

Each sold-gift row in the page looks roughly like::

    <tr class="tm-row-selectable table-row-thumbed">
      <td>
        <a href="/gift/plushpepe-1821?...">
          <img src="https://nft.fragment.com/gift/plushpepe-1821.medium.jpg"/>
          <div class="table-cell-value-row">
            <div class="table-cell-value tm-value">Plush Pepe #1821</div>
            <div class="table-cell-status-thin thin-only ...">Sold</div>
          </div>
          <div class="table-cell-desc tm-nowrap">Gummy Frog, Platinum, Ring</div>
        </a>
      </td>
      <td class="thin-last-col">
        <a href="...">
          <div class="table-cell-value tm-value icon-before icon-ton">88,888</div>
          <div class="table-cell-desc">Sale price</div>
        </a>
      </td>
      <td class="wide-last-col wide-only">
        <a href="...">
          <div class="table-cell-value tm-value tm-status-unavail">Sold</div>
          <div class="table-cell-desc">
            <time datetime="2026-02-05T14:41:27+00:00">5 Feb 2026 at 14:41</time>
          </div>
        </a>
      </td>
    </tr>

We extract slug, name, attributes, sale price (TON, may include thousands
separators or a decimal part), and the ISO timestamp.
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone

import httpx

from whale_feed.whale_types import WhaleSale, derive_slug

logger = logging.getLogger(__name__)

FRAGMENT_BASE = "https://fragment.com"
SOLD_PATH = "/gifts?filter=sold&sort=listed&view=list"
GIFT_THRESHOLD_TON = 100.0
POLL_INTERVAL_SEC = 30.0
HTTP_TIMEOUT_SEC = 20.0
# Fragment's ?filter=sold&sort=listed page reshuffles old gifts to the top
# whenever they see any *listing-side* activity (re-list, transfer between
# wallets, etc.) — even years after the original sale. The row keeps its
# original sale-price + sale-timestamp, so we filter by timestamp: any
# "sale" whose timestamp is older than this is almost certainly a stale
# row resurfacing due to a transfer, not a fresh sale.
MAX_SALE_AGE = timedelta(hours=6)
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)
SEEN_KEY_BUFFER = 1000

# One regex capturing all the fields we need from a single sold-gift row.
# DOTALL is essential since the row spans many lines.
_ROW_RE = re.compile(
    r'<tr class="tm-row-selectable table-row-thumbed">\s*'
    r"<td>\s*"
    r'<a href="(?P<href>/gift/[^"]+)"[^>]*>\s*'
    r'<img src="[^"]+"[^>]*/?>\s*'
    r'<div class="table-cell-value-row">\s*'
    r'<div class="table-cell-value tm-value">(?P<name>[^<]+)</div>'
    r".*?"
    r'<div class="table-cell-desc tm-nowrap">(?P<attrs>[^<]+)</div>'
    r".*?"
    r'<div class="table-cell-value tm-value icon-before icon-ton">(?P<price>[^<]+)</div>'
    r".*?"
    r'<time datetime="(?P<ts>[^"]+)"',
    re.DOTALL,
)

_stats: dict[str, int] = {
    "polls": 0,
    "items_seen": 0,
    "whales_emitted": 0,
    "http_errors": 0,
    "parse_errors": 0,
    "stale_filtered": 0,
}


def get_stats() -> dict[str, int]:
    return dict(_stats)


def _parse_price(raw: str) -> float | None:
    """Parse a Fragment-rendered TON amount into a float.

    Examples accepted: ``"88,888"`` -> 88888.0, ``"6.59"`` -> 6.59,
    ``"1,234.56"`` -> 1234.56. Returns ``None`` if unparseable.
    """
    cleaned = raw.replace(",", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_attributes(raw: str) -> tuple[str | None, str | None, str | None]:
    """Split the comma-separated attributes blurb into model/backdrop/symbol.

    Fragment renders attributes as ``"<Model>, <Backdrop>, <Symbol>"`` but
    occasionally omits one (or has an extra), so we map positionally and
    pad with None.
    """
    parts = [p.strip() for p in raw.split(",")]
    parts = [p for p in parts if p]
    parts += [""] * 3
    model = parts[0] or None
    backdrop = parts[1] or None
    symbol = parts[2] or None
    return model, backdrop, symbol


def _slug_from_href(href: str) -> str | None:
    """Extract ``plushpepe-1821`` from ``/gift/plushpepe-1821?...``."""
    m = re.match(r"^/gift/([^?]+)", href)
    return m.group(1) if m else None


def _parse_iso_ts(ts: str) -> datetime | None:
    """Parse an ISO-8601 timestamp from Fragment (e.g. ``2026-04-27T12:05:00+00:00``)."""
    try:
        return datetime.fromisoformat(ts)
    except ValueError:
        return None


def _row_to_sale(row: re.Match[str]) -> WhaleSale | None:
    """Build a WhaleSale from one parsed row, or None if unusable.

    Returns ``None`` for rows whose sale-timestamp is older than
    :data:`MAX_SALE_AGE` — those are almost certainly old sales
    resurfacing on the page due to a transfer, not new whale sales.
    """
    href = row.group("href")
    name = row.group("name").strip()
    attrs = row.group("attrs")
    price_str = row.group("price")
    ts = row.group("ts").strip()

    price = _parse_price(price_str)
    if price is None:
        _stats["parse_errors"] += 1
        return None

    sale_dt = _parse_iso_ts(ts)
    if sale_dt is not None:
        now = datetime.now(timezone.utc)
        age = now - sale_dt
        if age > MAX_SALE_AGE:
            _stats["stale_filtered"] += 1
            logger.debug(
                "Fragment row filtered as stale: %s sold %s (age=%s) — "
                "likely resurfacing due to transfer, not a real sale",
                name,
                ts,
                age,
            )
            return None

    fragment_slug = _slug_from_href(href)
    # Derive the canonical CamelCase slug from the gift name so links go to
    # https://t.me/nft/<CamelSlug> (the Telegram-native NFT page that
    # renders the gift directly) and dedup_key matches Getgems output.
    canon_slug, collection_title, num = derive_slug(name)

    model, backdrop, symbol = _parse_attributes(attrs)

    return WhaleSale(
        source="Fragment",
        title=name,
        price_ton=price,
        collection_title=collection_title,
        num=num,
        slug=canon_slug or fragment_slug,
        model=model,
        backdrop=backdrop,
        symbol=symbol,
    )


def _row_dedup_key(row: re.Match[str]) -> str:
    """Per-source dedup key — keyed on slug only.

    Fragment's ``?filter=sold&sort=listed`` page reshuffles rows whenever a
    gift sees any listing-side activity (including re-listings or transfers
    long after the original sale). Older sold gifts can re-enter the top-60
    visible window with their original sale price + timestamp. To avoid
    re-posting them as if they were fresh sales, we dedup on slug alone:
    once we've seen a gift in the sold list, we never re-emit it.
    """
    slug = _slug_from_href(row.group("href"))
    return slug or row.group("href")


async def _fetch_sold_html(client: httpx.AsyncClient) -> str | None:
    url = f"{FRAGMENT_BASE}{SOLD_PATH}"
    try:
        r = await client.get(url, headers={"User-Agent": USER_AGENT})
        r.raise_for_status()
    except Exception:
        _stats["http_errors"] += 1
        logger.exception("Fragment HTTP error on %s", url)
        return None
    return r.text


async def run_fragment_feed(
    on_sold: Callable[[WhaleSale], Awaitable[None]],
    threshold_ton: float = GIFT_THRESHOLD_TON,
    poll_interval_sec: float = POLL_INTERVAL_SEC,
) -> None:
    """Main loop: poll Fragment sold-gifts page and forward whales to on_sold."""
    seen_keys: set[str] = set()
    bootstrap_done = False

    logger.info(
        "Fragment feed starting: threshold=%.1f TON, poll=%.1fs",
        threshold_ton,
        poll_interval_sec,
    )

    backoff = poll_interval_sec
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_SEC, http2=False) as client:
        while True:
            html = await _fetch_sold_html(client)
            if html is None:
                # transient error — back off then retry
                await asyncio.sleep(min(backoff * 2, 300.0))
                backoff = min(backoff * 2, 300.0)
                continue
            backoff = poll_interval_sec  # reset on success

            _stats["polls"] += 1
            rows = list(_ROW_RE.finditer(html))
            _stats["items_seen"] += len(rows)

            if not bootstrap_done:
                # Mark every visible item as already seen so we don't replay
                # historical sales on first poll.
                for r in rows:
                    seen_keys.add(_row_dedup_key(r))
                bootstrap_done = True
                logger.info(
                    "Fragment bootstrap: marked %d existing sold items as seen.",
                    len(rows),
                )
                await asyncio.sleep(poll_interval_sec)
                continue

            new_whales: list[WhaleSale] = []
            for r in rows:
                key = _row_dedup_key(r)
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                sale = _row_to_sale(r)
                if sale is None:
                    continue
                if sale.price_ton < threshold_ton:
                    continue
                new_whales.append(sale)

            # Cap the seen-set so it can't grow without bound.
            if len(seen_keys) > SEEN_KEY_BUFFER:
                # Drop oldest by simply rebuilding from the most-recent rows
                # we just observed plus a tail of the existing set.
                fresh = {_row_dedup_key(r) for r in rows}
                seen_keys = fresh | set(list(seen_keys)[-SEEN_KEY_BUFFER:])

            # Fragment lists newest sales first; emit oldest-first so posts
            # arrive in chronological order.
            for sale in reversed(new_whales):
                _stats["whales_emitted"] += 1
                logger.info(
                    "Fragment whale sale: %s @ %.2f TON",
                    sale.title,
                    sale.price_ton,
                )
                try:
                    await on_sold(sale)
                except Exception:
                    logger.exception(
                        "on_sold callback failed for Fragment sale %s",
                        sale.title,
                    )

            await asyncio.sleep(poll_interval_sec)
