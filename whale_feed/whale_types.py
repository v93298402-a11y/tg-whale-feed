"""Shared types and helpers for multi-source whale-feed.

This module defines the :class:`WhaleSale` dataclass used by every whale-feed
source (Telegram Resale via Telethon, Getgems via REST, Fragment via HTML
scraping, …) when handing a confirmed sale event to the poster.

It also exposes a tiny in-memory dedup helper so a sale picked up by more
than one source produces only one channel post.
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class WhaleSale:
    """A confirmed gift sale ready to be posted to a channel.

    Sources fill in whatever fields they can. The poster handles missing
    fields gracefully.
    """

    source: str  # "Telegram", "Getgems", "Fragment"
    title: str  # e.g. "Lol Pop #1235"
    price_ton: float
    collection_title: str | None = None  # e.g. "Lol Pop"
    num: int | None = None  # e.g. 1235
    slug: str | None = None  # for https://t.me/nft/<slug>
    model: str | None = None
    backdrop: str | None = None
    symbol: str | None = None
    nft_address: str | None = None  # TON address (for Getgems/blockchain sources)
    seller_address: str | None = None
    buyer_address: str | None = None
    marketplace_url: str | None = None  # explicit link override (e.g. fragment.com)
    # True if the sale was an auction settlement (e.g. Tonnel "Auction
    # Finished!" posts). The poster renders this as a "(Auction)" suffix
    # on the source label so readers know the price came from a winning
    # bid rather than a fixed-price listing.
    is_auction: bool = False

    @property
    def link(self) -> str:
        if self.marketplace_url:
            return self.marketplace_url
        if self.slug:
            return f"https://t.me/nft/{self.slug}"
        if self.nft_address:
            return f"https://getgems.io/nft/{self.nft_address}"
        return ""

    @property
    def dedup_key(self) -> str:
        """Stable key for cross-source dedup.

        Two sources reporting the same sale will likely agree on slug (or
        nft_address) and price, so we key on that. Price is rounded to
        whole TON to absorb tiny rounding differences between data sources,
        and the slug is lowercased so e.g. ``plushpepe-1821`` (Fragment)
        matches ``PlushPepe-1821`` (Getgems / Telegram).
        """
        ident = (self.slug or self.nft_address or self.title or "").lower()
        return f"{ident}:{int(round(self.price_ton))}"


# ---------------------------------------------------------------------------
# Slug derivation from human-readable name
# ---------------------------------------------------------------------------

_NAME_NUM_RE = re.compile(r"^(?P<title>.+?)\s*#\s*(?P<num>\d+)\s*$")


def derive_slug(name: str | None) -> tuple[str | None, str | None, int | None]:
    """Best-effort: split a human gift name into (slug, collection_title, num).

    Examples:
        "Lol Pop #1235"      -> ("LolPop-1235", "Lol Pop", 1235)
        "Snoop Dogg #500"    -> ("SnoopDogg-500", "Snoop Dogg", 500)
        "Plush Pepe #14"     -> ("PlushPepe-14", "Plush Pepe", 14)

    Returns (None, name_or_None, None) if the format is unrecognised.
    """
    if not name:
        return None, None, None
    m = _NAME_NUM_RE.match(name.strip())
    if not m:
        return None, name.strip() or None, None
    title = m.group("title").strip()
    try:
        num = int(m.group("num"))
    except ValueError:
        return None, title or None, None
    # Telegram's https://t.me/nft/<slug> is built from CamelCase letters and
    # digits only — apostrophes, accents, and punctuation are dropped.
    # E.g. "Durov's Cap" -> "DurovsCap", "B-Day Candle" -> "BDayCandle".
    slug_title = re.sub(r"[^A-Za-z0-9]", "", title)
    if not slug_title:
        return None, title or None, num
    slug = f"{slug_title}-{num}"
    return slug, title, num


# ---------------------------------------------------------------------------
# Dedup cache
# ---------------------------------------------------------------------------


class DedupCache:
    """In-memory TTL cache for cross-source dedup of whale sales.

    Not thread-safe: caller is expected to use it from a single asyncio task
    (or to hold an asyncio.Lock around mark/seen if multiple tasks).
    """

    def __init__(self, ttl_sec: float = 1800.0, max_size: int = 5000) -> None:
        self._ttl = ttl_sec
        self._max = max_size
        self._seen: dict[str, float] = {}

    def _now(self) -> float:
        return time.monotonic()

    def _prune(self) -> None:
        now = self._now()
        expired = [k for k, ts in self._seen.items() if now - ts > self._ttl]
        for k in expired:
            self._seen.pop(k, None)
        # Hard cap size: drop oldest if exceeded.
        if len(self._seen) > self._max:
            ordered = sorted(self._seen.items(), key=lambda kv: kv[1])
            for k, _ in ordered[: len(self._seen) - self._max]:
                self._seen.pop(k, None)

    def seen(self, key: str) -> bool:
        """Return True if `key` is in cache and not expired."""
        ts = self._seen.get(key)
        if ts is None:
            return False
        if self._now() - ts > self._ttl:
            self._seen.pop(key, None)
            return False
        return True

    def mark(self, key: str) -> None:
        """Mark `key` as seen at the current time. Prunes opportunistically."""
        self._seen[key] = self._now()
        if len(self._seen) > self._max * 2:
            self._prune()

    def __contains__(self, key: object) -> bool:  # convenience
        return isinstance(key, str) and self.seen(key)

    def __len__(self) -> int:
        return len(self._seen)
