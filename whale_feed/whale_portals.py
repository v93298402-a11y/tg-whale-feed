"""Whale-feed source for Portals (portal-market.com) Telegram-gift sales.

Portals' web Mini-App fetches its public market activity from
``GET https://portal-market.com/api/market/actions/?offset=0&limit=20``.
The endpoint returns mixed events (listings, price updates, sales,
delistings, …); we filter to sale-like events and forward every sale at
or above ``threshold_ton`` to ``on_sold``.

Response (abbreviated)::

    {"actions": [
       {"nft": {
           "id": "<uuid>",
           "name": "Timeless Book",
           "external_collection_number": 21182,
           "attributes": [
              {"type": "model",    "value": "Rocket Science", ...},
              {"type": "symbol",   "value": "Nigiri",         ...},
              {"type": "backdrop", "value": "Lavender",       ...}
           ],
           ...
        },
        "type":  "price_update" | "listing" | "sale" | "buy" | …,
        "amount": "13.98",  # TON, decimal string
        "old_price": "13.99",
        "created_at": "2026-04-27T09:47:21.368822Z"
       },
       …
    ]}

The captured HAR didn't include any "sale"-type events (only
``price_update`` and ``listing``), so the actual tag for a sale isn't
100% confirmed. We accept any of {"sale","sold","buy","purchase",
"buyout"} as sale-events; logs will tell us which one MRKT actually
emits and we can tighten the filter later.
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone

import httpx

from whale_feed.whale_types import WhaleSale

logger = logging.getLogger(__name__)


PORTALS_API_BASE = "https://portal-market.com"
ACTIONS_PATH = "/api/market/actions/"
GIFT_THRESHOLD_TON = 100.0
POLL_INTERVAL_SEC = 60.0
HTTP_TIMEOUT_SEC = 20.0
ACTIONS_LIMIT = 200  # how many recent events to fetch per poll
SEEN_BUFFER = 1000
MAX_SALE_AGE = timedelta(hours=6)
SALE_TYPES = {"sale", "sold", "buy", "purchase", "buyout"}

_BASE_HEADERS = {
    "Accept": "application/json",
    "Referer": "https://portal-market.com/market-activity",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
    ),
}


_stats: dict[str, int] = {
    "polls": 0,
    "items_seen": 0,
    "sales_seen": 0,
    "whales_emitted": 0,
    "stale_filtered": 0,
    "errors": 0,
    "auth_refreshes": 0,
}


def get_stats() -> dict[str, int]:
    return dict(_stats)


def _parse_iso_ts(ts: str) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None


def _build_slug(title: str, number: int) -> str | None:
    """Build the canonical CamelCase slug Telegram uses for t.me/nft/<slug>.

    Same rule as :func:`whale_feed.whale_types.derive_slug`: drop everything
    except letters and digits, then append ``-<number>``.
    """
    cleaned = re.sub(r"[^A-Za-z0-9]", "", title or "")
    if not cleaned:
        return None
    return f"{cleaned}-{number}"


def _to_sale(action: dict) -> WhaleSale | None:
    """Convert one Portals action to a WhaleSale, or ``None`` to skip."""
    atype = (action.get("type") or "").lower()
    if atype not in SALE_TYPES:
        return None

    amount_raw = action.get("amount")
    if amount_raw is None:
        return None
    try:
        price_ton = float(amount_raw)
    except (TypeError, ValueError):
        return None
    if price_ton <= 0:
        return None

    nft = action.get("nft") or {}
    title = nft.get("name") or ""
    number = nft.get("external_collection_number")
    if not title or number is None:
        return None

    full_title = f"{title} #{number}"
    slug = _build_slug(title, number)

    # Portals returns attributes as [{type, value, rarity_per_mille}]
    attrs_by_type: dict[str, str] = {}
    for a in nft.get("attributes") or []:
        t = (a.get("type") or "").lower()
        v = a.get("value")
        if t and v:
            attrs_by_type[t] = v

    return WhaleSale(
        source="Portals",
        title=full_title,
        price_ton=price_ton,
        collection_title=title,
        num=int(number),
        slug=slug,
        model=attrs_by_type.get("model"),
        backdrop=attrs_by_type.get("backdrop"),
        symbol=attrs_by_type.get("symbol"),
        nft_address=None,
        seller_address=None,
        buyer_address=None,
    )


class _AuthError(Exception):
    pass


async def _fetch_actions(client: httpx.AsyncClient, token: str) -> list[dict]:
    url = PORTALS_API_BASE + ACTIONS_PATH
    # action_types=sell is a server-side filter the Mini-App uses: it
    # returns ONLY purchase events, dropping price_update / listing /
    # transfer noise. Without it, low-volume whale sales (1-2 per hour)
    # get pushed off the latest-N window by high-volume listing churn.
    params = {"offset": 0, "limit": ACTIONS_LIMIT, "action_types": "sell"}
    headers = dict(_BASE_HEADERS)
    if token:
        headers["Authorization"] = token
    resp = await client.get(url, params=params, headers=headers)
    if resp.status_code in (401, 403):
        raise _AuthError(f"Portals auth error {resp.status_code}")
    resp.raise_for_status()
    data = resp.json()
    return list(data.get("actions", []))


async def run_portals_feed(
    on_sold: Callable[[WhaleSale], Awaitable[None]],
    initial_token: str,
    refresh_token: Callable[[], Awaitable[str]] | None = None,
    threshold_ton: float = GIFT_THRESHOLD_TON,
    poll_interval_sec: float = POLL_INTERVAL_SEC,
) -> None:
    """Main loop: poll Portals' market-activity feed and forward whales.

    ``initial_token`` is the JWT from :func:`whale_feed.auth.get_portals_token`
    (``"tma <init_data>"`` form). ``refresh_token`` is called on 401/403.
    """
    seen_keys: set[str] = set()
    bootstrap_done = False
    token = initial_token

    logger.info(
        "Portals feed starting: threshold=%.1f TON, poll=%.1fs, token_present=%s",
        threshold_ton,
        poll_interval_sec,
        bool(token),
    )

    backoff = poll_interval_sec
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_SEC) as client:
        while True:
            try:
                actions = await _fetch_actions(client, token)
            except _AuthError:
                if refresh_token is None:
                    _stats["errors"] += 1
                    logger.warning(
                        "Portals auth expired and no refresh callback set — "
                        "sleeping and retrying."
                    )
                    await asyncio.sleep(poll_interval_sec)
                    continue
                logger.warning("Portals auth expired, re-fetching token…")
                try:
                    new_tok = await refresh_token()
                except Exception:
                    logger.exception("Portals token refresh failed")
                    new_tok = ""
                if new_tok:
                    token = new_tok
                    _stats["auth_refreshes"] += 1
                    logger.info("Portals token refreshed.")
                else:
                    _stats["errors"] += 1
                    logger.warning("Portals token refresh returned empty.")
                await asyncio.sleep(poll_interval_sec)
                continue
            except Exception:
                _stats["errors"] += 1
                logger.exception("Portals feed HTTP error")
                await asyncio.sleep(min(backoff * 2, 300.0))
                backoff = min(backoff * 2, 300.0)
                continue
            backoff = poll_interval_sec
            _stats["polls"] += 1
            _stats["items_seen"] += len(actions)

            # Each action's natural id: nft.id + '|' + created_at
            def _key(a: dict) -> str:
                nft = a.get("nft") or {}
                return f"{nft.get('id', '?')}|{a.get('created_at', '?')}|{a.get('type', '?')}"

            if not bootstrap_done:
                for a in actions:
                    seen_keys.add(_key(a))
                bootstrap_done = True
                logger.info(
                    "Portals bootstrap: marked %d existing actions as seen.",
                    len(actions),
                )
                await asyncio.sleep(poll_interval_sec)
                continue

            now = datetime.now(timezone.utc)
            new_whales: list[WhaleSale] = []
            unknown_types_logged: set[str] = set()
            for a in actions:
                k = _key(a)
                if k in seen_keys:
                    continue
                seen_keys.add(k)

                atype = (a.get("type") or "").lower()
                if atype not in SALE_TYPES:
                    # Log unknown types once, so we can extend SALE_TYPES if
                    # we see a sale-like event we don't recognize.
                    if atype and atype not in unknown_types_logged:
                        unknown_types_logged.add(atype)
                        try:
                            amt = float(a.get("amount") or 0)
                        except (TypeError, ValueError):
                            amt = 0.0
                        if amt >= 50.0:
                            logger.info(
                                "Portals: skipping unknown action type=%r "
                                "amount=%.2f nft=%s — extend SALE_TYPES if "
                                "this is a sale.",
                                atype,
                                amt,
                                (a.get("nft") or {}).get("name"),
                            )
                    continue
                _stats["sales_seen"] += 1

                dt = _parse_iso_ts(a.get("created_at", ""))
                if dt is not None and (now - dt) > MAX_SALE_AGE:
                    _stats["stale_filtered"] += 1
                    continue

                sale = _to_sale(a)
                if sale is None:
                    nft = a.get("nft") or {}
                    logger.warning(
                        "Portals: dropping sale-type=%r amount=%r — could "
                        "not build WhaleSale (nft=%s number=%s).",
                        atype,
                        a.get("amount"),
                        nft.get("name"),
                        nft.get("external_collection_number"),
                    )
                    continue
                # Always log sale-type events with non-trivial amount so we
                # can audit threshold filtering and dedup behaviour.
                if sale.price_ton >= 50.0:
                    logger.info(
                        "Portals sale seen: %s @ %.2f TON (threshold=%.1f, "
                        "%s)",
                        sale.title,
                        sale.price_ton,
                        threshold_ton,
                        "EMITTING" if sale.price_ton >= threshold_ton
                        else "below threshold",
                    )
                if sale.price_ton < threshold_ton:
                    continue
                new_whales.append(sale)

            if len(seen_keys) > SEEN_BUFFER:
                fresh = {_key(a) for a in actions}
                seen_keys = fresh | set(list(seen_keys)[-SEEN_BUFFER:])

            # Newest-first; emit oldest-first.
            for sale in reversed(new_whales):
                _stats["whales_emitted"] += 1
                logger.info(
                    "Portals whale sale: %s @ %.2f TON",
                    sale.title,
                    sale.price_ton,
                )
                try:
                    await on_sold(sale)
                except Exception:
                    logger.exception(
                        "on_sold callback failed for Portals sale %s",
                        sale.title,
                    )

            await asyncio.sleep(poll_interval_sec)
