"""Entry point: ``python -m whale_feed`` or the ``tg-whale-feed`` script.

Runs the multi-source whale-feed monitor. Sources:

  * TonCenter — on-chain monitoring via TonCenter REST API v3 (replaces
    the old Telegram Resale polling). Enabled by default; disable with
    ``WHALE_TONCENTER_ENABLED=0``.  Optional ``TONCENTER_API_KEY`` for
    higher rate limits.
  * Getgems  — enabled if ``GETGEMS_API_KEY`` is set.
  * Fragment — enabled by default; disable with ``WHALE_FRAGMENT_ENABLED=0``.
  * MRKT     — enabled by default; disable with ``WHALE_MRKT_ENABLED=0``.
  * Portals  — enabled by default; disable with ``WHALE_PORTALS_ENABLED=0``.
  * Tonnel   — enabled by default; disable with ``WHALE_TONNEL_ENABLED=0``.

Sales >= ``--threshold`` TON are posted to ``WHALE_CHANNEL`` via
``WHALE_BOT_TOKEN``.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys

from dotenv import load_dotenv
from telethon import TelegramClient

from whale_feed.auth import get_portals_token
from whale_feed.config import get_api_hash, get_api_id, get_session_name
from whale_feed.whale_fragment import get_stats as get_fragment_stats
from whale_feed.whale_fragment import run_fragment_feed
from whale_feed.whale_getgems import get_stats as get_getgems_stats
from whale_feed.whale_getgems import run_getgems_feed
from whale_feed.whale_mrkt import get_stats as get_mrkt_stats
from whale_feed.whale_mrkt import run_mrkt_feed
from whale_feed.whale_portals import get_stats as get_portals_stats
from whale_feed.whale_portals import run_portals_feed
from whale_feed.whale_poster import create_whale_poster
from whale_feed.whale_toncenter import get_stats as get_toncenter_stats
from whale_feed.whale_toncenter import run_toncenter_feed
from whale_feed.whale_tonnel import get_stats as get_tonnel_stats
from whale_feed.whale_tonnel import run_tonnel_feed

logger = logging.getLogger("whale_feed")


def _setup_logging(level: str) -> None:
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(level=getattr(logging, level, logging.INFO), format=fmt)


def _make_client() -> TelegramClient:
    return TelegramClient(get_session_name(), get_api_id(), get_api_hash())


def _env_flag(name: str, default: bool = True) -> bool:
    raw = os.getenv(name, "1" if default else "0").strip()
    return raw not in ("0", "false", "False", "no", "")


async def _run(threshold_ton: float) -> None:
    load_dotenv()
    bot_token = os.getenv("WHALE_BOT_TOKEN", "").strip()
    channel = os.getenv("WHALE_CHANNEL", "").strip()
    if not bot_token:
        logger.error("WHALE_BOT_TOKEN not set in .env — cannot start")
        sys.exit(1)
    if not channel:
        logger.error("WHALE_CHANNEL not set in .env — cannot start")
        sys.exit(1)

    toncenter_enabled = _env_flag("WHALE_TONCENTER_ENABLED")
    toncenter_key = os.getenv("TONCENTER_API_KEY", "").strip() or None
    getgems_key = os.getenv("GETGEMS_API_KEY", "").strip()
    fragment_enabled = _env_flag("WHALE_FRAGMENT_ENABLED")
    mrkt_enabled = _env_flag("WHALE_MRKT_ENABLED")
    portals_enabled = _env_flag("WHALE_PORTALS_ENABLED")
    tonnel_enabled = _env_flag("WHALE_TONNEL_ENABLED")

    client = _make_client()
    logger.info("Connecting to Telegram for whale feed…")
    await client.start()
    me = await client.get_me()
    logger.info("Logged in as %s (id=%d)", me.first_name, me.id)

    poster = create_whale_poster(bot_token=bot_token, channel=channel)
    logger.info(
        "Whale poster ready: channel=%s, threshold=%.1f TON",
        channel,
        threshold_ton,
    )

    loop = asyncio.get_running_loop()

    def _shutdown() -> None:
        logger.info(
            "Whale feed shutting down… toncenter=%s getgems=%s fragment=%s "
            "mrkt=%s portals=%s tonnel=%s",
            get_toncenter_stats(),
            get_getgems_stats(),
            get_fragment_stats(),
            get_mrkt_stats(),
            get_portals_stats(),
            get_tonnel_stats(),
        )
        for task in asyncio.all_tasks(loop):
            task.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown)

    tasks: list[asyncio.Task] = []

    if toncenter_enabled:
        logger.info(
            "TonCenter source enabled (on-chain NFT purchase monitoring, "
            "api_key=%s).",
            "present" if toncenter_key else "absent (free tier)",
        )
        tasks.append(
            asyncio.create_task(
                run_toncenter_feed(
                    on_sold=poster,
                    threshold_ton=threshold_ton,
                    api_key=toncenter_key,
                ),
                name="whale-toncenter",
            )
        )
    else:
        logger.info(
            "TonCenter source disabled (WHALE_TONCENTER_ENABLED=0).",
        )

    if getgems_key:
        logger.info("Getgems source enabled (API key present).")
        tasks.append(
            asyncio.create_task(
                run_getgems_feed(
                    api_key=getgems_key,
                    on_sold=poster,
                    threshold_ton=threshold_ton,
                ),
                name="whale-getgems",
            )
        )
    else:
        logger.info(
            "Getgems source disabled — set GETGEMS_API_KEY in .env to enable.",
        )

    if fragment_enabled:
        logger.info("Fragment source enabled (HTML polling).")
        tasks.append(
            asyncio.create_task(
                run_fragment_feed(
                    on_sold=poster,
                    threshold_ton=threshold_ton,
                ),
                name="whale-fragment",
            )
        )
    else:
        logger.info("Fragment source disabled (WHALE_FRAGMENT_ENABLED=0).")

    if mrkt_enabled:
        logger.info(
            "MRKT source enabled (Telethon channel scraper @giftwhalefeed).",
        )
        tasks.append(
            asyncio.create_task(
                run_mrkt_feed(
                    client=client,
                    on_sold=poster,
                    threshold_ton=threshold_ton,
                ),
                name="whale-mrkt",
            )
        )
    else:
        logger.info("MRKT source disabled (WHALE_MRKT_ENABLED=0).")

    if portals_enabled:
        logger.info("Portals source: fetching auth token via Telethon WebView…")
        portals_token = await get_portals_token(client)

        async def _refresh_portals() -> str:
            return await get_portals_token(client)

        if portals_token:
            logger.info("Portals source enabled (token obtained).")
            tasks.append(
                asyncio.create_task(
                    run_portals_feed(
                        on_sold=poster,
                        initial_token=portals_token,
                        refresh_token=_refresh_portals,
                        threshold_ton=threshold_ton,
                    ),
                    name="whale-portals",
                )
            )
        else:
            logger.warning(
                "Portals source disabled — failed to obtain auth token.",
            )
    else:
        logger.info("Portals source disabled (WHALE_PORTALS_ENABLED=0).")

    if tonnel_enabled:
        logger.info(
            "Tonnel source enabled (Telethon channel scraper @GiftNotification).",
        )
        tasks.append(
            asyncio.create_task(
                run_tonnel_feed(
                    client=client,
                    on_sold=poster,
                    threshold_ton=threshold_ton,
                ),
                name="whale-tonnel",
            )
        )
    else:
        logger.info("Tonnel source disabled (WHALE_TONNEL_ENABLED=0).")

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    finally:
        await client.disconnect()
        logger.info(
            "Whale feed disconnected. Final stats: toncenter=%s getgems=%s "
            "fragment=%s mrkt=%s portals=%s tonnel=%s",
            get_toncenter_stats(),
            get_getgems_stats(),
            get_fragment_stats(),
            get_mrkt_stats(),
            get_portals_stats(),
            get_tonnel_stats(),
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="tg-whale-feed",
        description=(
            "Post Telegram gift sales >= threshold TON across 7 marketplaces "
            "to a Telegram channel."
        ),
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=float(os.getenv("WHALE_THRESHOLD_TON", "100.0")),
        help="Minimum TON price for a sale to count as a whale (default: 100.0)",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO"),
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="Logging level (default: INFO)",
    )
    args = parser.parse_args()

    _setup_logging(args.log_level)

    try:
        asyncio.run(_run(threshold_ton=args.threshold))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
