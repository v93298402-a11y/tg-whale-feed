"""Read recent messages from @mrktnotification and @GiftNotification,
run them through the project's parsers, and report whales >= 100 TON."""

from __future__ import annotations

import asyncio
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
sys.path.insert(0, _ROOT)

from dotenv import load_dotenv
from telethon import TelegramClient

from whale_feed.config import DEFAULT_API_HASH, DEFAULT_API_ID
from whale_feed.whale_mrkt import parse_sale_message as parse_mrkt
from whale_feed.whale_tonnel import parse_sale_message as parse_tonnel

load_dotenv(os.path.join(_ROOT, ".env"))

THRESHOLD = 100.0
LIMIT = 100


async def scan(client, channel, parser, label):
    print(f"\n=== {label} (last {LIMIT} messages) ===")
    try:
        entity = await client.get_entity(channel)
    except Exception as e:
        print(f"  failed to resolve @{channel}: {e}")
        return
    total = 0
    parsed = 0
    whales = []
    misses_with_money = []
    async for msg in client.iter_messages(entity, limit=LIMIT):
        total += 1
        text = msg.message or ""
        if not text:
            continue
        sale = parser(text)
        if sale is None:
            if "TON" in text or "Price" in text:
                snippet = text.replace("\n", " | ")[:120]
                misses_with_money.append((msg.id, msg.date, snippet))
            continue
        parsed += 1
        if sale.price_ton >= THRESHOLD:
            whales.append((msg.id, msg.date, sale))
    print(f"  scanned: {total} messages, parsed: {parsed} sales")
    print(f"  whales >= {THRESHOLD} TON: {len(whales)}")
    for mid, dt, sale in whales[:20]:
        print(
            f"    [{dt:%Y-%m-%d %H:%M}] id={mid} {sale.title} {sale.price_ton} TON "
            f"slug={sale.slug} model={sale.model} sym={sale.symbol} bd={sale.backdrop}"
        )
    if misses_with_money:
        print(f"  unparsed messages mentioning Price/TON: {len(misses_with_money)}")
        for mid, dt, snippet in misses_with_money[:5]:
            print(f"    [{dt:%H:%M}] id={mid} :: {snippet}")


async def main():
    api_id = int(os.getenv("API_ID", str(DEFAULT_API_ID)))
    api_hash = os.getenv("API_HASH", DEFAULT_API_HASH)
    session = os.path.join(_ROOT, os.getenv("SESSION_NAME", "whale_feed"))
    client = TelegramClient(session, api_id, api_hash)
    await client.connect()
    if not await client.is_user_authorized():
        print("ERROR: session not authorised — cannot scan channels.")
        return
    me = await client.get_me()
    print(f"Connected as {me.first_name} (id={me.id})")
    await scan(client, "giftwhalefeed", parse_mrkt, "MRKT @giftwhalefeed (filter Sold on MRKT)")
    await scan(client, "GiftNotification", parse_tonnel, "Tonnel @GiftNotification")
    await client.disconnect()


asyncio.run(main())
