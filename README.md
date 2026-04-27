# tg-whale-feed

Telegram channel poster that aggregates gift whale-sales (≥ 100 TON by default)
from six marketplaces and posts them in real time to your channel.

Sources monitored:

| Source | Method | Auth |
|---|---|---|
| Telegram Resale | Telethon polling of marketplace listings | none (Telethon session) |
| Getgems | REST API (recent sales) | `GETGEMS_API_KEY` |
| Fragment | HTML scraping | none |
| Portals | REST API + Telegram WebView OAuth | auto-fetched |
| MRKT | Telethon scraper of `@giftwhalefeed` (filter `Sold on MRKT`) | none |
| Tonnel | Telethon scraper of `@GiftNotification` (Auction + Gift Sold) | none |

Cross-source dedup ensures the same sale picked up by 2+ sources is posted only
once. Source labels for Portals / Tonnel / MRKT include built-in referral
hyperlinks. Tonnel auction settlements are tagged `(Auction)`.

## Quick start

```bash
git clone https://github.com/<owner>/tg-whale-feed.git
cd tg-whale-feed
python3.10 -m venv .venv
.venv/bin/pip install -e .
cp .env.example .env
# edit .env: set WHALE_BOT_TOKEN and WHALE_CHANNEL

# First run logs you in to Telegram (interactive — phone + code).
.venv/bin/python -m whale_feed
```

After the first run, a `whale_feed.session` file is created with your Telethon
auth — subsequent runs are non-interactive.

## Running as a systemd service

```ini
# /etc/systemd/system/whale-feed.service
[Unit]
Description=Telegram whale-feed (gift sales >=100 TON to channel)
After=network-online.target

[Service]
Type=simple
WorkingDirectory=/root/tg-whale-feed
EnvironmentFile=/root/tg-whale-feed/.env
ExecStart=/root/tg-whale-feed/.venv/bin/python -m whale_feed
Restart=on-failure
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

```bash
systemctl daemon-reload
systemctl enable --now whale-feed
journalctl -u whale-feed -f
```

## Configuration reference

See [`.env.example`](./.env.example) for all environment variables. Most are
optional — only `WHALE_BOT_TOKEN` and `WHALE_CHANNEL` are required.

## Diagnostic scripts

* `scripts/diag_channels.py` — scan the last 100 messages of `@giftwhalefeed`
  and `@GiftNotification`, run them through the project's parsers, and report
  detected whales plus any unparsed messages mentioning `Price/TON`.
* `scripts/lookup_gift.py <slug>` — query the current owner of a Telegram gift
  by slug (e.g. `IonicDryer-11992`). Useful for tracing buyer addresses /
  Telegram user IDs after a sale.

Both scripts copy the active Telethon session to a temp directory before
connecting, so they can run alongside the live whale-feed service without
hitting `sqlite3 database is locked`.

## Layout

```
whale_feed/
  __init__.py
  __main__.py        # CLI entry point (python -m whale_feed)
  auth.py            # Portals / MRKT WebView OAuth helpers
  config.py          # Telegram API id/hash defaults + env helpers
  whale_types.py     # WhaleSale dataclass + DedupCache + slug derivation
  whale_poster.py    # post WhaleSale to channel (HTML format, dedup, throttle)
  whale_enrich.py    # backfill missing model/symbol/backdrop via GetUniqueStarGift
  whale_feed.py      # Telegram Resale source (Telethon)
  whale_getgems.py   # Getgems source (REST)
  whale_fragment.py  # Fragment source (HTML)
  whale_portals.py   # Portals source (REST + token refresh)
  whale_mrkt.py      # MRKT source (Telethon scraper)
  whale_tonnel.py    # Tonnel source (Telethon scraper)
scripts/
  diag_channels.py   # one-shot channel parser diagnostic
  lookup_gift.py     # one-shot gift owner lookup
```
