from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import datetime

from .config_loader import (
    ACCOUNTS_FILE_DEFAULT,
    API_BASE_DEFAULT,
    AUTH_FILE_DEFAULT,
    STATE_DB_DEFAULT,
    STRATEGY_FILE_DEFAULT,
    load_app_config,
)
from .engine import PortalEngine


def now_str() -> str:
    return datetime.now().strftime("%H:%M:%S")


def log(msg: str) -> None:
    print(f"[{now_str()}] {msg}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Portal Market multi-account trader")
    parser.add_argument("--api-base", default=os.getenv("PORTAL_API_BASE", API_BASE_DEFAULT))
    parser.add_argument("--auth-file", default=os.getenv("AUTH_FILE", AUTH_FILE_DEFAULT))
    parser.add_argument(
        "--strategy-file",
        default=os.getenv("STRATEGY_FILE", STRATEGY_FILE_DEFAULT),
        help="JSON strategy file (offers/orders/sell/liquidity/telegram)",
    )
    parser.add_argument(
        "--accounts-file",
        default=os.getenv("PORTAL_ACCOUNTS_FILE", ACCOUNTS_FILE_DEFAULT),
        help="JSON with multiple Portal accounts",
    )
    parser.add_argument(
        "--state-db",
        default=os.getenv("STATE_DB_PATH", STATE_DB_DEFAULT),
        help="SQLite DB path for trade journal and profit stats",
    )
    parser.add_argument("--live", action="store_true", help="Send real offers/orders/listings")
    parser.add_argument(
        "--no-warm-start",
        action="store_true",
        help="Process current listings immediately (without warm cache)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        cfg = load_app_config(
            strategy_file=args.strategy_file,
            accounts_file=args.accounts_file,
            auth_file=args.auth_file,
            api_base=args.api_base,
            state_db_path=args.state_db,
            live_mode=bool(args.live),
            no_warm_start=bool(args.no_warm_start),
        )
    except Exception as exc:
        log(f"CONFIG ERROR: {exc}")
        return 1

    mode = "LIVE" if not cfg.runtime.dry_run else "DRY-RUN"
    log(f"Mode: {mode}")
    log(f"Accounts: {', '.join(x.name for x in cfg.accounts)}")
    log(
        "Rules: "
        f"offers={len(cfg.offer_rules)} orders={len(cfg.order_rules)} sells={len(cfg.sell_rules)}"
    )
    if cfg.telegram.enabled:
        log("Telegram bot: enabled")
    else:
        log("Telegram bot: disabled (check telegram.enabled and TELEGRAM_BOT_TOKEN)")

    engine = PortalEngine(cfg)
    try:
        return asyncio.run(engine.run())
    except KeyboardInterrupt:
        log("Stopped by user")
        return 0


if __name__ == "__main__":
    sys.exit(main())
