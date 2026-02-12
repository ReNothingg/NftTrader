from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime


def now_str() -> str:
    return datetime.now().strftime("%H:%M:%S")


def log(message: str) -> None:
    print(f"[{now_str()}] [tonnel] {message}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Tonnel sniper (WIP)")
    parser.add_argument("--mode", choices=["mock", "live"], default="mock")
    parser.add_argument("--poll-interval", type=float, default=8.0)
    parser.add_argument("--once", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.mode == "live":
        log("live mode is not implemented yet")
        return 2

    log("mock mode started (WIP). Add API integration for Tonnel Relayer Bot.")
    while True:
        log("heartbeat")
        if args.once:
            return 0
        time.sleep(max(0.2, args.poll_interval))


if __name__ == "__main__":
    sys.exit(main())
