from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter


API_BASE_DEFAULT = "https://portal-market.com/api"
AUTH_FILE_DEFAULT = "auth.txt"
STRATEGY_FILE_DEFAULT = "src/services/portal/config/strategy.json"
DECIMAL_STEP = Decimal("0.01")
POLICY_OVERRIDE_KEYS = {
    "offer_factor",
    "max_listing_to_floor",
    "min_offer",
    "max_offer",
    "min_ask",
    "max_ask",
    "min_floor",
    "max_floor",
    "min_total_rarity_per_mille",
    "max_total_rarity_per_mille",
    "expiration_days",
    "skip_crafted",
    "price_step",
}


def now_str() -> str:
    return datetime.now().strftime("%H:%M:%S")


def log(msg: str) -> None:
    print(f"[{now_str()}] {msg}", flush=True)


def to_decimal(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def parse_required_decimal(value: Any, field_name: str) -> Decimal:
    d = to_decimal(value)
    if d is None:
        raise RuntimeError(f"Bad decimal for {field_name}: {value}")
    return d


def parse_optional_decimal(value: Any, field_name: str) -> Optional[Decimal]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    d = to_decimal(s)
    if d is None:
        raise RuntimeError(f"Bad decimal for {field_name}: {value}")
    return d


def q2(value: Decimal) -> Decimal:
    return value.quantize(DECIMAL_STEP, rounding=ROUND_DOWN)


def format_price(value: Decimal) -> str:
    return format(q2(value), "f")


def parse_csv(value: str) -> List[str]:
    out: List[str] = []
    for item in value.split(","):
        s = item.strip()
        if s:
            out.append(s)
    return out


def normalize_str_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        if "," in value:
            return parse_csv(value)
        s = value.strip()
        return [s] if s else []
    if isinstance(value, list):
        out: List[str] = []
        for item in value:
            s = str(item).strip()
            if s:
                out.append(s)
        return out
    return []


def as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    s = str(value).strip().lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return default


def read_auth(auth_file: str) -> str:
    env_auth = os.getenv("PORTAL_AUTH", "").strip()
    if env_auth:
        return env_auth

    if os.path.exists(auth_file):
        with open(auth_file, "r", encoding="utf-8") as f:
            file_auth = f.read().strip()
            if file_auth:
                return file_auth

    raise RuntimeError(
        "Authorization not found. Set PORTAL_AUTH env var or put header value into auth.txt"
    )


@dataclass
class OfferPolicy:
    name: str
    offer_factor: Decimal
    max_listing_to_floor: Decimal
    min_offer: Decimal
    max_offer: Optional[Decimal]
    min_ask: Optional[Decimal]
    max_ask: Optional[Decimal]
    min_floor: Optional[Decimal]
    max_floor: Optional[Decimal]
    min_total_rarity_per_mille: Optional[Decimal]
    max_total_rarity_per_mille: Optional[Decimal]
    expiration_days: int
    skip_crafted: bool
    price_step: Decimal


@dataclass
class Config:
    api_base: str
    idle_poll_interval: float
    hot_poll_interval: float
    hot_cycles: int
    request_timeout: float
    search_limit: int
    warm_start: bool
    dry_run: bool
    seen_cache_size: int
    seen_break_streak: int
    max_new_per_cycle: int
    max_offers_per_cycle: int
    target_collections: List[str]
    log_skips: bool
    strategy_file: str
    base_policy: OfferPolicy


@dataclass
class StrategyRule:
    name: str
    enabled: bool
    collection_ids: Tuple[str, ...]
    name_contains: Tuple[str, ...]
    policy: OfferPolicy


@dataclass
class Decision:
    offer: Optional[Decimal]
    reason: Optional[str]
    ask: Optional[Decimal]
    floor: Optional[Decimal]
    total_rarity: Optional[Decimal]


class PortalClient:
    def __init__(self, api_base: str, auth_header: str, timeout: float = 6.0) -> None:
        self.api_base = api_base.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20, max_retries=0)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.session.headers.update(
            {
                "accept": "application/json, text/plain, */*",
                "authorization": auth_header,
                "origin": "https://portals.tg",
                "referer": "https://portals.tg/",
                "user-agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/144.0.0.0 Safari/537.36"
                ),
            }
        )

    def _request_id_headers(self) -> Dict[str, str]:
        return {"x-request-id": str(uuid.uuid4())}

    def _raise_for_error(self, response: requests.Response) -> None:
        if response.status_code < 400:
            return
        message = response.text
        try:
            data = response.json()
            if isinstance(data, dict) and data.get("message"):
                message = str(data["message"])
            else:
                message = json.dumps(data, ensure_ascii=False)
        except Exception:
            pass
        raise RuntimeError(f"HTTP {response.status_code}: {message}")

    def check_auth(self) -> Dict[str, Any]:
        url = f"{self.api_base}/nfts/search"
        params = {"limit": 1}
        response = self.session.get(
            url, params=params, headers=self._request_id_headers(), timeout=self.timeout
        )
        self._raise_for_error(response)
        return {"id": "unknown", "username": "sniper_user"}

    def fetch_latest_listings(self, limit: int) -> List[Dict[str, Any]]:
        url = f"{self.api_base}/nfts/search"
        params = {
            "offset": 0,
            "limit": limit,
            "sort_by": "listed_at desc",
            "search": "",
            "exclude_bundled": "true",
            "status": "listed",
        }
        response = self.session.get(
            url, params=params, headers=self._request_id_headers(), timeout=self.timeout
        )
        self._raise_for_error(response)
        payload = response.json()
        results = payload.get("results", [])
        if not isinstance(results, list):
            return []
        return results

    def place_offer(self, nft_id: str, offer_price: Decimal, expiration_days: int) -> Dict[str, Any]:
        url = f"{self.api_base}/offers/"
        payload = {
            "offer": {
                "nft_id": nft_id,
                "offer_price": format_price(offer_price),
                "expiration_days": expiration_days,
            }
        }
        response = self.session.post(
            url,
            json=payload,
            headers={
                **self._request_id_headers(),
                "content-type": "application/json",
            },
            timeout=self.timeout,
        )
        self._raise_for_error(response)
        try:
            return response.json()
        except Exception:
            return {"status": "ok", "raw": response.text}


class SeenCache:
    def __init__(self, max_size: int) -> None:
        self.max_size = max_size
        self._items: Dict[str, None] = {}

    def __contains__(self, key: str) -> bool:
        return key in self._items

    def add(self, key: str) -> None:
        if key in self._items:
            return
        self._items[key] = None
        if len(self._items) > self.max_size:
            oldest = next(iter(self._items))
            self._items.pop(oldest, None)

    def extend(self, keys: Iterable[str]) -> None:
        for key in keys:
            self.add(key)

    def __len__(self) -> int:
        return len(self._items)


class StrategyEngine:
    def __init__(self, global_policy: OfferPolicy, rules: List[StrategyRule]) -> None:
        self.global_policy = global_policy
        self.rules = rules

    def resolve(self, item: Dict[str, Any]) -> Tuple[OfferPolicy, Optional[str]]:
        item_collection = str(item.get("collection_id", "")).strip().lower()
        item_name = str(item.get("name", "")).strip().lower()

        for rule in self.rules:
            if not rule.enabled:
                continue
            if rule.collection_ids and item_collection not in rule.collection_ids:
                continue
            if rule.name_contains and not any(part in item_name for part in rule.name_contains):
                continue
            return rule.policy, rule.name
        return self.global_policy, None


def should_track_collection(item: Dict[str, Any], target_collections: List[str]) -> bool:
    if not target_collections:
        return True
    cid = str(item.get("collection_id", "")).lower()
    name = str(item.get("name", "")).lower()
    for needle in target_collections:
        n = needle.lower()
        if n in cid or n in name:
            return True
    return False


def total_rarity_per_mille(item: Dict[str, Any]) -> Optional[Decimal]:
    attrs = item.get("attributes")
    if not isinstance(attrs, list):
        return None
    total = Decimal("0")
    count = 0
    for attr in attrs:
        if not isinstance(attr, dict):
            continue
        rarity = to_decimal(attr.get("rarity_per_mille"))
        if rarity is None:
            continue
        total += rarity
        count += 1
    return total if count > 0 else None


def apply_policy_overrides(base: OfferPolicy, overrides: Dict[str, Any], source: str) -> OfferPolicy:
    policy = replace(base)

    for key, raw_value in overrides.items():
        if key not in POLICY_OVERRIDE_KEYS:
            continue
        if raw_value is None:
            continue

        if key == "offer_factor":
            policy.offer_factor = parse_required_decimal(raw_value, f"{source}.{key}")
        elif key == "max_listing_to_floor":
            policy.max_listing_to_floor = parse_required_decimal(raw_value, f"{source}.{key}")
        elif key == "min_offer":
            policy.min_offer = parse_required_decimal(raw_value, f"{source}.{key}")
        elif key == "max_offer":
            policy.max_offer = parse_optional_decimal(raw_value, f"{source}.{key}")
        elif key == "min_ask":
            policy.min_ask = parse_optional_decimal(raw_value, f"{source}.{key}")
        elif key == "max_ask":
            policy.max_ask = parse_optional_decimal(raw_value, f"{source}.{key}")
        elif key == "min_floor":
            policy.min_floor = parse_optional_decimal(raw_value, f"{source}.{key}")
        elif key == "max_floor":
            policy.max_floor = parse_optional_decimal(raw_value, f"{source}.{key}")
        elif key == "min_total_rarity_per_mille":
            policy.min_total_rarity_per_mille = parse_optional_decimal(raw_value, f"{source}.{key}")
        elif key == "max_total_rarity_per_mille":
            policy.max_total_rarity_per_mille = parse_optional_decimal(raw_value, f"{source}.{key}")
        elif key == "expiration_days":
            policy.expiration_days = int(raw_value)
        elif key == "skip_crafted":
            policy.skip_crafted = as_bool(raw_value, default=policy.skip_crafted)
        elif key == "price_step":
            policy.price_step = parse_required_decimal(raw_value, f"{source}.{key}")

    if policy.offer_factor <= 0:
        raise RuntimeError(f"{source}: offer_factor must be > 0")
    if policy.max_listing_to_floor <= 0:
        raise RuntimeError(f"{source}: max_listing_to_floor must be > 0")
    if policy.min_offer < 0:
        raise RuntimeError(f"{source}: min_offer must be >= 0")
    if policy.max_offer is not None and policy.max_offer <= 0:
        raise RuntimeError(f"{source}: max_offer must be > 0")
    if policy.price_step <= 0:
        raise RuntimeError(f"{source}: price_step must be > 0")
    if not (1 <= policy.expiration_days <= 30):
        raise RuntimeError(f"{source}: expiration_days must be in range [1..30]")

    if policy.min_ask is not None and policy.max_ask is not None and policy.min_ask > policy.max_ask:
        raise RuntimeError(f"{source}: min_ask cannot be greater than max_ask")
    if policy.min_floor is not None and policy.max_floor is not None and policy.min_floor > policy.max_floor:
        raise RuntimeError(f"{source}: min_floor cannot be greater than max_floor")
    if (
        policy.min_total_rarity_per_mille is not None
        and policy.max_total_rarity_per_mille is not None
        and policy.min_total_rarity_per_mille > policy.max_total_rarity_per_mille
    ):
        raise RuntimeError(
            f"{source}: min_total_rarity_per_mille cannot be greater than max_total_rarity_per_mille"
        )

    return policy


def collect_overrides(raw: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for section in ("offer", "filters"):
        sec = raw.get(section)
        if isinstance(sec, dict):
            out.update(sec)
    for key in POLICY_OVERRIDE_KEYS:
        if key in raw:
            out[key] = raw[key]
    return out


def build_strategy_engine(base_policy: OfferPolicy, strategy_file: str) -> Tuple[StrategyEngine, bool]:
    if not strategy_file or not os.path.exists(strategy_file):
        return StrategyEngine(base_policy, []), False

    try:
        with open(strategy_file, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception as e:
        raise RuntimeError(f"Failed to read strategy file '{strategy_file}': {e}") from e

    if not isinstance(raw, dict):
        raise RuntimeError("strategy file must contain a JSON object")

    global_overrides: Dict[str, Any] = {}
    for key in ("global_offer", "global_filters", "defaults"):
        sec = raw.get(key)
        if isinstance(sec, dict):
            global_overrides.update(sec)
    for key in POLICY_OVERRIDE_KEYS:
        if key in raw:
            global_overrides[key] = raw[key]

    global_policy = apply_policy_overrides(base_policy, global_overrides, "strategy.global")

    parsed_rules: List[StrategyRule] = []
    rules_raw = raw.get("rules", [])
    if isinstance(rules_raw, list):
        for idx, item in enumerate(rules_raw):
            if not isinstance(item, dict):
                continue

            name = str(item.get("name", f"rule_{idx + 1}")).strip() or f"rule_{idx + 1}"
            enabled = as_bool(item.get("enabled"), default=True)

            match = item.get("match")
            collection_ids: List[str] = []
            name_contains: List[str] = []
            if isinstance(match, dict):
                collection_ids.extend(normalize_str_list(match.get("collection_ids")))
                collection_ids.extend(normalize_str_list(match.get("collection_id")))
                name_contains.extend(normalize_str_list(match.get("name_contains")))
            collection_ids.extend(normalize_str_list(item.get("collection_ids")))
            collection_ids.extend(normalize_str_list(item.get("collection_id")))
            name_contains.extend(normalize_str_list(item.get("name_contains")))

            collection_ids_norm = tuple(sorted({x.lower() for x in collection_ids if x.strip()}))
            name_contains_norm = tuple(sorted({x.lower() for x in name_contains if x.strip()}))
            if not collection_ids_norm and not name_contains_norm:
                continue

            overrides = collect_overrides(item)
            policy = apply_policy_overrides(global_policy, overrides, f"strategy.rules[{name}]")
            parsed_rules.append(
                StrategyRule(
                    name=name,
                    enabled=enabled,
                    collection_ids=collection_ids_norm,
                    name_contains=name_contains_norm,
                    policy=policy,
                )
            )

    return StrategyEngine(global_policy, parsed_rules), True


def evaluate_offer(item: Dict[str, Any], policy: OfferPolicy) -> Decision:
    ask = to_decimal(item.get("price"))
    floor = to_decimal(item.get("floor_price")) or ask

    if ask is None or floor is None:
        return Decision(None, "missing_price_or_floor", ask, floor, None)
    if ask <= 0:
        return Decision(None, "ask_lte_zero", ask, floor, None)
    if floor <= 0:
        return Decision(None, "floor_lte_zero", ask, floor, None)

    if policy.skip_crafted and bool(item.get("is_crafted")):
        return Decision(None, "crafted", ask, floor, None)

    if policy.min_ask is not None and ask < policy.min_ask:
        return Decision(None, "ask_below_min_ask", ask, floor, None)
    if policy.max_ask is not None and ask > policy.max_ask:
        return Decision(None, "ask_above_max_ask", ask, floor, None)
    if policy.min_floor is not None and floor < policy.min_floor:
        return Decision(None, "floor_below_min_floor", ask, floor, None)
    if policy.max_floor is not None and floor > policy.max_floor:
        return Decision(None, "floor_above_max_floor", ask, floor, None)

    if ask > floor * policy.max_listing_to_floor:
        return Decision(None, "ask_too_far_from_floor", ask, floor, None)

    rarity_sum = total_rarity_per_mille(item)
    if policy.min_total_rarity_per_mille is not None:
        if rarity_sum is None or rarity_sum < policy.min_total_rarity_per_mille:
            return Decision(None, "rarity_sum_below_min", ask, floor, rarity_sum)
    if policy.max_total_rarity_per_mille is not None:
        if rarity_sum is None or rarity_sum > policy.max_total_rarity_per_mille:
            return Decision(None, "rarity_sum_above_max", ask, floor, rarity_sum)

    offer = q2(floor * policy.offer_factor)
    if policy.max_offer is not None and offer > policy.max_offer:
        offer = q2(policy.max_offer)

    max_allowed = q2(ask - policy.price_step)
    if max_allowed <= 0:
        return Decision(None, "max_allowed_lte_zero", ask, floor, rarity_sum)
    if offer > max_allowed:
        offer = max_allowed
    if offer < policy.min_offer:
        return Decision(None, "offer_below_min_offer", ask, floor, rarity_sum)
    if offer <= 0:
        return Decision(None, "offer_lte_zero", ask, floor, rarity_sum)

    return Decision(offer, None, ask, floor, rarity_sum)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Portal Market auto-offer sniper")
    parser.add_argument("--api-base", default=os.getenv("PORTAL_API_BASE", API_BASE_DEFAULT))
    parser.add_argument("--auth-file", default=os.getenv("AUTH_FILE", AUTH_FILE_DEFAULT))
    parser.add_argument(
        "--strategy-file",
        default=os.getenv("STRATEGY_FILE", STRATEGY_FILE_DEFAULT),
        help="JSON file with global/rule-based strategy settings",
    )

    parser.add_argument(
        "--poll-interval",
        dest="idle_poll_interval",
        type=float,
        default=float(os.getenv("POLL_INTERVAL", "0.8")),
        help="Idle polling interval (seconds)",
    )
    parser.add_argument(
        "--hot-poll-interval",
        type=float,
        default=float(os.getenv("HOT_POLL_INTERVAL", "0.20")),
        help="Polling interval after detecting new listings",
    )
    parser.add_argument(
        "--hot-cycles",
        type=int,
        default=int(os.getenv("HOT_CYCLES", "6")),
        help="How many fast cycles to keep after a hit",
    )
    parser.add_argument(
        "--request-timeout",
        type=float,
        default=float(os.getenv("REQUEST_TIMEOUT", "6.0")),
        help="HTTP timeout in seconds",
    )
    parser.add_argument("--limit", type=int, default=int(os.getenv("SEARCH_LIMIT", "40")))

    parser.add_argument(
        "--expiration-days",
        type=int,
        default=int(os.getenv("EXPIRATION_DAYS", "7")),
    )
    parser.add_argument(
        "--offer-factor",
        default=os.getenv("OFFER_FACTOR", "0.85"),
        help="Offer = floor_price * factor",
    )
    parser.add_argument(
        "--max-listing-to-floor",
        default=os.getenv("MAX_LISTING_TO_FLOOR", "1.40"),
        help="Ignore listing if ask > floor * value",
    )
    parser.add_argument(
        "--min-offer",
        default=os.getenv("MIN_OFFER", "0.10"),
        help="Minimum offer amount",
    )
    parser.add_argument(
        "--max-offer",
        default=os.getenv("MAX_OFFER", ""),
        help="Optional cap for offer amount",
    )
    parser.add_argument("--min-ask", default=os.getenv("MIN_ASK", ""))
    parser.add_argument("--max-ask", default=os.getenv("MAX_ASK", ""))
    parser.add_argument("--min-floor", default=os.getenv("MIN_FLOOR", ""))
    parser.add_argument("--max-floor", default=os.getenv("MAX_FLOOR", ""))
    parser.add_argument(
        "--min-total-rarity",
        default=os.getenv("MIN_TOTAL_RARITY_PER_MILLE", ""),
    )
    parser.add_argument(
        "--max-total-rarity",
        default=os.getenv("MAX_TOTAL_RARITY_PER_MILLE", ""),
    )
    parser.add_argument("--skip-crafted", action="store_true", help="Skip crafted NFTs")

    parser.add_argument(
        "--seen-cache-size",
        type=int,
        default=int(os.getenv("SEEN_CACHE_SIZE", "10000")),
    )
    parser.add_argument(
        "--seen-break-streak",
        type=int,
        default=int(os.getenv("SEEN_BREAK_STREAK", "2")),
        help="Break scan when this many seen IDs are met in a row",
    )
    parser.add_argument(
        "--max-new-per-cycle",
        type=int,
        default=int(os.getenv("MAX_NEW_PER_CYCLE", "30")),
        help="Process at most this many new NFTs in one loop",
    )
    parser.add_argument(
        "--max-offers-per-cycle",
        type=int,
        default=int(os.getenv("MAX_OFFERS_PER_CYCLE", "5")),
        help="Live mode guardrail: max offer attempts per cycle",
    )
    parser.add_argument(
        "--collections",
        default=os.getenv("TARGET_COLLECTIONS", ""),
        help="Comma-separated collection IDs or names to track. Empty = all.",
    )

    parser.add_argument(
        "--no-warm-start",
        action="store_true",
        help="Immediately process current listings (default: skip current and only track new ones).",
    )
    parser.add_argument("--live", action="store_true", help="Send real offers")
    parser.add_argument("--log-skips", action="store_true", help="Print rejected item reasons")
    return parser.parse_args()


def build_config(args: argparse.Namespace) -> Config:
    try:
        base_policy = OfferPolicy(
            name="cli",
            offer_factor=parse_required_decimal(args.offer_factor, "offer_factor"),
            max_listing_to_floor=parse_required_decimal(
                args.max_listing_to_floor, "max_listing_to_floor"
            ),
            min_offer=parse_required_decimal(args.min_offer, "min_offer"),
            max_offer=parse_optional_decimal(args.max_offer, "max_offer"),
            min_ask=parse_optional_decimal(args.min_ask, "min_ask"),
            max_ask=parse_optional_decimal(args.max_ask, "max_ask"),
            min_floor=parse_optional_decimal(args.min_floor, "min_floor"),
            max_floor=parse_optional_decimal(args.max_floor, "max_floor"),
            min_total_rarity_per_mille=parse_optional_decimal(
                args.min_total_rarity, "min_total_rarity"
            ),
            max_total_rarity_per_mille=parse_optional_decimal(
                args.max_total_rarity, "max_total_rarity"
            ),
            expiration_days=max(1, min(30, int(args.expiration_days))),
            skip_crafted=bool(args.skip_crafted),
            price_step=DECIMAL_STEP,
        )
        base_policy = apply_policy_overrides(base_policy, {}, "cli")
    except Exception as e:
        raise RuntimeError(f"Bad numeric config: {e}") from e

    return Config(
        api_base=args.api_base,
        idle_poll_interval=max(0.05, float(args.idle_poll_interval)),
        hot_poll_interval=max(0.05, float(args.hot_poll_interval)),
        hot_cycles=max(0, int(args.hot_cycles)),
        request_timeout=max(1.0, float(args.request_timeout)),
        search_limit=max(1, min(200, int(args.limit))),
        warm_start=not args.no_warm_start,
        dry_run=not args.live,
        seen_cache_size=max(100, int(args.seen_cache_size)),
        seen_break_streak=max(0, int(args.seen_break_streak)),
        max_new_per_cycle=max(1, int(args.max_new_per_cycle)),
        max_offers_per_cycle=max(1, int(args.max_offers_per_cycle)),
        target_collections=parse_csv(args.collections),
        log_skips=bool(args.log_skips),
        strategy_file=args.strategy_file,
        base_policy=base_policy,
    )


def main() -> int:
    args = parse_args()

    try:
        cfg = build_config(args)
    except Exception as e:
        log(f"CONFIG ERROR: {e}")
        return 1

    try:
        auth = read_auth(args.auth_file)
    except Exception as e:
        log(f"CONFIG ERROR: {e}")
        return 1

    try:
        engine, strategy_loaded = build_strategy_engine(cfg.base_policy, cfg.strategy_file)
    except Exception as e:
        log(f"STRATEGY ERROR: {e}")
        return 1

    client = PortalClient(cfg.api_base, auth, timeout=cfg.request_timeout)

    mode = "LIVE" if not cfg.dry_run else "DRY-RUN"
    log(f"Mode: {mode}")
    log(
        f"idle_poll={cfg.idle_poll_interval}s hot_poll={cfg.hot_poll_interval}s "
        f"hot_cycles={cfg.hot_cycles} limit={cfg.search_limit}"
    )

    gp = engine.global_policy
    log(
        f"Policy: factor={gp.offer_factor} max_listing_to_floor={gp.max_listing_to_floor} "
        f"min_offer={gp.min_offer} exp={gp.expiration_days}d"
    )
    log(f"Rules loaded: {len(engine.rules)} ({'strategy file' if strategy_loaded else 'none'})")

    if cfg.target_collections:
        log(f"Tracking collections: {', '.join(cfg.target_collections)}")

    try:
        me = client.check_auth()
        user_id = me.get("id", "?")
        username = me.get("username", "") or me.get("first_name", "")
        log(f"Auth OK: user={user_id} {username}")
    except Exception as e:
        log(f"AUTH CHECK FAILED: {e}")
        log("Tip: copy exact 'authorization' header value from a working request into auth.txt.")
        return 1

    seen = SeenCache(cfg.seen_cache_size)

    try:
        initial = client.fetch_latest_listings(cfg.search_limit)
    except Exception as e:
        log(f"Initial fetch failed: {e}")
        return 1

    if cfg.warm_start:
        seen.extend(str(x.get("id", "")) for x in initial if x.get("id"))
        log(f"Warm start: skipped {len(initial)} current listings")
    else:
        log(f"No warm start: will process {len(initial)} current listings as new")

    burst_left = 0

    while True:
        try:
            cycle_started = time.perf_counter()
            listings = client.fetch_latest_listings(cfg.search_limit)

            new_items: List[Dict[str, Any]] = []
            seen_streak = 0
            for item in listings:
                nft_id = str(item.get("id", "")).strip()
                if not nft_id:
                    continue
                if nft_id in seen:
                    seen_streak += 1
                    if cfg.seen_break_streak > 0 and seen_streak >= cfg.seen_break_streak:
                        break
                    continue

                seen_streak = 0
                seen.add(nft_id)
                new_items.append(item)
                if len(new_items) >= cfg.max_new_per_cycle:
                    break

            if new_items:
                offer_attempts = 0
                for item in new_items:
                    nft_id = str(item.get("id", "")).strip()
                    tg_id = str(item.get("tg_id", ""))
                    name = str(item.get("name", ""))

                    if not nft_id:
                        continue
                    if not should_track_collection(item, cfg.target_collections):
                        continue

                    policy, rule_name = engine.resolve(item)
                    decision = evaluate_offer(item, policy)
                    rule_suffix = f" rule={rule_name}" if rule_name else ""

                    if decision.offer is None:
                        if cfg.log_skips:
                            log(
                                f"SKIP {name} {tg_id}{rule_suffix} reason={decision.reason} "
                                f"ask={format_price(decision.ask or Decimal('0'))} "
                                f"floor={format_price(decision.floor or Decimal('0'))}"
                            )
                        continue

                    if cfg.dry_run:
                        log(
                            f"DRY  {name} {tg_id}{rule_suffix} "
                            f"ask={format_price(decision.ask or Decimal('0'))} "
                            f"floor={format_price(decision.floor or Decimal('0'))} "
                            f"offer={format_price(decision.offer)}"
                        )
                        continue

                    if offer_attempts >= cfg.max_offers_per_cycle:
                        log(f"Offer attempts cap reached ({cfg.max_offers_per_cycle})")
                        break

                    try:
                        offer_attempts += 1
                        result = client.place_offer(nft_id, decision.offer, policy.expiration_days)
                        message = ""
                        if isinstance(result, dict):
                            message = result.get("message", "") or result.get("status", "")
                        log(
                            f"OFFER SENT {name} {tg_id}{rule_suffix} "
                            f"offer={format_price(decision.offer)} exp={policy.expiration_days}d "
                            f"{message}".strip()
                        )
                    except Exception as e:
                        log(f"OFFER FAIL {name} {tg_id}{rule_suffix}: {e}")

            if new_items:
                burst_left = cfg.hot_cycles
            elif burst_left > 0:
                burst_left -= 1

            sleep_target = cfg.hot_poll_interval if burst_left > 0 else cfg.idle_poll_interval
            elapsed = time.perf_counter() - cycle_started
            sleep_for = sleep_target - elapsed
            if sleep_for > 0:
                time.sleep(sleep_for)

        except KeyboardInterrupt:
            log("Stopped by user")
            return 0
        except requests.RequestException as e:
            log(f"Network error: {e}")
            time.sleep(max(1.0, cfg.idle_poll_interval))
        except Exception as e:
            log(f"Loop error: {e}")
            time.sleep(max(1.0, cfg.idle_poll_interval))


if __name__ == "__main__":
    sys.exit(main())

