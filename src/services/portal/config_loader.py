from __future__ import annotations

import json
import os
from dataclasses import replace
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .models import (
    ApiRoutes,
    AppConfig,
    AccountConfig,
    LiquiditySettings,
    OfferOrderRule,
    RuleSelector,
    RuntimeSettings,
    SellRule,
    TelegramSettings,
)


API_BASE_DEFAULT = "https://portal-market.com/api"
AUTH_FILE_DEFAULT = "auth.txt"
STRATEGY_FILE_DEFAULT = "src/services/portal/config/strategy.json"
ACCOUNTS_FILE_DEFAULT = "configs/portal_accounts.json"
STATE_DB_DEFAULT = "data/portal_trader.db"


def _to_decimal(value: Any, field_name: str) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise RuntimeError(f"Bad decimal for {field_name}: {value}") from exc


def _to_optional_decimal(value: Any, field_name: str) -> Optional[Decimal]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return _to_decimal(text, field_name)


def _to_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _normalize_list(value: Any) -> Tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        parts = [x.strip().lower() for x in value.split(",") if x.strip()]
        return tuple(sorted(set(parts)))
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        out: List[str] = []
        for item in value:
            text = str(item).strip().lower()
            if text:
                out.append(text)
        return tuple(sorted(set(out)))
    return ()


def _read_json(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    with p.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, dict):
        raise RuntimeError(f"JSON root must be object: {path}")
    return payload


def _resolve_auth(auth_file: str) -> str:
    env = os.getenv("PORTAL_AUTH", "").strip()
    if env:
        return env
    p = Path(auth_file)
    if p.exists():
        val = p.read_text(encoding="utf-8").strip()
        if val:
            return val
    raise RuntimeError("Authorization not found: set PORTAL_AUTH or fill auth.txt")


def _parse_selector(raw: Dict[str, Any]) -> RuleSelector:
    match = raw.get("match")
    m = match if isinstance(match, dict) else {}
    return RuleSelector(
        collection_ids=_normalize_list(m.get("collection_ids") or raw.get("collection_ids")),
        gift_names=_normalize_list(m.get("gift_names") or raw.get("gift_names")),
        name_contains=_normalize_list(m.get("name_contains") or raw.get("name_contains")),
        models=_normalize_list(m.get("models") or raw.get("models")),
        backgrounds=_normalize_list(m.get("backgrounds") or raw.get("backgrounds")),
        only_recent_seconds=(
            int(m.get("only_recent_seconds") or raw.get("only_recent_seconds") or 0) or None
        ),
    )


def _parse_offer_rule(raw: Dict[str, Any], name_fallback: str, mode: str) -> OfferOrderRule:
    name = str(raw.get("name", name_fallback)).strip() or name_fallback
    offer = raw.get("offer")
    filters = raw.get("filters")
    offer_data = offer if isinstance(offer, dict) else {}
    filter_data = filters if isinstance(filters, dict) else {}

    merged: Dict[str, Any] = {}
    merged.update(filter_data)
    merged.update(offer_data)
    merged.update(raw)
    merged.pop("offer", None)
    merged.pop("filters", None)
    merged.pop("match", None)

    return OfferOrderRule(
        name=name,
        enabled=_to_bool(raw.get("enabled"), True),
        mode=str(raw.get("mode", mode)).strip().lower() or mode,
        selector=_parse_selector(raw),
        offer_factor=_to_decimal(merged.get("offer_factor", "0.85"), f"{name}.offer_factor"),
        min_offer=_to_decimal(merged.get("min_offer", "0.10"), f"{name}.min_offer"),
        max_offer=_to_optional_decimal(merged.get("max_offer"), f"{name}.max_offer"),
        min_ask=_to_optional_decimal(merged.get("min_ask"), f"{name}.min_ask"),
        max_ask=_to_optional_decimal(merged.get("max_ask"), f"{name}.max_ask"),
        min_floor=_to_optional_decimal(merged.get("min_floor"), f"{name}.min_floor"),
        max_floor=_to_optional_decimal(merged.get("max_floor"), f"{name}.max_floor"),
        max_listing_to_floor=_to_decimal(
            merged.get("max_listing_to_floor", "1.25"),
            f"{name}.max_listing_to_floor",
        ),
        min_discount_pct=_to_optional_decimal(
            merged.get("min_discount_pct"), f"{name}.min_discount_pct"
        ),
        max_discount_pct=_to_optional_decimal(
            merged.get("max_discount_pct"), f"{name}.max_discount_pct"
        ),
        outbid_step=_to_decimal(merged.get("outbid_step", "0.01"), f"{name}.outbid_step"),
        bump_if_outbid=_to_bool(merged.get("bump_if_outbid"), True),
        skip_crafted=_to_bool(merged.get("skip_crafted"), True),
        expiration_days=max(1, min(30, int(merged.get("expiration_days", 7)))),
        expiration_seconds=(
            int(merged.get("expiration_seconds")) if merged.get("expiration_seconds") else None
        ),
        max_actions_per_cycle=max(1, int(merged.get("max_actions_per_cycle", 4))),
    )


def _parse_sell_rule(raw: Dict[str, Any], name_fallback: str) -> SellRule:
    name = str(raw.get("name", name_fallback)).strip() or name_fallback
    return SellRule(
        name=name,
        enabled=_to_bool(raw.get("enabled"), True),
        selector=_parse_selector(raw),
        markup_pct=_to_decimal(raw.get("markup_pct", "0"), f"{name}.markup_pct"),
        floor_undercut_step=_to_decimal(
            raw.get("floor_undercut_step", "0.01"), f"{name}.floor_undercut_step"
        ),
        min_sell_price=_to_optional_decimal(raw.get("min_sell_price"), f"{name}.min_sell_price"),
        max_sell_price=_to_optional_decimal(raw.get("max_sell_price"), f"{name}.max_sell_price"),
        auto_reprice_below_floor=_to_bool(raw.get("auto_reprice_below_floor"), True),
        reprice_step=_to_decimal(raw.get("reprice_step", "0.01"), f"{name}.reprice_step"),
        expiration_days=max(1, min(30, int(raw.get("expiration_days", 7)))),
        expiration_seconds=(
            int(raw.get("expiration_seconds")) if raw.get("expiration_seconds") else None
        ),
    )


def _parse_runtime(raw: Dict[str, Any]) -> RuntimeSettings:
    base = RuntimeSettings()
    runtime_raw = raw.get("runtime")
    if not isinstance(runtime_raw, dict):
        runtime_raw = {}

    return replace(
        base,
        dry_run=_to_bool(runtime_raw.get("dry_run"), base.dry_run),
        idle_poll_interval=max(0.05, float(runtime_raw.get("idle_poll_interval", base.idle_poll_interval))),
        hot_poll_interval=max(0.05, float(runtime_raw.get("hot_poll_interval", base.hot_poll_interval))),
        hot_cycles=max(0, int(runtime_raw.get("hot_cycles", base.hot_cycles))),
        request_timeout=max(1.0, float(runtime_raw.get("request_timeout", base.request_timeout))),
        search_limit=max(1, min(200, int(runtime_raw.get("search_limit", base.search_limit)))),
        warm_start=_to_bool(runtime_raw.get("warm_start"), base.warm_start),
        seen_cache_size=max(100, int(runtime_raw.get("seen_cache_size", base.seen_cache_size))),
        seen_break_streak=max(0, int(runtime_raw.get("seen_break_streak", base.seen_break_streak))),
        max_new_per_cycle=max(1, int(runtime_raw.get("max_new_per_cycle", base.max_new_per_cycle))),
        max_offers_per_cycle=max(1, int(runtime_raw.get("max_offers_per_cycle", base.max_offers_per_cycle))),
        activity_poll_every_sec=max(
            3.0,
            float(runtime_raw.get("activity_poll_every_sec", base.activity_poll_every_sec)),
        ),
        inventory_poll_every_sec=max(
            3.0,
            float(runtime_raw.get("inventory_poll_every_sec", base.inventory_poll_every_sec)),
        ),
        orders_poll_every_sec=max(
            3.0,
            float(runtime_raw.get("orders_poll_every_sec", base.orders_poll_every_sec)),
        ),
        listings_poll_every_sec=max(
            3.0,
            float(runtime_raw.get("listings_poll_every_sec", base.listings_poll_every_sec)),
        ),
    )


def _parse_liquidity(raw: Dict[str, Any]) -> LiquiditySettings:
    base = LiquiditySettings()
    liq_raw = raw.get("liquidity")
    if not isinstance(liq_raw, dict):
        liq_raw = {}
    return replace(
        base,
        enabled=_to_bool(liq_raw.get("enabled"), base.enabled),
        min_recent_sales=max(0, int(liq_raw.get("min_recent_sales", base.min_recent_sales))),
        min_sell_through=_to_decimal(liq_raw.get("min_sell_through", base.min_sell_through), "liquidity.min_sell_through"),
        max_floor_to_last_sale=_to_optional_decimal(
            liq_raw.get("max_floor_to_last_sale"), "liquidity.max_floor_to_last_sale"
        )
        if "max_floor_to_last_sale" in liq_raw
        else base.max_floor_to_last_sale,
    )


def _parse_routes(raw: Dict[str, Any]) -> ApiRoutes:
    base = ApiRoutes()
    api_raw = raw.get("api")
    if not isinstance(api_raw, dict):
        api_raw = {}
    routes_raw = api_raw.get("routes")
    if not isinstance(routes_raw, dict):
        routes_raw = {}
    return ApiRoutes(
        search_listings=str(routes_raw.get("search_listings", base.search_listings)),
        create_offer=str(routes_raw.get("create_offer", base.create_offer)),
        my_offers=str(routes_raw.get("my_offers", base.my_offers)),
        cancel_offer=str(routes_raw.get("cancel_offer", base.cancel_offer)),
        create_order=str(routes_raw.get("create_order", base.create_order)),
        my_orders=str(routes_raw.get("my_orders", base.my_orders)),
        cancel_order=str(routes_raw.get("cancel_order", base.cancel_order)),
        inventory=str(routes_raw.get("inventory", base.inventory)),
        create_listing=str(routes_raw.get("create_listing", base.create_listing)),
        my_listings=str(routes_raw.get("my_listings", base.my_listings)),
        update_listing=str(routes_raw.get("update_listing", base.update_listing)),
        cancel_listing=str(routes_raw.get("cancel_listing", base.cancel_listing)),
        recent_sales=str(routes_raw.get("recent_sales", base.recent_sales)),
        activity=str(routes_raw.get("activity", base.activity)),
    )


def _parse_telegram(raw: Dict[str, Any]) -> TelegramSettings:
    tg_raw = raw.get("telegram")
    if not isinstance(tg_raw, dict):
        tg_raw = {}

    token = str(tg_raw.get("token") or os.getenv("TELEGRAM_BOT_TOKEN", "")).strip()
    chat_ids_raw = tg_raw.get("chat_ids")
    if chat_ids_raw is None:
        chat_ids_raw = os.getenv("TELEGRAM_CHAT_IDS", "")
    chat_ids: List[int] = []
    if isinstance(chat_ids_raw, str):
        for part in chat_ids_raw.split(","):
            part = part.strip()
            if part:
                try:
                    chat_ids.append(int(part))
                except ValueError:
                    continue
    elif isinstance(chat_ids_raw, list):
        for item in chat_ids_raw:
            try:
                chat_ids.append(int(item))
            except (TypeError, ValueError):
                continue

    enabled_raw = tg_raw.get("enabled")
    if enabled_raw is None:
        enabled_raw = os.getenv("TELEGRAM_ENABLED")
    enabled = _to_bool(enabled_raw, False) and bool(token)
    return TelegramSettings(enabled=enabled, token=token, chat_ids=tuple(sorted(set(chat_ids))))


def _parse_accounts(accounts_file: str, auth_file: str) -> Tuple[AccountConfig, ...]:
    payload = _read_json(accounts_file)
    raw_accounts = payload.get("accounts")

    parsed: List[AccountConfig] = []
    if isinstance(raw_accounts, list):
        for idx, item in enumerate(raw_accounts):
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", f"account_{idx + 1}")).strip() or f"account_{idx + 1}"
            auth = str(item.get("auth", "")).strip()
            auth_env = str(item.get("auth_env", "")).strip()
            if not auth and auth_env:
                auth = os.getenv(auth_env, "").strip()
            if not auth:
                continue
            parsed.append(AccountConfig(name=name, auth=auth))

    if parsed:
        return tuple(parsed)

    return (AccountConfig(name="main", auth=_resolve_auth(auth_file)),)


def _legacy_strategy_bridge(raw: Dict[str, Any]) -> Dict[str, Any]:
    # - global_offer/global_filters + rules[]
    if "offer_rules" in raw or "order_rules" in raw or "sell_rules" in raw:
        return raw

    bridged = dict(raw)
    defaults: Dict[str, Any] = {}
    for key in ("global_offer", "global_filters", "defaults"):
        sec = raw.get(key)
        if isinstance(sec, dict):
            defaults.update(sec)

    offer_rules: List[Dict[str, Any]] = []
    for idx, item in enumerate(raw.get("rules", []) if isinstance(raw.get("rules"), list) else []):
        if not isinstance(item, dict):
            continue
        merged = {"name": item.get("name", f"rule_{idx + 1}")}
        merged.update(defaults)
        merged.update(item)
        offer_rules.append(merged)

    if not offer_rules and defaults:
        offer_rules.append({"name": "default_offer_rule", **defaults})

    bridged["offer_rules"] = offer_rules
    bridged.setdefault("order_rules", [])
    bridged.setdefault("sell_rules", [])
    return bridged


def load_app_config(
    *,
    strategy_file: str,
    accounts_file: str,
    auth_file: str,
    api_base: str,
    state_db_path: str,
    live_mode: bool,
    no_warm_start: bool,
) -> AppConfig:
    raw = _legacy_strategy_bridge(_read_json(strategy_file))
    runtime = _parse_runtime(raw)
    if live_mode:
        runtime = replace(runtime, dry_run=False)
    if no_warm_start:
        runtime = replace(runtime, warm_start=False)

    offer_rules_raw = raw.get("offer_rules", [])
    if not isinstance(offer_rules_raw, list):
        offer_rules_raw = []
    order_rules_raw = raw.get("order_rules", [])
    if not isinstance(order_rules_raw, list):
        order_rules_raw = []
    sell_rules_raw = raw.get("sell_rules", [])
    if not isinstance(sell_rules_raw, list):
        sell_rules_raw = []

    offer_rules = tuple(
        _parse_offer_rule(item, f"offer_rule_{idx + 1}", mode="offer")
        for idx, item in enumerate(offer_rules_raw)
        if isinstance(item, dict)
    )
    order_rules = tuple(
        _parse_offer_rule(item, f"order_rule_{idx + 1}", mode="order")
        for idx, item in enumerate(order_rules_raw)
        if isinstance(item, dict)
    )
    sell_rules = tuple(
        _parse_sell_rule(item, f"sell_rule_{idx + 1}")
        for idx, item in enumerate(sell_rules_raw)
        if isinstance(item, dict)
    )

    api_raw = raw.get("api")
    api_section = api_raw if isinstance(api_raw, dict) else {}
    base_from_file = str(api_section.get("base", "")).strip()
    final_base = api_base.strip() or base_from_file or API_BASE_DEFAULT

    accounts = _parse_accounts(accounts_file, auth_file)
    routes = _parse_routes(raw)
    liquidity = _parse_liquidity(raw)
    telegram = _parse_telegram(raw)

    if not offer_rules:
        offer_rules = (
            OfferOrderRule(
                name="default_offer_rule",
                enabled=True,
                mode="offer",
                selector=RuleSelector(),
            ),
        )

    return AppConfig(
        api_base=final_base,
        routes=routes,
        accounts=accounts,
        runtime=runtime,
        liquidity=liquidity,
        offer_rules=offer_rules,
        order_rules=order_rules,
        sell_rules=sell_rules,
        state_db_path=state_db_path or STATE_DB_DEFAULT,
        telegram=telegram,
        strategy_file=strategy_file,
    )
