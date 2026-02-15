from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any, Dict, Iterable, Optional, Tuple

from .models import (
    InventoryGift,
    LiquiditySettings,
    MarketListing,
    OfferOrderRule,
    PRICE_STEP,
    RuleSelector,
    SellRule,
)


def to_decimal(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def q2(value: Decimal) -> Decimal:
    return value.quantize(PRICE_STEP, rounding=ROUND_DOWN)


def parse_unix_ts(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        ts = int(value)
        if ts > 10_000_000_000:
            return int(ts / 1000)
        return ts
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        ts = int(text)
        if ts > 10_000_000_000:
            return int(ts / 1000)
        return ts
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            dt = datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except ValueError:
            continue
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return int(dt.timestamp())
    except ValueError:
        return None


def now_ts() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp())


def _find_attr(attrs: Iterable[Dict[str, Any]], *keys: str) -> str:
    lowered = {k.lower() for k in keys}
    for attr in attrs:
        if not isinstance(attr, dict):
            continue
        title = str(attr.get("name") or attr.get("trait_type") or "").strip().lower()
        if title not in lowered:
            continue
        value = str(attr.get("value") or "").strip()
        if value:
            return value
    return ""


def extract_traits(item: Dict[str, Any]) -> Tuple[str, str]:
    attrs = item.get("attributes")
    if not isinstance(attrs, list):
        attrs = []

    model = (
        str(item.get("model") or item.get("gift_model") or "").strip()
        or _find_attr(attrs, "model", "gift model")
    )
    background = (
        str(item.get("background") or item.get("gift_background") or "").strip()
        or _find_attr(attrs, "background", "bg", "back")
    )
    return model, background


def parse_listing(item: Dict[str, Any]) -> MarketListing:
    nft_id = str(item.get("id") or item.get("nft_id") or "").strip()
    name = str(item.get("name") or item.get("gift_name") or "").strip()
    collection_id = str(item.get("collection_id") or "").strip()
    tg_id = str(item.get("tg_id") or "").strip()
    ask = to_decimal(item.get("price") or item.get("ask_price"))
    floor = to_decimal(item.get("floor_price") or item.get("collection_floor_price")) or ask
    model, background = extract_traits(item)
    listed_at = parse_unix_ts(item.get("listed_at") or item.get("created_at"))
    return MarketListing(
        nft_id=nft_id,
        name=name,
        collection_id=collection_id,
        tg_id=tg_id,
        ask_price=ask,
        floor_price=floor,
        listed_at_ts=listed_at,
        model=model,
        background=background,
        is_crafted=bool(item.get("is_crafted")),
        raw=item,
    )


def parse_inventory_item(item: Dict[str, Any]) -> InventoryGift:
    nft_id = str(item.get("id") or item.get("nft_id") or "").strip()
    name = str(item.get("name") or item.get("gift_name") or "").strip()
    collection_id = str(item.get("collection_id") or "").strip()
    model, background = extract_traits(item)
    listed = bool(item.get("is_listed") or item.get("listed"))
    return InventoryGift(
        nft_id=nft_id,
        name=name,
        collection_id=collection_id,
        model=model,
        background=background,
        listed=listed,
        raw=item,
    )


def selector_matches_listing(selector: RuleSelector, listing: MarketListing) -> bool:
    if not selector.matches_collection(listing.collection_id):
        return False
    if not selector.matches_name(listing.name):
        return False
    if not selector.matches_traits(listing.model, listing.background):
        return False
    if selector.only_recent_seconds and listing.listed_at_ts:
        if now_ts() - listing.listed_at_ts > selector.only_recent_seconds:
            return False
    return True


def selector_matches_inventory(selector: RuleSelector, gift: InventoryGift) -> bool:
    if not selector.matches_collection(gift.collection_id):
        return False
    if not selector.matches_name(gift.name):
        return False
    if not selector.matches_traits(gift.model, gift.background):
        return False
    return True


def pass_liquidity(
    *,
    listing: MarketListing,
    liquidity: LiquiditySettings,
    recent_sales_count: int,
    total_active_listings: int,
    last_sale_price: Optional[Decimal],
) -> bool:
    if not liquidity.enabled:
        return True
    if recent_sales_count < liquidity.min_recent_sales:
        return False
    if total_active_listings > 0:
        sell_through = Decimal(recent_sales_count) / Decimal(total_active_listings)
        if sell_through < liquidity.min_sell_through:
            return False
    if liquidity.max_floor_to_last_sale is not None and last_sale_price is not None:
        if listing.floor_price is not None and last_sale_price > 0:
            ratio = listing.floor_price / last_sale_price
            if ratio > liquidity.max_floor_to_last_sale:
                return False
    return True


def _apply_discount_bounds(
    *,
    price: Decimal,
    floor: Decimal,
    min_discount_pct: Optional[Decimal],
    max_discount_pct: Optional[Decimal],
) -> Decimal:
    out = price
    if min_discount_pct is not None:
        cap = floor * (Decimal("1") - (min_discount_pct / Decimal("100")))
        out = min(out, cap)
    if max_discount_pct is not None:
        min_price = floor * (Decimal("1") - (max_discount_pct / Decimal("100")))
        out = max(out, min_price)
    return out


def evaluate_offer_price(listing: MarketListing, rule: OfferOrderRule) -> Tuple[Optional[Decimal], str]:
    ask = listing.ask_price
    floor = listing.floor_price or ask
    if ask is None or floor is None:
        return None, "missing_prices"
    if ask <= 0 or floor <= 0:
        return None, "invalid_prices"
    if rule.skip_crafted and listing.is_crafted:
        return None, "crafted"
    if rule.min_ask is not None and ask < rule.min_ask:
        return None, "ask_below_min"
    if rule.max_ask is not None and ask > rule.max_ask:
        return None, "ask_above_max"
    if rule.min_floor is not None and floor < rule.min_floor:
        return None, "floor_below_min"
    if rule.max_floor is not None and floor > rule.max_floor:
        return None, "floor_above_max"
    if ask > floor * rule.max_listing_to_floor:
        return None, "ask_far_from_floor"

    candidate = q2(floor * rule.offer_factor)
    candidate = q2(
        _apply_discount_bounds(
            price=candidate,
            floor=floor,
            min_discount_pct=rule.min_discount_pct,
            max_discount_pct=rule.max_discount_pct,
        )
    )

    max_allowed = q2(ask - rule.outbid_step)
    if max_allowed <= 0:
        return None, "max_allowed_lte_zero"
    if candidate > max_allowed:
        candidate = max_allowed
    if rule.max_offer is not None and candidate > rule.max_offer:
        candidate = q2(rule.max_offer)
    if candidate < rule.min_offer:
        return None, "below_min_offer"
    if candidate <= 0:
        return None, "candidate_lte_zero"
    return candidate, "ok"


def evaluate_order_price(listing_floor: Optional[Decimal], rule: OfferOrderRule) -> Tuple[Optional[Decimal], str]:
    floor = listing_floor
    if floor is None or floor <= 0:
        return None, "missing_floor"
    if rule.min_floor is not None and floor < rule.min_floor:
        return None, "floor_below_min"
    if rule.max_floor is not None and floor > rule.max_floor:
        return None, "floor_above_max"

    candidate = q2(floor * rule.offer_factor)
    candidate = q2(
        _apply_discount_bounds(
            price=candidate,
            floor=floor,
            min_discount_pct=rule.min_discount_pct,
            max_discount_pct=rule.max_discount_pct,
        )
    )
    if rule.max_offer is not None and candidate > rule.max_offer:
        candidate = q2(rule.max_offer)
    if candidate < rule.min_offer:
        return None, "below_min_offer"
    if candidate <= 0:
        return None, "candidate_lte_zero"
    return candidate, "ok"


def compute_bump_price(
    *,
    own_price: Decimal,
    competitor_price: Optional[Decimal],
    step: Decimal,
    cap_price: Optional[Decimal],
) -> Optional[Decimal]:
    if competitor_price is None:
        return None
    if competitor_price < own_price:
        return None
    bumped = q2(competitor_price + step)
    if bumped <= own_price:
        return None
    if cap_price is not None and bumped > cap_price:
        return None
    return bumped


def compute_sell_price(
    *,
    floor_price: Optional[Decimal],
    buy_price: Optional[Decimal],
    sell_rule: SellRule,
) -> Tuple[Optional[Decimal], str]:
    if floor_price is None and buy_price is None:
        return None, "missing_floor_and_buy"

    if floor_price is not None and floor_price > 0:
        candidate = floor_price * (Decimal("1") + (sell_rule.markup_pct / Decimal("100")))
    else:
        candidate = buy_price or Decimal("0")
    candidate = q2(candidate)

    if sell_rule.min_sell_price is not None and candidate < sell_rule.min_sell_price:
        candidate = q2(sell_rule.min_sell_price)
    if sell_rule.max_sell_price is not None and candidate > sell_rule.max_sell_price:
        candidate = q2(sell_rule.max_sell_price)
    if candidate <= 0:
        return None, "candidate_lte_zero"
    return candidate, "ok"


def compute_reprice_below_floor(
    *,
    competitor_floor: Optional[Decimal],
    current_price: Decimal,
    step: Decimal,
    min_price: Optional[Decimal],
) -> Optional[Decimal]:
    if competitor_floor is None:
        return None
    target = q2(competitor_floor - step)
    if target <= 0:
        return None
    if target >= current_price:
        return None
    if min_price is not None and target < min_price:
        return None
    return target


def infer_remote_id(payload: Dict[str, Any], *keys: str) -> str:
    for key in keys:
        val = payload.get(key)
        if val is not None:
            text = str(val).strip()
            if text:
                return text
    for section_key in ("offer", "order", "listing", "result", "data"):
        sec = payload.get(section_key)
        if isinstance(sec, dict):
            for key in keys:
                val = sec.get(key)
                if val is not None:
                    text = str(val).strip()
                    if text:
                        return text
            v = sec.get("id")
            if v is not None:
                text = str(v).strip()
                if text:
                    return text
    return ""


def infer_competitor_price(item: Dict[str, Any], own_price_keys: Tuple[str, ...]) -> Optional[Decimal]:
    known = (
        "top_offer_price",
        "best_offer_price",
        "highest_offer_price",
        "top_order_price",
        "best_order_price",
        "highest_order_price",
        "best_bid",
    )
    for key in known:
        v = to_decimal(item.get(key))
        if v is not None:
            return v
    for own_key in own_price_keys:
        nested = item.get(own_key)
        if isinstance(nested, dict):
            for key in known:
                v = to_decimal(nested.get(key))
                if v is not None:
                    return v
    return None


def selector_to_order_payload(selector: RuleSelector) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    if selector.collection_ids:
        payload["collection_id"] = selector.collection_ids[0]
    if selector.gift_names:
        payload["gift_name"] = selector.gift_names[0]
    if selector.models:
        payload["model"] = selector.models[0]
    if selector.backgrounds:
        payload["background"] = selector.backgrounds[0]
    return payload

