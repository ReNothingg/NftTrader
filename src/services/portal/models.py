from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, Optional, Tuple


PRICE_STEP = Decimal("0.01")


@dataclass(frozen=True)
class RuleSelector:
    collection_ids: Tuple[str, ...] = ()
    gift_names: Tuple[str, ...] = ()
    name_contains: Tuple[str, ...] = ()
    models: Tuple[str, ...] = ()
    backgrounds: Tuple[str, ...] = ()
    only_recent_seconds: Optional[int] = None

    def fingerprint(self) -> str:
        return "|".join(
            [
                ",".join(self.collection_ids),
                ",".join(self.gift_names),
                ",".join(self.name_contains),
                ",".join(self.models),
                ",".join(self.backgrounds),
                str(self.only_recent_seconds or 0),
            ]
        )

    def _match_text(self, value: str, allowed: Tuple[str, ...]) -> bool:
        if not allowed:
            return True
        x = value.strip().lower()
        return x in allowed

    def matches_name(self, name: str) -> bool:
        n = name.strip().lower()
        if self.gift_names and n not in self.gift_names:
            return False
        if self.name_contains and not any(part in n for part in self.name_contains):
            return False
        return True

    def matches_collection(self, collection_id: str) -> bool:
        if not self.collection_ids:
            return True
        return collection_id.strip().lower() in self.collection_ids

    def matches_traits(self, model: str, background: str) -> bool:
        if self.models and model.strip().lower() not in self.models:
            return False
        if self.backgrounds and background.strip().lower() not in self.backgrounds:
            return False
        return True


@dataclass
class OfferOrderRule:
    name: str
    enabled: bool = True
    mode: str = "offer"  # "offer" | "order"
    selector: RuleSelector = field(default_factory=RuleSelector)
    offer_factor: Decimal = Decimal("0.85")
    min_offer: Decimal = Decimal("0.10")
    max_offer: Optional[Decimal] = None
    min_ask: Optional[Decimal] = None
    max_ask: Optional[Decimal] = None
    min_floor: Optional[Decimal] = None
    max_floor: Optional[Decimal] = None
    max_listing_to_floor: Decimal = Decimal("1.25")
    min_discount_pct: Optional[Decimal] = None
    max_discount_pct: Optional[Decimal] = None
    outbid_step: Decimal = PRICE_STEP
    bump_if_outbid: bool = True
    skip_crafted: bool = True
    expiration_days: int = 7
    expiration_seconds: Optional[int] = None
    max_actions_per_cycle: int = 4


@dataclass
class SellRule:
    name: str
    enabled: bool = True
    selector: RuleSelector = field(default_factory=RuleSelector)
    markup_pct: Decimal = Decimal("0")
    floor_undercut_step: Decimal = PRICE_STEP
    min_sell_price: Optional[Decimal] = None
    max_sell_price: Optional[Decimal] = None
    auto_reprice_below_floor: bool = True
    reprice_step: Decimal = PRICE_STEP
    expiration_days: int = 7
    expiration_seconds: Optional[int] = None


@dataclass
class LiquiditySettings:
    enabled: bool = True
    min_recent_sales: int = 2
    min_sell_through: Decimal = Decimal("0.02")
    max_floor_to_last_sale: Optional[Decimal] = Decimal("1.8")


@dataclass
class RuntimeSettings:
    dry_run: bool = True
    idle_poll_interval: float = 0.9
    hot_poll_interval: float = 0.25
    hot_cycles: int = 6
    request_timeout: float = 6.0
    search_limit: int = 60
    warm_start: bool = True
    seen_cache_size: int = 10000
    seen_break_streak: int = 2
    max_new_per_cycle: int = 40
    max_offers_per_cycle: int = 8
    activity_poll_every_sec: float = 20.0
    inventory_poll_every_sec: float = 15.0
    orders_poll_every_sec: float = 12.0
    listings_poll_every_sec: float = 12.0


@dataclass
class ApiRoutes:
    search_listings: str = "/nfts/search"
    create_offer: str = "/offers/"
    my_offers: str = "/offers/my"
    cancel_offer: str = "/offers/{offer_id}"
    create_order: str = "/orders/"
    my_orders: str = "/orders/my"
    cancel_order: str = "/orders/{order_id}"
    inventory: str = "/users/me/nfts"
    create_listing: str = "/listings/"
    my_listings: str = "/listings/my"
    update_listing: str = "/listings/{listing_id}"
    cancel_listing: str = "/listings/{listing_id}"
    recent_sales: str = "/sales/recent"
    activity: str = "/activity/me"


@dataclass
class AccountConfig:
    name: str
    auth: str


@dataclass
class TelegramSettings:
    enabled: bool = False
    token: str = ""
    chat_ids: Tuple[int, ...] = ()


@dataclass
class AppConfig:
    api_base: str
    routes: ApiRoutes
    accounts: Tuple[AccountConfig, ...]
    runtime: RuntimeSettings
    liquidity: LiquiditySettings
    offer_rules: Tuple[OfferOrderRule, ...]
    order_rules: Tuple[OfferOrderRule, ...]
    sell_rules: Tuple[SellRule, ...]
    state_db_path: str
    telegram: TelegramSettings
    strategy_file: str


@dataclass
class MarketListing:
    nft_id: str
    name: str
    collection_id: str
    tg_id: str
    ask_price: Optional[Decimal]
    floor_price: Optional[Decimal]
    listed_at_ts: Optional[int]
    model: str
    background: str
    is_crafted: bool
    raw: Dict[str, Any]


@dataclass
class InventoryGift:
    nft_id: str
    name: str
    collection_id: str
    model: str
    background: str
    listed: bool
    raw: Dict[str, Any]


@dataclass
class ManagedAction:
    key: str
    kind: str  # "offer" | "order" | "listing"
    rule_name: str
    remote_id: Optional[str]
    nft_id: Optional[str]
    selector_key: str
    price: Decimal
    cap_price: Optional[Decimal]
    created_ts: int
    expires_ts: Optional[int]
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TradeEvent:
    account: str
    event_id: str
    kind: str  # "buy" | "sell"
    nft_id: str
    gift_name: str
    model: str
    background: str
    price: Decimal
    fee: Decimal
    ts: int
    raw: Dict[str, Any]
