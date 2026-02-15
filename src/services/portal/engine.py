from __future__ import annotations

import asyncio
import time
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

from .client import PortalClient
from .models import AppConfig, ManagedAction, MarketListing, OfferOrderRule, SellRule, TradeEvent
from .storage import TradeLedger
from .strategy import (
    compute_bump_price,
    compute_reprice_below_floor,
    compute_sell_price,
    evaluate_offer_price,
    evaluate_order_price,
    infer_competitor_price,
    infer_remote_id,
    now_ts,
    parse_inventory_item,
    parse_listing,
    parse_unix_ts,
    pass_liquidity,
    q2,
    selector_matches_inventory,
    selector_matches_listing,
    selector_to_order_payload,
    to_decimal,
)
from .telegram_bot import TelegramSupervisor


def now_str() -> str:
    return datetime.now().strftime("%H:%M:%S")


def log(scope: str, message: str) -> None:
    print(f"[{now_str()}] [{scope}] {message}", flush=True)


def _first_decimal(data: Dict[str, Any], keys: Iterable[str]) -> Optional[Decimal]:
    for key in keys:
        d = to_decimal(data.get(key))
        if d is not None:
            return d
    return None


class AccountWorker:
    def __init__(
        self,
        *,
        app_config: AppConfig,
        account_name: str,
        auth_header: str,
        ledger: TradeLedger,
        notifier: TelegramSupervisor,
    ) -> None:
        self.app_config = app_config
        self.account_name = account_name
        self.ledger = ledger
        self.notifier = notifier
        self.client = PortalClient(
            api_base=app_config.api_base,
            auth_header=auth_header,
            routes=app_config.routes,
            timeout=app_config.runtime.request_timeout,
        )

        self.runtime = app_config.runtime
        self._seen: Dict[str, None] = {}
        self._actions: Dict[str, ManagedAction] = {}
        self._liquidity_cache: Dict[str, Tuple[int, int, Optional[Decimal], float]] = {}
        self._last_activity_poll = 0.0
        self._last_inventory_poll = 0.0
        self._last_orders_poll = 0.0
        self._last_listings_poll = 0.0
        self._burst_left = 0
        self._status = "booting"

    @property
    def status(self) -> str:
        return self._status

    async def _call(self, fn, *args, **kwargs):
        return await asyncio.to_thread(fn, *args, **kwargs)

    def _seen_add(self, nft_id: str) -> None:
        if nft_id in self._seen:
            return
        self._seen[nft_id] = None
        if len(self._seen) > self.runtime.seen_cache_size:
            oldest = next(iter(self._seen))
            self._seen.pop(oldest, None)

    def _build_offer_action_key(self, nft_id: str, rule: OfferOrderRule) -> str:
        return f"offer:{nft_id}:{rule.name}"

    def _build_order_action_key(self, rule: OfferOrderRule) -> str:
        return f"order:{rule.name}:{rule.selector.fingerprint()}"

    def _build_listing_action_key(self, nft_id: str, rule: SellRule) -> str:
        return f"listing:{nft_id}:{rule.name}"

    def _trait_key(self, collection_id: str, model: str, background: str) -> str:
        return "|".join(
            [
                collection_id.strip().lower(),
                model.strip().lower(),
                background.strip().lower(),
            ]
        )

    def _find_rule(self, name: str) -> Optional[OfferOrderRule]:
        for rule in (*self.app_config.offer_rules, *self.app_config.order_rules):
            if rule.name == name:
                return rule
        return None

    async def _place_offer(
        self,
        *,
        listing: MarketListing,
        rule: OfferOrderRule,
        offer_price: Decimal,
        cap_price: Optional[Decimal],
    ) -> None:
        key = self._build_offer_action_key(listing.nft_id, rule)
        if key in self._actions:
            return

        expires_ts = None
        if rule.expiration_seconds:
            expires_ts = now_ts() + int(rule.expiration_seconds)

        if self.runtime.dry_run:
            log(
                self.account_name,
                (
                    f"DRY OFFER {listing.name} {listing.tg_id} "
                    f"rule={rule.name} price={offer_price}"
                ),
            )
            self._actions[key] = ManagedAction(
                key=key,
                kind="offer",
                rule_name=rule.name,
                remote_id=f"dry-{key}",
                nft_id=listing.nft_id,
                selector_key=rule.selector.fingerprint(),
                price=offer_price,
                cap_price=cap_price,
                created_ts=now_ts(),
                expires_ts=expires_ts,
                extra={"tg_id": listing.tg_id},
            )
            return

        payload = await self._call(
            self.client.place_offer,
            listing.nft_id,
            offer_price,
            rule.expiration_days,
        )
        remote_id = infer_remote_id(payload, "id", "offer_id")
        self._actions[key] = ManagedAction(
            key=key,
            kind="offer",
            rule_name=rule.name,
            remote_id=remote_id or None,
            nft_id=listing.nft_id,
            selector_key=rule.selector.fingerprint(),
            price=offer_price,
            cap_price=cap_price,
            created_ts=now_ts(),
            expires_ts=expires_ts,
            extra={"tg_id": listing.tg_id},
        )
        log(
            self.account_name,
            (
                f"OFFER SENT {listing.name} {listing.tg_id} "
                f"rule={rule.name} price={offer_price}"
            ),
        )

    async def _cancel_action(self, action: ManagedAction) -> None:
        if self.runtime.dry_run:
            log(self.account_name, f"DRY CANCEL {action.kind} key={action.key}")
            self._actions.pop(action.key, None)
            return
        remote_id = action.remote_id or ""
        if not remote_id:
            self._actions.pop(action.key, None)
            return
        if action.kind == "offer":
            await self._call(self.client.cancel_offer, remote_id)
        elif action.kind == "order":
            await self._call(self.client.cancel_order, remote_id)
        elif action.kind == "listing":
            await self._call(self.client.cancel_listing, remote_id)
        self._actions.pop(action.key, None)
        log(self.account_name, f"CANCELLED {action.kind} key={action.key}")

    def _build_floor_index(
        self, listings: List[MarketListing]
    ) -> Tuple[
        Dict[str, Decimal],
        Dict[str, int],
        Dict[str, Decimal],
    ]:
        floor_by_traits: Dict[str, Decimal] = {}
        count_by_traits: Dict[str, int] = {}
        floor_by_nft: Dict[str, Decimal] = {}

        for listing in listings:
            if listing.floor_price is not None and listing.floor_price > 0:
                floor_by_nft[listing.nft_id] = listing.floor_price
            key = self._trait_key(listing.collection_id, listing.model, listing.background)
            count_by_traits[key] = count_by_traits.get(key, 0) + 1
            if listing.ask_price is None or listing.ask_price <= 0:
                continue
            prev = floor_by_traits.get(key)
            if prev is None or listing.ask_price < prev:
                floor_by_traits[key] = listing.ask_price
        return floor_by_traits, count_by_traits, floor_by_nft

    async def _get_liquidity_snapshot(
        self,
        listing: MarketListing,
        active_count: int,
    ) -> Tuple[int, Optional[Decimal]]:
        key = self._trait_key(listing.collection_id, listing.model, listing.background)
        cached = self._liquidity_cache.get(key)
        now_monotonic = time.monotonic()
        if cached and now_monotonic - cached[3] < 45:
            return cached[0], cached[2]

        recent = await self._call(
            self.client.fetch_recent_sales,
            collection_id=listing.collection_id,
            model=listing.model,
            background=listing.background,
            limit=20,
        )
        recent_count = len(recent)
        last_sale = None
        if recent:
            first = recent[0]
            if isinstance(first, dict):
                last_sale = _first_decimal(first, ("price", "sale_price", "amount"))

        self._liquidity_cache[key] = (recent_count, active_count, last_sale, now_monotonic)
        return recent_count, last_sale

    async def _process_new_listings(self, new_listings: List[MarketListing], active_count_index: Dict[str, int]) -> int:
        offer_actions = 0
        for listing in new_listings:
            if offer_actions >= self.runtime.max_offers_per_cycle:
                break
            for rule in self.app_config.offer_rules:
                if not rule.enabled or rule.mode != "offer":
                    continue
                if not selector_matches_listing(rule.selector, listing):
                    continue

                price, _reason = evaluate_offer_price(listing, rule)
                if price is None:
                    continue

                key = self._trait_key(listing.collection_id, listing.model, listing.background)
                active_count = active_count_index.get(key, 0)
                recent_sales_count, last_sale = await self._get_liquidity_snapshot(listing, active_count)
                if not pass_liquidity(
                    listing=listing,
                    liquidity=self.app_config.liquidity,
                    recent_sales_count=recent_sales_count,
                    total_active_listings=max(1, active_count),
                    last_sale_price=last_sale,
                ):
                    continue

                cap_price = listing.ask_price
                if cap_price is not None:
                    cap_price = q2(cap_price - rule.outbid_step)
                if rule.max_offer is not None:
                    cap_price = min(cap_price, rule.max_offer) if cap_price is not None else rule.max_offer

                try:
                    await self._place_offer(
                        listing=listing,
                        rule=rule,
                        offer_price=price,
                        cap_price=cap_price,
                    )
                    offer_actions += 1
                    break
                except Exception as exc:
                    log(
                        self.account_name,
                        f"OFFER FAIL rule={rule.name} nft={listing.nft_id}: {exc}",
                    )
        return offer_actions

    def _floor_for_rule(self, rule: OfferOrderRule, listings: List[MarketListing]) -> Optional[Decimal]:
        floor: Optional[Decimal] = None
        for listing in listings:
            if not selector_matches_listing(rule.selector, listing):
                continue
            candidate = listing.floor_price or listing.ask_price
            if candidate is None or candidate <= 0:
                continue
            if floor is None or candidate < floor:
                floor = candidate
        return floor

    async def _place_or_refresh_orders(self, listings: List[MarketListing]) -> None:
        for rule in self.app_config.order_rules:
            if not rule.enabled:
                continue
            floor = self._floor_for_rule(rule, listings)
            order_price, _reason = evaluate_order_price(floor, rule)
            if order_price is None:
                continue

            key = self._build_order_action_key(rule)
            action = self._actions.get(key)
            cap_price = floor
            if cap_price is not None and rule.max_offer is not None:
                cap_price = min(cap_price, rule.max_offer)

            if action is not None:
                if action.price >= order_price:
                    continue
                try:
                    await self._replace_order(action=action, rule=rule, new_price=order_price, cap_price=cap_price)
                except Exception as exc:
                    log(self.account_name, f"ORDER UPDATE FAIL rule={rule.name}: {exc}")
                continue

            try:
                await self._create_order(rule=rule, price=order_price, cap_price=cap_price)
            except Exception as exc:
                log(self.account_name, f"ORDER FAIL rule={rule.name}: {exc}")

    async def _create_order(
        self,
        *,
        rule: OfferOrderRule,
        price: Decimal,
        cap_price: Optional[Decimal],
    ) -> None:
        key = self._build_order_action_key(rule)
        selector_payload = selector_to_order_payload(rule.selector)
        expires_ts = None
        if rule.expiration_seconds:
            expires_ts = now_ts() + int(rule.expiration_seconds)

        if self.runtime.dry_run:
            log(
                self.account_name,
                f"DRY ORDER rule={rule.name} price={price} selector={selector_payload}",
            )
            self._actions[key] = ManagedAction(
                key=key,
                kind="order",
                rule_name=rule.name,
                remote_id=f"dry-{key}",
                nft_id=None,
                selector_key=rule.selector.fingerprint(),
                price=price,
                cap_price=cap_price,
                created_ts=now_ts(),
                expires_ts=expires_ts,
                extra=selector_payload,
            )
            return

        payload = await self._call(
            self.client.place_order,
            selector_payload=selector_payload,
            order_price=price,
            expiration_days=rule.expiration_days,
        )
        remote_id = infer_remote_id(payload, "id", "order_id")
        self._actions[key] = ManagedAction(
            key=key,
            kind="order",
            rule_name=rule.name,
            remote_id=remote_id or None,
            nft_id=None,
            selector_key=rule.selector.fingerprint(),
            price=price,
            cap_price=cap_price,
            created_ts=now_ts(),
            expires_ts=expires_ts,
            extra=selector_payload,
        )
        log(self.account_name, f"ORDER PLACED rule={rule.name} price={price}")

    async def _replace_order(
        self,
        *,
        action: ManagedAction,
        rule: OfferOrderRule,
        new_price: Decimal,
        cap_price: Optional[Decimal],
    ) -> None:
        if self.runtime.dry_run:
            log(
                self.account_name,
                f"DRY ORDER UPDATE rule={rule.name} from={action.price} to={new_price}",
            )
            action.price = new_price
            action.cap_price = cap_price
            self._actions[action.key] = action
            return

        if action.remote_id:
            await self._call(self.client.cancel_order, action.remote_id)
        self._actions.pop(action.key, None)
        await self._create_order(rule=rule, price=new_price, cap_price=cap_price)

    async def _auto_cancel_expired(self) -> None:
        ts = now_ts()
        keys = [
            key
            for key, action in self._actions.items()
            if action.expires_ts is not None and ts >= action.expires_ts
        ]
        for key in keys:
            action = self._actions.get(key)
            if not action:
                continue
            try:
                await self._cancel_action(action)
            except Exception as exc:
                log(self.account_name, f"CANCEL FAIL key={key}: {exc}")

    async def _sync_offer_outbids(self) -> None:
        now_monotonic = time.monotonic()
        if now_monotonic - self._last_orders_poll < self.runtime.orders_poll_every_sec:
            return
        self._last_orders_poll = now_monotonic

        try:
            my_offers = await self._call(self.client.fetch_my_offers, self.runtime.search_limit)
        except Exception as exc:
            log(self.account_name, f"MY OFFERS FAIL: {exc}")
            return

        offers_by_nft: Dict[str, Dict[str, Any]] = {}
        for item in my_offers:
            if not isinstance(item, dict):
                continue
            nft_id = str(item.get("nft_id") or item.get("id") or "").strip()
            if nft_id:
                offers_by_nft[nft_id] = item

        for key, action in list(self._actions.items()):
            if action.kind != "offer":
                continue
            rule = self._find_rule(action.rule_name)
            if rule is None or not rule.bump_if_outbid:
                continue
            nft_id = action.nft_id or ""
            raw = offers_by_nft.get(nft_id)
            if raw is None:
                continue
            if not action.remote_id:
                action.remote_id = infer_remote_id(raw, "id", "offer_id")

            own_price = _first_decimal(raw, ("offer_price", "price")) or action.price
            competitor = infer_competitor_price(raw, own_price_keys=("nft", "item"))
            target = compute_bump_price(
                own_price=own_price,
                competitor_price=competitor,
                step=rule.outbid_step,
                cap_price=action.cap_price,
            )
            if target is None:
                continue

            if self.runtime.dry_run:
                log(self.account_name, f"DRY OUTBID OFFER nft={nft_id} {own_price}->{target}")
                action.price = target
                self._actions[key] = action
                continue

            if not action.remote_id:
                continue
            try:
                await self._call(self.client.cancel_offer, action.remote_id)
                payload = await self._call(
                    self.client.place_offer,
                    nft_id,
                    target,
                    rule.expiration_days,
                )
                action.remote_id = infer_remote_id(payload, "id", "offer_id") or action.remote_id
                action.price = target
                action.created_ts = now_ts()
                self._actions[key] = action
                log(self.account_name, f"OFFER BUMPED nft={nft_id} to={target}")
            except Exception as exc:
                log(self.account_name, f"OFFER BUMP FAIL nft={nft_id}: {exc}")

    async def _sync_order_outbids(self) -> None:
        try:
            my_orders = await self._call(self.client.fetch_my_orders, self.runtime.search_limit)
        except Exception as exc:
            log(self.account_name, f"MY ORDERS FAIL: {exc}")
            return

        for key, action in list(self._actions.items()):
            if action.kind != "order":
                continue
            rule = self._find_rule(action.rule_name)
            if rule is None or not rule.bump_if_outbid:
                continue

            matched_raw = None
            for item in my_orders:
                if not isinstance(item, dict):
                    continue
                rid = str(item.get("id") or item.get("order_id") or "").strip()
                if action.remote_id and rid and rid == action.remote_id:
                    matched_raw = item
                    break
            if matched_raw is None:
                continue

            own_price = _first_decimal(matched_raw, ("order_price", "price")) or action.price
            competitor = infer_competitor_price(matched_raw, own_price_keys=("target", "market"))
            target = compute_bump_price(
                own_price=own_price,
                competitor_price=competitor,
                step=rule.outbid_step,
                cap_price=action.cap_price,
            )
            if target is None:
                continue

            try:
                await self._replace_order(
                    action=action,
                    rule=rule,
                    new_price=target,
                    cap_price=action.cap_price,
                )
                log(self.account_name, f"ORDER BUMPED rule={rule.name} to={target}")
            except Exception as exc:
                log(self.account_name, f"ORDER BUMP FAIL rule={rule.name}: {exc}")

    async def _auto_sell(
        self,
        floor_by_traits: Dict[str, Decimal],
    ) -> None:
        now_monotonic = time.monotonic()
        if now_monotonic - self._last_inventory_poll < self.runtime.inventory_poll_every_sec:
            return
        self._last_inventory_poll = now_monotonic

        try:
            inventory_raw = await self._call(self.client.fetch_my_inventory, self.runtime.search_limit)
        except Exception as exc:
            log(self.account_name, f"INVENTORY FAIL: {exc}")
            return

        listings_raw: List[Dict[str, Any]] = []
        try:
            listings_raw = await self._call(self.client.fetch_my_listings, self.runtime.search_limit)
        except Exception as exc:
            log(self.account_name, f"MY LISTINGS FAIL: {exc}")
        listed_ids = {
            str(item.get("nft_id") or item.get("id") or "").strip()
            for item in listings_raw
            if isinstance(item, dict)
        }

        for raw in inventory_raw:
            gift = parse_inventory_item(raw)
            if not gift.nft_id or gift.nft_id in listed_ids or gift.listed:
                continue

            matched_rule = None
            for rule in self.app_config.sell_rules:
                if not rule.enabled:
                    continue
                if selector_matches_inventory(rule.selector, gift):
                    matched_rule = rule
                    break
            if matched_rule is None:
                continue

            key = self._trait_key(gift.collection_id, gift.model, gift.background)
            floor = floor_by_traits.get(key)
            buy_price = self.ledger.get_buy_price(self.account_name, gift.nft_id)
            price, _reason = compute_sell_price(
                floor_price=floor,
                buy_price=buy_price,
                sell_rule=matched_rule,
            )
            if price is None:
                continue

            action_key = self._build_listing_action_key(gift.nft_id, matched_rule)
            if action_key in self._actions:
                continue

            expires_ts = None
            if matched_rule.expiration_seconds:
                expires_ts = now_ts() + int(matched_rule.expiration_seconds)

            if self.runtime.dry_run:
                log(
                    self.account_name,
                    (
                        f"DRY SELL {gift.name} model={gift.model} background={gift.background} "
                        f"rule={matched_rule.name} price={price}"
                    ),
                )
                self._actions[action_key] = ManagedAction(
                    key=action_key,
                    kind="listing",
                    rule_name=matched_rule.name,
                    remote_id=f"dry-{action_key}",
                    nft_id=gift.nft_id,
                    selector_key=matched_rule.selector.fingerprint(),
                    price=price,
                    cap_price=matched_rule.max_sell_price,
                    created_ts=now_ts(),
                    expires_ts=expires_ts,
                    extra={},
                )
                continue

            try:
                payload = await self._call(
                    self.client.create_listing,
                    gift.nft_id,
                    price,
                    matched_rule.expiration_days,
                )
                remote_id = infer_remote_id(payload, "id", "listing_id")
                self._actions[action_key] = ManagedAction(
                    key=action_key,
                    kind="listing",
                    rule_name=matched_rule.name,
                    remote_id=remote_id or None,
                    nft_id=gift.nft_id,
                    selector_key=matched_rule.selector.fingerprint(),
                    price=price,
                    cap_price=matched_rule.max_sell_price,
                    created_ts=now_ts(),
                    expires_ts=expires_ts,
                    extra={},
                )
                log(
                    self.account_name,
                    (
                        f"SELL LISTED {gift.name} model={gift.model} background={gift.background} "
                        f"rule={matched_rule.name} price={price}"
                    ),
                )
            except Exception as exc:
                log(self.account_name, f"SELL FAIL nft={gift.nft_id} rule={matched_rule.name}: {exc}")

    async def _reprice_listings(self, floor_by_traits: Dict[str, Decimal]) -> None:
        now_monotonic = time.monotonic()
        if now_monotonic - self._last_listings_poll < self.runtime.listings_poll_every_sec:
            return
        self._last_listings_poll = now_monotonic

        try:
            my_listings = await self._call(self.client.fetch_my_listings, self.runtime.search_limit)
        except Exception as exc:
            log(self.account_name, f"MY LISTINGS FAIL: {exc}")
            return

        for raw in my_listings:
            if not isinstance(raw, dict):
                continue
            nft_id = str(raw.get("nft_id") or raw.get("id") or "").strip()
            listing_id = str(raw.get("listing_id") or raw.get("id") or "").strip()
            price = _first_decimal(raw, ("price", "ask_price"))
            if not nft_id or not listing_id or price is None:
                continue

            gift = parse_inventory_item(raw)
            matched_rule = None
            for rule in self.app_config.sell_rules:
                if not rule.enabled:
                    continue
                if selector_matches_inventory(rule.selector, gift):
                    matched_rule = rule
                    break
            if matched_rule is None or not matched_rule.auto_reprice_below_floor:
                continue

            key = self._trait_key(gift.collection_id, gift.model, gift.background)
            competitor_floor = floor_by_traits.get(key)
            buy_price = self.ledger.get_buy_price(self.account_name, nft_id)
            min_price = matched_rule.min_sell_price
            if buy_price is not None:
                min_by_markup = q2(buy_price * (Decimal("1") + matched_rule.markup_pct / Decimal("100")))
                min_price = max(min_price, min_by_markup) if min_price is not None else min_by_markup
            target = compute_reprice_below_floor(
                competitor_floor=competitor_floor,
                current_price=price,
                step=matched_rule.reprice_step,
                min_price=min_price,
            )
            if target is None:
                continue

            if self.runtime.dry_run:
                log(
                    self.account_name,
                    f"DRY REPRICE listing={listing_id} nft={nft_id} from={price} to={target}",
                )
                continue

            try:
                await self._call(self.client.update_listing, listing_id, target)
                log(
                    self.account_name,
                    f"REPRICE listing={listing_id} nft={nft_id} from={price} to={target}",
                )
            except Exception as exc:
                log(self.account_name, f"REPRICE FAIL listing={listing_id}: {exc}")

    def _extract_trade_events(self, activity_rows: List[Dict[str, Any]]) -> List[TradeEvent]:
        out: List[TradeEvent] = []
        for row in activity_rows:
            if not isinstance(row, dict):
                continue
            raw_kind = str(row.get("type") or row.get("event_type") or row.get("kind") or "").strip().lower()
            if "buy" in raw_kind or "purchase" in raw_kind:
                kind = "buy"
            elif "sell" in raw_kind:
                kind = "sell"
            else:
                continue

            event_id = str(row.get("id") or row.get("event_id") or row.get("tx_id") or "").strip()
            if not event_id:
                continue

            nft_sec = row.get("nft")
            nft = nft_sec if isinstance(nft_sec, dict) else {}
            nft_id = str(row.get("nft_id") or nft.get("id") or row.get("id_nft") or "").strip()
            if not nft_id:
                continue

            name = str(row.get("name") or nft.get("name") or row.get("gift_name") or "").strip()
            model = str(row.get("model") or nft.get("model") or "").strip()
            background = str(row.get("background") or nft.get("background") or "").strip()
            price = _first_decimal(row, ("price", "amount", "sale_price", "total_price"))
            if price is None:
                continue
            fee = _first_decimal(row, ("fee", "commission", "market_fee")) or Decimal("0")
            ts = parse_unix_ts(row.get("created_at") or row.get("timestamp") or row.get("date")) or now_ts()

            out.append(
                TradeEvent(
                    account=self.account_name,
                    event_id=event_id,
                    kind=kind,
                    nft_id=nft_id,
                    gift_name=name,
                    model=model,
                    background=background,
                    price=price,
                    fee=fee,
                    ts=ts,
                    raw=row,
                )
            )
        return out

    async def _ingest_activity(self) -> None:
        now_monotonic = time.monotonic()
        if now_monotonic - self._last_activity_poll < self.runtime.activity_poll_every_sec:
            return
        self._last_activity_poll = now_monotonic

        try:
            activity = await self._call(self.client.fetch_activity, self.runtime.search_limit)
        except Exception as exc:
            log(self.account_name, f"ACTIVITY FAIL: {exc}")
            return

        events = self._extract_trade_events(activity)
        for event in events:
            created = self.ledger.record_trade(event)
            if not created:
                continue
            msg = (
                f"{self.account_name}: {event.kind.upper()} {event.gift_name} "
                f"{event.model}/{event.background} {event.price}"
            )
            await self.notifier.notify(msg)
            log(self.account_name, msg)

    async def run(self) -> None:
        self._status = "auth"
        try:
            await self._call(self.client.check_auth)
        except Exception as exc:
            self._status = f"auth_fail:{exc}"
            log(self.account_name, f"AUTH FAIL: {exc}")
            return

        self._status = "warm_start"
        try:
            initial_raw = await self._call(self.client.fetch_latest_listings, self.runtime.search_limit)
        except Exception as exc:
            self._status = f"initial_fail:{exc}"
            log(self.account_name, f"INITIAL FETCH FAIL: {exc}")
            return

        if self.runtime.warm_start:
            for raw in initial_raw:
                listing = parse_listing(raw)
                if listing.nft_id:
                    self._seen_add(listing.nft_id)
            log(self.account_name, f"Warm start: skipped {len(initial_raw)} items")
        else:
            log(self.account_name, "Warm start disabled: process current items")

        self._status = "running"
        while True:
            cycle_started = time.perf_counter()
            try:
                listings_raw = await self._call(
                    self.client.fetch_latest_listings,
                    self.runtime.search_limit,
                )
                listings = [parse_listing(x) for x in listings_raw if isinstance(x, dict)]
                listings = [x for x in listings if x.nft_id]
                floor_by_traits, active_count_index, _ = self._build_floor_index(listings)

                new_listings: List[MarketListing] = []
                seen_streak = 0
                for listing in listings:
                    if listing.nft_id in self._seen:
                        seen_streak += 1
                        if (
                            self.runtime.seen_break_streak > 0
                            and seen_streak >= self.runtime.seen_break_streak
                        ):
                            break
                        continue
                    seen_streak = 0
                    self._seen_add(listing.nft_id)
                    new_listings.append(listing)
                    if len(new_listings) >= self.runtime.max_new_per_cycle:
                        break

                offers = 0
                if new_listings:
                    offers = await self._process_new_listings(new_listings, active_count_index)

                await self._place_or_refresh_orders(listings)
                await self._sync_offer_outbids()
                await self._sync_order_outbids()
                await self._auto_cancel_expired()
                await self._auto_sell(floor_by_traits)
                await self._reprice_listings(floor_by_traits)
                await self._ingest_activity()

                if new_listings or offers > 0:
                    self._burst_left = self.runtime.hot_cycles
                elif self._burst_left > 0:
                    self._burst_left -= 1

                sleep_target = (
                    self.runtime.hot_poll_interval
                    if self._burst_left > 0
                    else self.runtime.idle_poll_interval
                )
                elapsed = time.perf_counter() - cycle_started
                wait_for = sleep_target - elapsed
                if wait_for > 0:
                    await asyncio.sleep(wait_for)

                self._status = (
                    f"running seen={len(self._seen)} actions={len(self._actions)} burst={self._burst_left}"
                )
            except requests.RequestException as exc:
                self._status = f"net_err:{exc}"
                log(self.account_name, f"NETWORK ERROR: {exc}")
                await asyncio.sleep(max(1.0, self.runtime.idle_poll_interval))
            except asyncio.CancelledError:
                self._status = "stopped"
                raise
            except Exception as exc:
                self._status = f"loop_err:{exc}"
                log(self.account_name, f"LOOP ERROR: {exc}")
                await asyncio.sleep(max(1.0, self.runtime.idle_poll_interval))


class PortalEngine:
    def __init__(self, app_config: AppConfig) -> None:
        self.app_config = app_config
        self.ledger = TradeLedger(app_config.state_db_path)
        self._workers: Dict[str, AccountWorker] = {}
        self.telegram = TelegramSupervisor(
            settings=app_config.telegram,
            ledger=self.ledger,
            workers_snapshot=self.workers_snapshot,
            logger=lambda msg: log("telegram", msg),
        )

    def workers_snapshot(self) -> Dict[str, str]:
        return {name: worker.status for name, worker in self._workers.items()}

    async def run(self) -> int:
        await self.telegram.start()

        tasks: List[asyncio.Task[None]] = []
        for account in self.app_config.accounts:
            worker = AccountWorker(
                app_config=self.app_config,
                account_name=account.name,
                auth_header=account.auth,
                ledger=self.ledger,
                notifier=self.telegram,
            )
            self._workers[account.name] = worker
            tasks.append(asyncio.create_task(worker.run(), name=f"worker:{account.name}"))

        if not tasks:
            log("engine", "No accounts configured")
            await self.telegram.stop()
            return 1

        try:
            await asyncio.gather(*tasks)
            return 0
        except asyncio.CancelledError:
            for task in tasks:
                task.cancel()
            raise
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await self.telegram.stop()
