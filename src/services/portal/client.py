from __future__ import annotations

import json
import uuid
from decimal import Decimal
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter

from .models import ApiRoutes


def format_price(value: Decimal) -> str:
    return format(value.quantize(Decimal("0.01")), "f")


class PortalClient:
    def __init__(
        self,
        *,
        api_base: str,
        auth_header: str,
        routes: ApiRoutes,
        timeout: float = 6.0,
    ) -> None:
        self.api_base = api_base.rstrip("/")
        self.routes = routes
        self.timeout = timeout
        self.session = requests.Session()
        adapter = HTTPAdapter(pool_connections=30, pool_maxsize=30, max_retries=0)
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

    def _path(self, path: str, **kwargs: str) -> str:
        rendered = path.format(**kwargs)
        if rendered.startswith("http://") or rendered.startswith("https://"):
            return rendered
        if not rendered.startswith("/"):
            rendered = "/" + rendered
        return f"{self.api_base}{rendered}"

    def _request_id_headers(self) -> Dict[str, str]:
        return {"x-request-id": str(uuid.uuid4())}

    def _raise_for_error(self, response: requests.Response) -> None:
        if response.status_code < 400:
            return
        message = response.text
        try:
            payload = response.json()
            if isinstance(payload, dict) and payload.get("message"):
                message = str(payload["message"])
            else:
                message = json.dumps(payload, ensure_ascii=False)
        except Exception:
            pass
        raise RuntimeError(f"HTTP {response.status_code}: {message}")

    def _json_or_text(self, response: requests.Response) -> Any:
        try:
            return response.json()
        except Exception:
            return {"raw": response.text}

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        response = self.session.get(
            self._path(path),
            params=params or {},
            headers=self._request_id_headers(),
            timeout=self.timeout,
        )
        self._raise_for_error(response)
        return self._json_or_text(response)

    def _post(self, path: str, payload: Dict[str, Any]) -> Any:
        response = self.session.post(
            self._path(path),
            json=payload,
            headers={**self._request_id_headers(), "content-type": "application/json"},
            timeout=self.timeout,
        )
        self._raise_for_error(response)
        return self._json_or_text(response)

    def _patch(self, path: str, payload: Dict[str, Any]) -> Any:
        response = self.session.patch(
            self._path(path),
            json=payload,
            headers={**self._request_id_headers(), "content-type": "application/json"},
            timeout=self.timeout,
        )
        self._raise_for_error(response)
        return self._json_or_text(response)

    def _delete(self, path: str) -> Any:
        response = self.session.delete(
            self._path(path),
            headers=self._request_id_headers(),
            timeout=self.timeout,
        )
        self._raise_for_error(response)
        return self._json_or_text(response)

    def check_auth(self) -> Dict[str, Any]:
        payload = self._get(self.routes.search_listings, {"limit": 1})
        _ = payload
        return {"id": "unknown", "username": "portal_user"}

    def fetch_latest_listings(self, limit: int) -> List[Dict[str, Any]]:
        payload = self._get(
            self.routes.search_listings,
            {
                "offset": 0,
                "limit": max(1, limit),
                "sort_by": "listed_at desc",
                "search": "",
                "exclude_bundled": "true",
                "status": "listed",
            },
        )
        if isinstance(payload, dict):
            results = payload.get("results")
            if isinstance(results, list):
                return [x for x in results if isinstance(x, dict)]
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]
        return []

    def fetch_recent_sales(
        self,
        *,
        collection_id: str = "",
        model: str = "",
        background: str = "",
        limit: int = 30,
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {"limit": max(1, limit)}
        if collection_id:
            params["collection_id"] = collection_id
        if model:
            params["model"] = model
        if background:
            params["background"] = background
        payload = self._get(self.routes.recent_sales, params)
        if isinstance(payload, dict):
            results = payload.get("results")
            if isinstance(results, list):
                return [x for x in results if isinstance(x, dict)]
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]
        return []

    def fetch_my_offers(self, limit: int = 200) -> List[Dict[str, Any]]:
        payload = self._get(self.routes.my_offers, {"limit": max(1, limit)})
        if isinstance(payload, dict):
            results = payload.get("results")
            if isinstance(results, list):
                return [x for x in results if isinstance(x, dict)]
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]
        return []

    def fetch_my_orders(self, limit: int = 200) -> List[Dict[str, Any]]:
        payload = self._get(self.routes.my_orders, {"limit": max(1, limit)})
        if isinstance(payload, dict):
            results = payload.get("results")
            if isinstance(results, list):
                return [x for x in results if isinstance(x, dict)]
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]
        return []

    def fetch_my_inventory(self, limit: int = 200) -> List[Dict[str, Any]]:
        payload = self._get(self.routes.inventory, {"limit": max(1, limit), "status": "owned"})
        if isinstance(payload, dict):
            results = payload.get("results")
            if isinstance(results, list):
                return [x for x in results if isinstance(x, dict)]
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]
        return []

    def fetch_my_listings(self, limit: int = 200) -> List[Dict[str, Any]]:
        payload = self._get(self.routes.my_listings, {"limit": max(1, limit), "status": "listed"})
        if isinstance(payload, dict):
            results = payload.get("results")
            if isinstance(results, list):
                return [x for x in results if isinstance(x, dict)]
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]
        return []

    def fetch_activity(self, limit: int = 120) -> List[Dict[str, Any]]:
        payload = self._get(self.routes.activity, {"limit": max(1, limit)})
        if isinstance(payload, dict):
            results = payload.get("results")
            if isinstance(results, list):
                return [x for x in results if isinstance(x, dict)]
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]
        return []

    def place_offer(self, nft_id: str, offer_price: Decimal, expiration_days: int) -> Dict[str, Any]:
        payload = {
            "offer": {
                "nft_id": nft_id,
                "offer_price": format_price(offer_price),
                "expiration_days": max(1, min(30, int(expiration_days))),
            }
        }
        response = self._post(self.routes.create_offer, payload)
        return response if isinstance(response, dict) else {"response": response}

    def cancel_offer(self, offer_id: str) -> Dict[str, Any]:
        response = self._delete(self.routes.cancel_offer.format(offer_id=offer_id))
        return response if isinstance(response, dict) else {"response": response}

    def place_order(
        self,
        *,
        selector_payload: Dict[str, Any],
        order_price: Decimal,
        expiration_days: int,
    ) -> Dict[str, Any]:
        payload = {
            "order": {
                **selector_payload,
                "order_price": format_price(order_price),
                "expiration_days": max(1, min(30, int(expiration_days))),
            }
        }
        response = self._post(self.routes.create_order, payload)
        return response if isinstance(response, dict) else {"response": response}

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        response = self._delete(self.routes.cancel_order.format(order_id=order_id))
        return response if isinstance(response, dict) else {"response": response}

    def create_listing(self, nft_id: str, price: Decimal, expiration_days: int) -> Dict[str, Any]:
        payload = {
            "listing": {
                "nft_id": nft_id,
                "price": format_price(price),
                "expiration_days": max(1, min(30, int(expiration_days))),
            }
        }
        response = self._post(self.routes.create_listing, payload)
        return response if isinstance(response, dict) else {"response": response}

    def update_listing(self, listing_id: str, price: Decimal) -> Dict[str, Any]:
        payload = {"listing": {"price": format_price(price)}}
        response = self._patch(self.routes.update_listing.format(listing_id=listing_id), payload)
        return response if isinstance(response, dict) else {"response": response}

    def cancel_listing(self, listing_id: str) -> Dict[str, Any]:
        response = self._delete(self.routes.cancel_listing.format(listing_id=listing_id))
        return response if isinstance(response, dict) else {"response": response}

