from __future__ import annotations

import json
import sqlite3
import threading
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .models import TradeEvent


@dataclass
class ProfitStats:
    buy_count: int
    sell_count: int
    total_buy: Decimal
    total_sell: Decimal
    total_fee: Decimal
    net_profit: Decimal
    realized_profit: Decimal


class TradeLedger:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._lock = threading.Lock()
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                    account TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    nft_id TEXT NOT NULL,
                    gift_name TEXT NOT NULL,
                    model TEXT NOT NULL,
                    background TEXT NOT NULL,
                    price TEXT NOT NULL,
                    fee TEXT NOT NULL,
                    ts INTEGER NOT NULL,
                    payload_json TEXT NOT NULL,
                    PRIMARY KEY(account, event_id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS positions (
                    account TEXT NOT NULL,
                    nft_id TEXT NOT NULL,
                    gift_name TEXT NOT NULL,
                    model TEXT NOT NULL,
                    background TEXT NOT NULL,
                    buy_price TEXT NOT NULL DEFAULT '0',
                    buy_ts INTEGER NOT NULL DEFAULT 0,
                    sell_price TEXT NOT NULL DEFAULT '0',
                    sell_ts INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL,
                    PRIMARY KEY(account, nft_id)
                )
                """
            )
            conn.commit()

    def has_event(self, account: str, event_id: str) -> bool:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM events WHERE account = ? AND event_id = ?",
                (account, event_id),
            ).fetchone()
            return row is not None

    def record_trade(self, event: TradeEvent) -> bool:
        with self._lock, self._connect() as conn:
            exists = conn.execute(
                "SELECT 1 FROM events WHERE account = ? AND event_id = ?",
                (event.account, event.event_id),
            ).fetchone()
            if exists:
                return False

            conn.execute(
                """
                INSERT INTO events (
                    account, event_id, kind, nft_id, gift_name, model, background, price, fee, ts, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.account,
                    event.event_id,
                    event.kind,
                    event.nft_id,
                    event.gift_name,
                    event.model,
                    event.background,
                    str(event.price),
                    str(event.fee),
                    int(event.ts),
                    json.dumps(event.raw, ensure_ascii=False),
                ),
            )

            if event.kind == "buy":
                conn.execute(
                    """
                    INSERT INTO positions (
                        account, nft_id, gift_name, model, background, buy_price, buy_ts, status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'open')
                    ON CONFLICT(account, nft_id) DO UPDATE SET
                        gift_name = excluded.gift_name,
                        model = excluded.model,
                        background = excluded.background,
                        buy_price = excluded.buy_price,
                        buy_ts = excluded.buy_ts,
                        status = 'open'
                    """,
                    (
                        event.account,
                        event.nft_id,
                        event.gift_name,
                        event.model,
                        event.background,
                        str(event.price),
                        int(event.ts),
                    ),
                )
            elif event.kind == "sell":
                open_pos = conn.execute(
                    "SELECT buy_price FROM positions WHERE account = ? AND nft_id = ?",
                    (event.account, event.nft_id),
                ).fetchone()
                if open_pos is None:
                    conn.execute(
                        """
                        INSERT INTO positions (
                            account, nft_id, gift_name, model, background, buy_price, buy_ts,
                            sell_price, sell_ts, status
                        ) VALUES (?, ?, ?, ?, ?, '0', 0, ?, ?, 'closed')
                        """,
                        (
                            event.account,
                            event.nft_id,
                            event.gift_name,
                            event.model,
                            event.background,
                            str(event.price),
                            int(event.ts),
                        ),
                    )
                else:
                    conn.execute(
                        """
                        UPDATE positions
                           SET gift_name = ?,
                               model = ?,
                               background = ?,
                               sell_price = ?,
                               sell_ts = ?,
                               status = 'closed'
                         WHERE account = ? AND nft_id = ?
                        """,
                        (
                            event.gift_name,
                            event.model,
                            event.background,
                            str(event.price),
                            int(event.ts),
                            event.account,
                            event.nft_id,
                        ),
                    )

            conn.commit()
            return True

    def get_profit_stats(self, account: Optional[str] = None, since_ts: int = 0) -> ProfitStats:
        with self._lock, self._connect() as conn:
            where = "WHERE ts >= ?"
            params: List[Any] = [int(since_ts)]
            if account:
                where += " AND account = ?"
                params.append(account)

            rows = conn.execute(
                f"SELECT kind, price, fee FROM events {where}",
                params,
            ).fetchall()

            total_buy = Decimal("0")
            total_sell = Decimal("0")
            total_fee = Decimal("0")
            buy_count = 0
            sell_count = 0
            for row in rows:
                kind = str(row["kind"])
                price = Decimal(str(row["price"]))
                fee = Decimal(str(row["fee"]))
                total_fee += fee
                if kind == "buy":
                    buy_count += 1
                    total_buy += price
                elif kind == "sell":
                    sell_count += 1
                    total_sell += price

            pos_where = "WHERE status = 'closed'"
            pos_params: List[Any] = []
            if account:
                pos_where += " AND account = ?"
                pos_params.append(account)
            if since_ts > 0:
                pos_where += " AND sell_ts >= ?"
                pos_params.append(int(since_ts))

            closed = conn.execute(
                f"SELECT buy_price, sell_price FROM positions {pos_where}",
                pos_params,
            ).fetchall()
            realized = Decimal("0")
            for row in closed:
                realized += Decimal(str(row["sell_price"])) - Decimal(str(row["buy_price"]))

            return ProfitStats(
                buy_count=buy_count,
                sell_count=sell_count,
                total_buy=total_buy,
                total_sell=total_sell,
                total_fee=total_fee,
                net_profit=total_sell - total_buy - total_fee,
                realized_profit=realized,
            )

    def get_recent_events(self, limit: int = 20, account: Optional[str] = None) -> List[Dict[str, Any]]:
        with self._lock, self._connect() as conn:
            if account:
                rows = conn.execute(
                    """
                    SELECT account, event_id, kind, nft_id, gift_name, model, background, price, fee, ts
                      FROM events
                     WHERE account = ?
                     ORDER BY ts DESC
                     LIMIT ?
                    """,
                    (account, max(1, limit)),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT account, event_id, kind, nft_id, gift_name, model, background, price, fee, ts
                      FROM events
                     ORDER BY ts DESC
                     LIMIT ?
                    """,
                    (max(1, limit),),
                ).fetchall()
            return [dict(row) for row in rows]

    def get_open_positions(self, limit: int = 30, account: Optional[str] = None) -> List[Dict[str, Any]]:
        with self._lock, self._connect() as conn:
            if account:
                rows = conn.execute(
                    """
                    SELECT account, nft_id, gift_name, model, background, buy_price, buy_ts
                      FROM positions
                     WHERE status = 'open' AND account = ?
                     ORDER BY buy_ts DESC
                     LIMIT ?
                    """,
                    (account, max(1, limit)),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT account, nft_id, gift_name, model, background, buy_price, buy_ts
                      FROM positions
                     WHERE status = 'open'
                     ORDER BY buy_ts DESC
                     LIMIT ?
                    """,
                    (max(1, limit),),
                ).fetchall()
            return [dict(row) for row in rows]

    def get_buy_price(self, account: str, nft_id: str) -> Optional[Decimal]:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT buy_price FROM positions WHERE account = ? AND nft_id = ?",
                (account, nft_id),
            ).fetchone()
            if row is None:
                return None
            return Decimal(str(row["buy_price"]))

