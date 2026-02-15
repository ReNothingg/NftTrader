from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from typing import Callable, Dict, List, Optional

from .models import TelegramSettings
from .storage import TradeLedger


def _fmt_amount(value: Decimal) -> str:
    return f"{value.quantize(Decimal('0.01'))}"


def _utc_day_start_ts() -> int:
    now = datetime.now(tz=timezone.utc)
    start = datetime(year=now.year, month=now.month, day=now.day, tzinfo=timezone.utc)
    return int(start.timestamp())


class TelegramSupervisor:
    def __init__(
        self,
        *,
        settings: TelegramSettings,
        ledger: TradeLedger,
        workers_snapshot: Callable[[], Dict[str, str]],
        logger: Callable[[str], None],
    ) -> None:
        self.settings = settings
        self.ledger = ledger
        self.workers_snapshot = workers_snapshot
        self.log = logger

        self._enabled_runtime = False
        self._queue: asyncio.Queue[str] = asyncio.Queue(maxsize=2000)
        self._bot = None
        self._dp = None
        self._polling_task: Optional[asyncio.Task[None]] = None
        self._sender_task: Optional[asyncio.Task[None]] = None

    @property
    def enabled(self) -> bool:
        return bool(self.settings.enabled and self.settings.token)

    def _on_task_done(self, task: asyncio.Task[None], name: str) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is None:
            return
        self._enabled_runtime = False
        self.log(f"[tg] {name} crashed: {exc}")

    async def start(self) -> None:
        if not self.enabled:
            return
        try:
            from aiogram import Bot, Dispatcher, Router
            from aiogram.filters import Command
            from aiogram.types import Message
        except Exception as exc:
            self.log(f"[tg] aiogram unavailable: {exc}")
            return

        allowed = set(self.settings.chat_ids)
        self._bot = Bot(self.settings.token)
        self._dp = Dispatcher()
        router = Router()
        try:
            me = await self._bot.get_me()
        except Exception as exc:
            self.log(f"[tg] bot auth failed: {exc}")
            try:
                await self._bot.session.close()
            except Exception:
                pass
            self._bot = None
            self._dp = None
            return

        username = getattr(me, "username", "")
        if username:
            self.log(f"[tg] connected as @{username}")
        else:
            self.log(f"[tg] connected as id={me.id}")

        def _allowed(chat_id: int) -> bool:
            if not allowed:
                return True
            return chat_id in allowed

        async def _reply(message: Message, text: str) -> None:
            if not _allowed(message.chat.id):
                return
            await message.answer(text)

        @router.message(Command("start"))
        async def handle_start(message: Message) -> None:
            await _reply(
                message,
                "Portal bot online.\n"
                "Commands: /stats /today /positions /last /workers",
            )

        @router.message(Command("stats"))
        async def handle_stats(message: Message) -> None:
            if not _allowed(message.chat.id):
                return
            stats = self.ledger.get_profit_stats()
            await _reply(
                message,
                "\n".join(
                    [
                        "All-time stats:",
                        f"Buys: {stats.buy_count} ({_fmt_amount(stats.total_buy)})",
                        f"Sells: {stats.sell_count} ({_fmt_amount(stats.total_sell)})",
                        f"Fees: {_fmt_amount(stats.total_fee)}",
                        f"Net: {_fmt_amount(stats.net_profit)}",
                        f"Realized: {_fmt_amount(stats.realized_profit)}",
                    ]
                ),
            )

        @router.message(Command("today"))
        async def handle_today(message: Message) -> None:
            if not _allowed(message.chat.id):
                return
            stats = self.ledger.get_profit_stats(since_ts=_utc_day_start_ts())
            await _reply(
                message,
                "\n".join(
                    [
                        "Today UTC:",
                        f"Buys: {stats.buy_count} ({_fmt_amount(stats.total_buy)})",
                        f"Sells: {stats.sell_count} ({_fmt_amount(stats.total_sell)})",
                        f"Fees: {_fmt_amount(stats.total_fee)}",
                        f"Net: {_fmt_amount(stats.net_profit)}",
                    ]
                ),
            )

        @router.message(Command("positions"))
        async def handle_positions(message: Message) -> None:
            if not _allowed(message.chat.id):
                return
            rows = self.ledger.get_open_positions(limit=10)
            if not rows:
                await _reply(message, "Open positions: none")
                return
            lines = ["Open positions (last 10):"]
            for row in rows:
                lines.append(
                    f"{row['account']} | {row['gift_name']} | "
                    f"{row['model']}/{row['background']} | buy {row['buy_price']}"
                )
            await _reply(message, "\n".join(lines))

        @router.message(Command("last"))
        async def handle_last(message: Message) -> None:
            if not _allowed(message.chat.id):
                return
            rows = self.ledger.get_recent_events(limit=10)
            if not rows:
                await _reply(message, "No trades yet")
                return
            lines = ["Last trades (10):"]
            for row in rows:
                lines.append(
                    f"{row['account']} | {row['kind']} | {row['gift_name']} "
                    f"{row['model']}/{row['background']} | {row['price']}"
                )
            await _reply(message, "\n".join(lines))

        @router.message(Command("workers"))
        async def handle_workers(message: Message) -> None:
            if not _allowed(message.chat.id):
                return
            snapshot = self.workers_snapshot()
            lines = ["Workers:"]
            for name, state in sorted(snapshot.items()):
                lines.append(f"{name}: {state}")
            await _reply(message, "\n".join(lines))

        self._dp.include_router(router)
        self._sender_task = asyncio.create_task(self._sender_loop(), name="tg-sender")
        self._polling_task = asyncio.create_task(
            self._dp.start_polling(self._bot),
            name="tg-polling",
        )
        self._sender_task.add_done_callback(lambda task: self._on_task_done(task, "sender"))
        self._polling_task.add_done_callback(lambda task: self._on_task_done(task, "polling"))
        self._enabled_runtime = True
        self.log("[tg] bot started")

    async def _sender_loop(self) -> None:
        if self._bot is None:
            return
        while True:
            text = await self._queue.get()
            for chat_id in self.settings.chat_ids:
                try:
                    await self._bot.send_message(chat_id=chat_id, text=text)
                except Exception as exc:
                    self.log(f"[tg] send failed chat={chat_id}: {exc}")
            self._queue.task_done()

    async def notify(self, text: str) -> None:
        if not self._enabled_runtime:
            return
        try:
            self._queue.put_nowait(text)
        except asyncio.QueueFull:
            self.log("[tg] queue full, dropping notification")

    async def stop(self) -> None:
        was_running = (
            self._enabled_runtime
            or self._polling_task is not None
            or self._sender_task is not None
        )
        if self._polling_task:
            self._polling_task.cancel()
            try:
                await self._polling_task
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                self.log(f"[tg] polling stop error: {exc}")
            self._polling_task = None

        if self._sender_task:
            self._sender_task.cancel()
            try:
                await self._sender_task
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                self.log(f"[tg] sender stop error: {exc}")
            self._sender_task = None

        if self._bot is not None:
            try:
                await self._bot.session.close()
            except Exception:
                pass
        self._bot = None
        self._dp = None
        self._enabled_runtime = False
        if was_running:
            self.log("[tg] bot stopped")
