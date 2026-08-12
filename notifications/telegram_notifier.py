"""Proactive alerts to a Telegram chat via the Bot API — trade opened,
trade closed (stop-loss/take-profit/manual, with the P&L figure), and
risk-halt events, so these are seen immediately instead of discovered
later by checking the dashboard.

Deliberately never raises. A notification is a side effect of a trade,
never a precondition for one — a Telegram outage or bad token must not be
able to block an order or crash the process that's supposed to be
guarding real money.
"""
from __future__ import annotations

import aiohttp

from config.logging_config import get_logger

logger = get_logger(__name__)

_API_BASE = "https://api.telegram.org"


class TelegramNotifier:
    def __init__(self, bot_token: str, chat_id: str) -> None:
        self._bot_token = bot_token
        self._chat_id = chat_id

    @property
    def enabled(self) -> bool:
        return bool(self._bot_token and self._chat_id)

    async def send(self, text: str) -> None:
        if not self.enabled:
            return
        url = f"{_API_BASE}/bot{self._bot_token}/sendMessage"
        payload = {"chat_id": self._chat_id, "text": text, "parse_mode": "Markdown"}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        logger.warning("telegram.send_failed", status=resp.status, body=body[:200])
        except Exception as exc:
            logger.warning("telegram.send_error", error=str(exc))
