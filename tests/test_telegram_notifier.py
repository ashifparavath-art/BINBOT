"""TelegramNotifier must never raise — a notification is a side effect of
a trade, never a precondition for one. These tests exist to catch the one
regression that would matter here: some future change making send()
propagate an exception (network error, bad token, non-200 response) up
into caller code that isn't expecting to handle it.
"""
from __future__ import annotations

from unittest.mock import patch

from notifications.telegram_notifier import TelegramNotifier


class _FakeResponse:
    def __init__(self, status: int = 200, body: str = "") -> None:
        self.status = status
        self._body = body

    async def __aenter__(self) -> "_FakeResponse":
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False

    async def text(self) -> str:
        return self._body


class _FakeSession:
    def __init__(self, response: "_FakeResponse | None" = None, raise_on_post: Exception | None = None) -> None:
        self._response = response or _FakeResponse()
        self._raise_on_post = raise_on_post
        self.calls: list[dict] = []

    async def __aenter__(self) -> "_FakeSession":
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False

    def post(self, url: str, json: dict | None = None, timeout: object = None) -> "_FakeResponse":
        self.calls.append({"url": url, "json": json})
        if self._raise_on_post is not None:
            raise self._raise_on_post
        return self._response


def _patch_session(fake_session: _FakeSession):
    return patch("notifications.telegram_notifier.aiohttp.ClientSession", return_value=fake_session)


class TestEnabled:
    def test_disabled_without_token(self):
        assert not TelegramNotifier("", "123").enabled

    def test_disabled_without_chat_id(self):
        assert not TelegramNotifier("tok", "").enabled

    def test_enabled_with_both_set(self):
        assert TelegramNotifier("tok", "123").enabled


class TestSend:
    async def test_noop_when_disabled_makes_no_http_call(self):
        fake = _FakeSession()
        with _patch_session(fake):
            await TelegramNotifier("", "").send("hello")
        assert fake.calls == []

    async def test_posts_expected_url_and_payload(self):
        fake = _FakeSession()
        with _patch_session(fake):
            await TelegramNotifier("tok", "42").send("hello")

        assert len(fake.calls) == 1
        call = fake.calls[0]
        assert call["url"] == "https://api.telegram.org/bottok/sendMessage"
        assert call["json"] == {"chat_id": "42", "text": "hello", "parse_mode": "Markdown"}

    async def test_does_not_raise_on_non_200_response(self):
        fake = _FakeSession(response=_FakeResponse(status=400, body="bad request"))
        with _patch_session(fake):
            await TelegramNotifier("tok", "42").send("hello")  # must not raise

    async def test_does_not_raise_on_network_error(self):
        fake = _FakeSession(raise_on_post=ConnectionError("no network"))
        with _patch_session(fake):
            await TelegramNotifier("tok", "42").send("hello")  # must not raise
