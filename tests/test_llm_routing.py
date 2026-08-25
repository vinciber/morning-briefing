import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from llm_routing import (
    complete_with_fallback,
    complete_with_morning_fallback,
    configured_gemini_keys,
    configured_news_keys,
)


class FakeLogger:
    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass


class ApiError(Exception):
    def __init__(self, status_code, message=""):
        self.status_code = status_code
        super().__init__(message)


class FakeClient:
    def __init__(self, outcomes, calls):
        self.outcomes = iter(outcomes)
        self.calls = calls
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self.create))

    def create(self, **kwargs):
        self.calls.append(kwargs["model"])
        outcome = next(self.outcomes)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def test_news_keys_do_not_include_shared_backend_slots():
    assert configured_news_keys({
        "GROQ_API_KEY_NEWS": "news-1",
        "GROQ_API_KEY_NEWS_2": "news-2",
        "GROQ_API_KEY_2": "shared-2",
        "GROQ_API_KEY": "shared-1",
    }) == [("GROQ_API_KEY_NEWS", "news-1"), ("GROQ_API_KEY_NEWS_2", "news-2")]


def test_gemini_keys_are_in_priority_order_without_duplicates():
    assert configured_gemini_keys({
        "GEMINI_API_KEY": "gemini-1",
        "GEMINI_API_KEY_2": "gemini-2",
        "GEMINI_API_KEY_3": "gemini-1",
    }) == [("GEMINI_API_KEY", "gemini-1"), ("GEMINI_API_KEY_2", "gemini-2")]


def test_retired_model_moves_to_next_model_without_touching_second_key():
    calls = []
    primary = FakeClient([ApiError(404), "ok"], calls)
    secondary = FakeClient(["unused"], calls)

    result = complete_with_fallback(
        [("GROQ_API_KEY_NEWS", primary), ("GROQ_API_KEY_NEWS_2", secondary)],
        ("retired-model", "working-model"), FakeLogger(), purpose="test", messages=[],
    )

    assert result == "ok"
    assert calls == ["retired-model", "working-model"]


def test_quota_rotates_only_to_next_dedicated_news_key():
    calls = []
    primary = FakeClient([ApiError(429)], calls)
    secondary = FakeClient(["ok"], calls)

    assert complete_with_fallback(
        [("GROQ_API_KEY_NEWS", primary), ("GROQ_API_KEY_NEWS_2", secondary)],
        ("working-model",), FakeLogger(), purpose="test", messages=[],
    ) == "ok"
    assert calls == ["working-model", "working-model"]


def test_exhausted_model_pool_uses_next_model_on_the_same_dedicated_key():
    calls = []
    primary = FakeClient([ApiError(429), "ok"], calls)

    assert complete_with_fallback(
        [("GROQ_API_KEY_NEWS", primary)],
        ("preferred-model", "fallback-model"), FakeLogger(), purpose="test", messages=[],
    ) == "ok"
    assert calls == ["preferred-model", "fallback-model"]


def test_json_generation_validation_is_retried_once():
    calls = []
    primary = FakeClient([
        ApiError(400, "json_validate_failed"),
        "ok",
    ], calls)

    assert complete_with_fallback(
        [("GROQ_API_KEY_NEWS", primary)],
        ("working-model",), FakeLogger(), purpose="test", messages=[],
    ) == "ok"
    assert calls == ["working-model", "working-model"]


def test_persistent_json_generation_validation_uses_next_model():
    calls = []
    primary = FakeClient([
        ApiError(400, "Failed to validate JSON"),
        ApiError(400, "Failed to validate JSON"),
        "ok",
    ], calls)

    assert complete_with_fallback(
        [("GROQ_API_KEY_NEWS", primary)],
        ("preferred-model", "fallback-model"), FakeLogger(), purpose="test", messages=[],
    ) == "ok"
    assert calls == ["preferred-model", "preferred-model", "fallback-model"]


def test_morning_briefing_uses_groq_news_before_gemini():
    gemini_calls, groq_calls = [], []
    gemini = FakeClient(["unused"], gemini_calls)
    groq = FakeClient(["groq-ok"], groq_calls)

    assert complete_with_morning_fallback(
        [("GEMINI_API_KEY", gemini)], "gemini-model",
        [("GROQ_API_KEY_NEWS", groq)], ("groq-model",), FakeLogger(),
        purpose="test", messages=[],
    ) == "groq-ok"
    assert groq_calls == ["groq-model"]
    assert gemini_calls == []


def test_morning_briefing_falls_back_to_gemini_after_groq_news_failure():
    gemini_calls, groq_calls = [], []
    gemini = FakeClient(["gemini-ok"], gemini_calls)
    groq = FakeClient([ApiError(429)], groq_calls)

    assert complete_with_morning_fallback(
        [("GEMINI_API_KEY", gemini)], "gemini-model",
        [("GROQ_API_KEY_NEWS", groq)], ("groq-model",), FakeLogger(),
        purpose="test", messages=[],
    ) == "gemini-ok"
    assert gemini_calls == ["gemini-model"]
    assert groq_calls == ["groq-model"]


def test_morning_briefing_uses_news_2_only_after_groq_and_gemini_fail():
    gemini_calls, primary_calls, secondary_calls = [], [], []
    gemini = FakeClient([ApiError(429)], gemini_calls)
    primary = FakeClient([ApiError(429)], primary_calls)
    secondary = FakeClient(["news-2-ok"], secondary_calls)

    assert complete_with_morning_fallback(
        [("GEMINI_API_KEY", gemini)], "gemini-model",
        [("GROQ_API_KEY_NEWS", primary), ("GROQ_API_KEY_NEWS_2", secondary)],
        ("groq-model",), FakeLogger(), purpose="test", messages=[],
    ) == "news-2-ok"
    assert primary_calls == ["groq-model"]
    assert gemini_calls == ["gemini-model"]
    assert secondary_calls == ["groq-model"]
