import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))

import market_fetcher


PREOPEN = datetime(2026, 8, 21, 5, 47, tzinfo=timezone.utc)  # 07:47 CEST


def test_europe_futures_signal_is_based_on_the_futures_quote(monkeypatch):
    monkeypatch.setattr(
        market_fetcher,
        'get_yahoo_intraday_quote',
        lambda _: {
            'price': 2_186.0,
            'previous_close': 2_190.0,
            'quote_timestamp': int(datetime(2026, 8, 21, 5, 42, tzinfo=timezone.utc).timestamp()),
            'name': 'EURO STOXX 50 Futures Roll I',
            'delayed_by_minutes': None,
        },
    )

    signal = market_fetcher.get_europe_futures_signal(PREOPEN)

    assert signal['status'] == 'available'
    assert signal['change_pct'] < 0
    assert 'avvio europeo moderatamente negativo' in signal['summary_it']
    assert 'Asia' not in signal['summary_it']


def test_europe_futures_signal_is_omitted_when_the_quote_is_stale(monkeypatch):
    monkeypatch.setattr(
        market_fetcher,
        'get_yahoo_intraday_quote',
        lambda _: {
            'price': 2_184.0,
            'previous_close': 2_190.0,
            'quote_timestamp': int(datetime(2026, 8, 21, 5, 0, tzinfo=timezone.utc).timestamp()),
            'name': 'EURO STOXX 50 Futures Roll I',
            'delayed_by_minutes': None,
        },
    )

    signal = market_fetcher.get_europe_futures_signal(PREOPEN)

    assert signal['status'] == 'unavailable'
    assert signal['reason'] == 'stale_quote'


def test_europe_futures_signal_is_omitted_outside_preopen(monkeypatch):
    monkeypatch.setattr(market_fetcher, 'get_yahoo_intraday_quote', lambda _: None)

    signal = market_fetcher.get_europe_futures_signal(
        datetime(2026, 8, 21, 8, 5, tzinfo=timezone.utc)  # 10:05 CEST
    )

    assert signal['status'] == 'unavailable'
    assert signal['reason'] == 'outside_europe_preopen'
