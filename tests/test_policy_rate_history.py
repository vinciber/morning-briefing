import sys
import types
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))

import market_fetcher

# La logica testata non usa il client LLM; il virtualenv leggero dei test non
# include il relativo SDK, quindi ne basta un modulo segnaposto in import.
groq_module = types.ModuleType('groq')
groq_module.Groq = object
sys.modules.setdefault('groq', groq_module)
import summarizer


class _Response:
    def __init__(self, observations):
        self._observations = observations

    def raise_for_status(self):
        return None

    def json(self):
        return {'observations': self._observations}


def test_fed_target_range_uses_previous_distinct_range(monkeypatch):
    histories = {
        'DFEDTARL': [
            {'date': '2026-09-19', 'value': '3.75'},
            {'date': '2026-09-18', 'value': '3.75'},
            {'date': '2026-09-17', 'value': '3.50'},
        ],
        'DFEDTARU': [
            {'date': '2026-09-19', 'value': '4.00'},
            {'date': '2026-09-18', 'value': '4.00'},
            {'date': '2026-09-17', 'value': '3.75'},
        ],
    }

    def fake_get(_url, *, params, timeout):
        assert params['limit'] > 2
        assert timeout == 15
        return _Response(histories[params['series_id']])

    monkeypatch.setattr(market_fetcher, 'FRED_API_KEY', 'test-key')
    monkeypatch.setattr(market_fetcher.requests, 'get', fake_get)

    assert market_fetcher.get_fed_target_range() == {
        'value': '3.75% - 4.00%',
        'previous': '3.50% - 3.75%',
        'release_date': '2026-09-19',
    }


def test_ecb_rates_keep_previous_distinct_levels(monkeypatch):
    histories = {
        'ECBMRRFR': [
            {'date': '2026-09-19', 'value': '2.65'},
            {'date': '2026-09-18', 'value': '2.65'},
            {'date': '2026-09-17', 'value': '2.90'},
        ],
        'ECBDFR': [
            {'date': '2026-09-19', 'value': '2.50'},
            {'date': '2026-09-18', 'value': '2.50'},
            {'date': '2026-09-17', 'value': '2.75'},
        ],
    }

    def fake_get(_url, *, params, timeout):
        assert params['limit'] > 2
        assert timeout == 15
        return _Response(histories[params['series_id']])

    monkeypatch.setenv('FRED_API_KEY', 'test-key')
    monkeypatch.setattr(market_fetcher, 'fetch_eurostat_indicator', lambda _url: (None, None, None))
    monkeypatch.setattr(market_fetcher.requests, 'get', fake_get)

    result = market_fetcher.get_macro_calendar_eu()

    assert result['ecb_rate']['value'] == '2.65%'
    assert result['ecb_rate']['previous'] == '2.90%'
    assert result['ecb_deposit_rate']['value'] == '2.50%'
    assert result['ecb_deposit_rate']['previous'] == '2.75%'


def test_policy_rate_cards_are_unique_and_keep_previous_values():
    market_data = {
        'macro_calendar': {},
        'macro_calendar_eu': {
            'ecb_rate': {'value': '2.65%', 'previous': '2.90%'},
            'ecb_deposit_rate': {'value': '2.50%', 'previous': '2.75%'},
        },
    }

    summarizer._set_policy_rate_cards(
        market_data,
        ecb_deposit='2.50%',
        ecb_refi='2.65%',
        fed_rate_range='3.75% - 4.00%',
        ecb_last='2026-09-10',
        ecb_next='2026-10-29',
        fed_last='2026-09-16',
        fed_next='2026-11-04',
        source_dfr={'previous': '2.75%'},
        source_refi={'previous': '2.90%'},
        source_fed={'previous': '3.50% - 3.75%'},
    )

    macro_eu = market_data['macro_calendar_eu']
    assert 'ecb_deposit_rate' not in macro_eu
    assert macro_eu['ecb_rate']['previous'] == '2.75%'
    assert macro_eu['ecb_refi_rate']['previous'] == '2.90%'
    assert market_data['macro_calendar']['fed_funds']['previous'] == '3.50% - 3.75%'
