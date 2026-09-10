import base64
import json
import sys
from pathlib import Path
from unittest.mock import Mock
import pytest
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from delivery_state import Ledger


class Store:
    def __init__(self): self.value = None; self.version = 0
    def get(self, *a, **kw):
        if self.value is None: return Mock(status_code=404)
        return Mock(status_code=200, json=lambda: {'sha': str(self.version),
            'content': base64.b64encode(json.dumps(self.value).encode()).decode()})
    def put(self, *a, json, **kw):
        assert json.get('sha') == (str(self.version) if self.version else None)
        self.value = __import__('json').loads(base64.b64decode(json['content']))
        self.version += 1
        return Mock(status_code=200, json=lambda: {'content': {'sha': str(self.version)}})


@pytest.fixture
def store(monkeypatch):
    monkeypatch.setenv('GITHUB_TOKEN', 'synthetic')
    monkeypatch.setenv('GITHUB_REPOSITORY', 'example/test')
    return Store()


def test_crash_after_intent_does_not_send_again(store):
    ledger = Ledger('2026-09-09', store)
    send = Mock(side_effect=TimeoutError())
    with pytest.raises(TimeoutError): ledger.deliver('text:0', send)
    retry = Ledger('2026-09-09', store)
    with pytest.raises(RuntimeError, match='Ambiguous'): retry.deliver('text:0', send)
    assert send.call_count == 1


def test_success_skips_sent_parts_and_failed_rejection_can_retry(store):
    ledger = Ledger('2026-09-09', store)
    send = Mock(return_value=Mock(status_code=200, json=lambda: {'ok': True}))
    ledger.deliver('text:0', send)
    Ledger('2026-09-09', store).deliver('text:0', send)
    assert send.call_count == 1
    with pytest.raises(RuntimeError, match='rejected'):
        ledger.deliver('audio', lambda: Mock(status_code=429, json=lambda: {'ok': False}))
    Ledger('2026-09-09', store).deliver('audio', send)
    assert send.call_count == 2


def test_ledger_write_failure_prevents_send(store):
    ledger = Ledger('2026-09-09', store)
    store.put = lambda *a, **kw: Mock(status_code=409)
    send = Mock()
    with pytest.raises(RuntimeError): ledger.deliver('text', send)
    send.assert_not_called()
