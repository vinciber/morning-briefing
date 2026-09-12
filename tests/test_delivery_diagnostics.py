import sys
from pathlib import Path

import requests


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from telegram_bot import delivery_error_detail


def test_delivery_diagnostic_keeps_runtime_cause_but_redacts_request_url():
    assert delivery_error_detail(RuntimeError('Publication is not current')) == 'Publication is not current'
    error = requests.ConnectionError('https://api.telegram.org/botprivate-token/sendMessage')
    assert delivery_error_detail(error) == 'network_or_http_error_url_redacted'
