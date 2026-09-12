"""Durable delivery intents. An uncertain Telegram outcome is never auto-retried.

GitHub Contents SHA acts as a compare-and-swap; workflow concurrency serializes
daily runs. No recipient, message text or credentials enter the ledger.
"""
import base64
import hashlib
import json
import os
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

PAGES_BASE_URL = 'https://vinciber.github.io/morning-briefing/'


def today():
    return datetime.now(ZoneInfo('Europe/Rome')).date().isoformat()


def valid_briefing(briefing, date):
    """Validate the immutable metadata shared by local and published output."""
    try:
        if briefing['date'] != date:
            return False
        for lang in ('it', 'en'):
            manifest = briefing[f'audio_manifest_{lang}']
            name = f"audio/briefing_{date.replace('-', '')}{'_en' if lang == 'en' else ''}.mp3"
            if (manifest['schema_version'] != 1 or manifest['date'] != date or
                    manifest['language'] != lang or manifest['audio_url'] != name or
                    manifest['duration_ms'] <= 0 or not manifest['sha256']):
                return False
        return True
    except (KeyError, TypeError):
        return False


def ready(root, date):
    try:
        root = Path(root)
        briefing = json.loads((root / 'data/briefing_today.json').read_text())
        if not valid_briefing(briefing, date):
            return False
        for lang in ('it', 'en'):
            m = briefing[f'audio_manifest_{lang}']
            if hashlib.sha256((root / 'docs' / m['audio_url']).read_bytes()).hexdigest() != m['sha256']:
                return False
        return True
    except (KeyError, ValueError, TypeError, OSError):
        return False


def archived_ready(root, date):
    """Check the versioned archive used by clean workflow checkouts."""
    try:
        root = Path(root)
        briefing = json.loads((root / f'docs/archive/{date}.json').read_text())
        if not valid_briefing(briefing, date):
            return False
        for lang in ('it', 'en'):
            manifest = briefing[f'audio_manifest_{lang}']
            if hashlib.sha256((root / 'docs' / manifest['audio_url']).read_bytes()).hexdigest() != manifest['sha256']:
                return False
        return True
    except (KeyError, ValueError, TypeError, OSError):
        return False


def load_published_briefing(date, session=requests):
    """Load the full, immutable daily archive from the already deployed site.

    The workflow deliberately does not version `data/briefing_today.json`.
    A downstream runner must therefore use this archive rather than assuming
    that its clean checkout contains another job's temporary output.
    """
    response = session.get(f'{PAGES_BASE_URL}archive/{date}.json', timeout=20)
    response.raise_for_status()
    try:
        briefing = response.json()
    except ValueError as error:
        raise RuntimeError('Published briefing is not valid JSON') from error
    if not valid_briefing(briefing, date):
        raise RuntimeError('Published briefing is incomplete or not current')
    return briefing


def fetch_published_audio(briefing, lang='it', session=requests):
    """Fetch one published MP3 and verify it against the briefing manifest."""
    manifest = briefing[f'audio_manifest_{lang}']
    response = session.get(
        f"{PAGES_BASE_URL}{manifest['audio_url']}",
        params={'v': manifest['sha256']}, timeout=30)
    response.raise_for_status()
    if hashlib.sha256(response.content).hexdigest() != manifest['sha256']:
        raise RuntimeError('Published audio differs')
    return response.content


class Ledger:
    def __init__(self, date, session=requests):
        self.session = session
        repo = os.environ.get('GITHUB_REPOSITORY', '')
        token = os.environ.get('GITHUB_TOKEN', '')
        if not repo or not token:
            raise RuntimeError('Delivery ledger credentials unavailable')
        self.url = f'https://api.github.com/repos/{repo}/contents/data/delivery-state/{date}.json'
        self.headers = {'Authorization': f'Bearer {token}', 'Accept': 'application/vnd.github+json'}
        r = session.get(self.url, headers=self.headers, params={'ref': 'main'}, timeout=15)
        self.sha = None
        self.state = {'schema_version': 1, 'date': date, 'published': False, 'complete': False, 'messages': {}}
        if r.status_code == 200:
            payload = r.json()
            self.sha = payload['sha']
            self.state = json.loads(base64.b64decode(payload['content']))
            if self.state.get('date') != date:
                raise ValueError('Wrong delivery date')
        elif r.status_code != 404:
            raise RuntimeError(f'Cannot read delivery ledger: HTTP {r.status_code}')

    def save(self):
        body = {'message': f"briefing delivery state: {self.state['date']}", 'branch': 'main',
                'content': base64.b64encode(json.dumps(self.state, sort_keys=True).encode()).decode()}
        if self.sha:
            body['sha'] = self.sha
        r = self.session.put(self.url, headers=self.headers, json=body, timeout=15)
        if r.status_code not in (200, 201):
            raise RuntimeError(f'Cannot persist delivery ledger: HTTP {r.status_code}')
        self.sha = r.json()['content']['sha']

    def deliver(self, key, callback):
        status = self.state['messages'].get(key)
        if status == 'sent':
            return
        if status == 'pending':
            raise RuntimeError('Ambiguous previous send: operator reconciliation required')
        self.state['messages'][key] = 'pending'
        self.save()  # A crash after here requires reconciliation, never blind resending.
        response = callback()
        try:
            result = response.json()
        except ValueError:
            raise RuntimeError('Ambiguous Telegram response') from None
        if response.status_code == 200 and result.get('ok') is True:
            self.state['messages'][key] = 'sent'
            self.save()
        elif response.status_code < 500 and result.get('ok') is False:
            self.state['messages'][key] = 'failed'
            self.save()
            raise RuntimeError(f'Telegram rejected delivery: HTTP {response.status_code}')
        else:
            raise RuntimeError('Ambiguous Telegram response')


def verify_publication(briefing, session=requests):
    response = session.get(PAGES_BASE_URL + 'api/today.json', params={'date': briefing['date']}, timeout=20)
    response.raise_for_status()
    remote = response.json()
    if remote.get('date') != briefing['date']:
        raise RuntimeError('Publication is not current')
    for lang in ('it', 'en'):
        expected = briefing[f'audio_manifest_{lang}']
        if remote.get(f'audio_manifest_{lang}') != expected:
            raise RuntimeError('Published manifest differs')
        fetch_published_audio(briefing, lang, session)
