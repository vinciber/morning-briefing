import hashlib
import sys
from tempfile import TemporaryDirectory
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from delivery_state import (PAGES_BASE_URL, archived_ready, fetch_published_audio,
                            load_published_briefing, verify_publication)


class Response:
    def __init__(self, payload=None, content=b''):
        self.payload = payload
        self.content = content

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class Pages:
    def __init__(self, briefing, audio):
        self.briefing = briefing
        self.audio = audio
        self.urls = []

    def get(self, url, **kwargs):
        self.urls.append(url)
        if url.endswith('/api/today.json') or '/archive/' in url:
            return Response(self.briefing)
        return Response(content=self.audio[url.rsplit('/', 1)[-1]])


def test_published_archive_and_audio_are_validated_without_runner_data():
    date = '2026-09-12'
    audio = {'briefing_20260912.mp3': b'italiano', 'briefing_20260912_en.mp3': b'english'}
    briefing = {
        'date': date,
        'audio_manifest_it': {
            'schema_version': 1, 'date': date, 'language': 'it',
            'audio_url': 'audio/briefing_20260912.mp3', 'duration_ms': 1,
            'sha256': hashlib.sha256(audio['briefing_20260912.mp3']).hexdigest(),
        },
        'audio_manifest_en': {
            'schema_version': 1, 'date': date, 'language': 'en',
            'audio_url': 'audio/briefing_20260912_en.mp3', 'duration_ms': 1,
            'sha256': hashlib.sha256(audio['briefing_20260912_en.mp3']).hexdigest(),
        },
    }
    pages = Pages(briefing, audio)

    loaded = load_published_briefing(date, pages)
    assert loaded == briefing
    assert fetch_published_audio(loaded, 'it', pages) == b'italiano'
    verify_publication(loaded, pages)
    assert pages.urls[0] == f'{PAGES_BASE_URL}archive/{date}.json'

    with TemporaryDirectory() as directory:
        root = Path(directory)
        (root / 'docs/archive').mkdir(parents=True)
        (root / 'docs/audio').mkdir()
        import json
        (root / f'docs/archive/{date}.json').write_text(json.dumps(briefing))
        for name, content in audio.items():
            (root / 'docs/audio' / name).write_bytes(content)
        assert archived_ready(root, date) is True
