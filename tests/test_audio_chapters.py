import importlib.util
import json
import math
import struct
import sys
import types
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from audio_chapters import make_it_chapters, sanitize_audio_text, translated_chapters


def test_sanitize_audio_text_fixes_gold_and_calendar_dates():
    italian = "L’orologio di oro è in calo. Mercoledì 16/09 – Tasso Fed Funds."
    assert sanitize_audio_text(italian) == "L'oro è in calo. Mercoledì 16 settembre – Tasso Fed Funds."
    assert sanitize_audio_text("The gold clock is down on 16/09.", 'en') == \
        "The gold is down on September 16."


def test_make_and_translate_chapters_keep_sanitized_audio_text():
    chapters = make_it_chapters(
        {'chapters_it': {'apertura': 'Buongiorno', 'asia': 'L’orologio di oro, 16/09.'}},
        {'crypto': 'Cripto'},
        {'close': 'Fine'},
    )
    asia = next(chapter for chapter in chapters if chapter['id'] == 'asia')
    assert asia['text'] == "L'oro, 16 settembre."

    translated, _ = translated_chapters({
        'audio_chapters_en': [dict(chapter, text='The gold clock, 16/09.') for chapter in chapters]
    }, chapters)
    translated_asia = next(chapter for chapter in translated if chapter['id'] == 'asia')
    assert translated_asia['text'] == 'The gold, September 16.'


def test_wrapped_finance_and_deterministic_translation_identity():
    chapters = make_it_chapters({'chapters_it': {'apertura': 'Buongiorno', 'asia': 'Asia'}},
                               {'crypto': 'Cripto'}, {'close': 'Fine'})
    assert [c['id'] for c in chapters] == ['intro', 'asia', 'crypto', 'close']
    translated = [dict(c, text='English', title='Untrusted title') for c in reversed(chapters)]
    result, text = translated_chapters({'audio_chapters_en': translated}, chapters)
    assert [c['id'] for c in result] == [c['id'] for c in chapters]
    assert result[0]['title'] == 'Opening'
    invalid, fallback = translated_chapters({'audio_chapters_en': translated[:-1]}, chapters)
    assert invalid == [] and fallback


@pytest.fixture
def tts(monkeypatch, tmp_path):
    voice_module = types.ModuleType('piper.voice')
    class Voice:
        texts = []
        @classmethod
        def load(cls, path): return cls()
        def synthesize(self, text, wav, length_scale):
            self.texts.append(text)
            wav.setparams((1, 2, 16000, 0, 'NONE', 'not compressed'))
            count = int(16000 * length_scale)
            wav.writeframes(b''.join(struct.pack('<h', int(3000 * math.sin(i / 7))) for i in range(count)))
    voice_module.PiperVoice = Voice
    monkeypatch.setitem(sys.modules, 'piper', types.ModuleType('piper'))
    monkeypatch.setitem(sys.modules, 'piper.voice', voice_module)
    spec = importlib.util.spec_from_file_location('review_tts', Path(__file__).resolve().parents[1] / 'src/tts.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.INPUT_PATH = tmp_path / 'briefing.json'
    module.OUTPUT_DIR = tmp_path / 'audio'
    module.MODEL_DIR = tmp_path / 'models'
    module.MODEL_DIR.mkdir()
    for name in ('it_IT-paola-medium.onnx', 'en_US-amy-medium.onnx'):
        (module.MODEL_DIR / name).touch()
    script = {'date': '2026-09-09'}
    for lang in ('it', 'en'):
        script[f'audio_narrative_{lang}'] = [
            {'id': 'finance', 'title': 'Markets', 'text': 'BTC +2%'},
            {'id': 'crypto', 'title': 'Crypto', 'text': 'ETH'},
            {'id': 'close', 'title': 'Close', 'text': 'Fine'}]
        script[f'audio_script_{lang}'] = 'BTC +2%\n\nETH\n\nFine'
    module.INPUT_PATH.write_text(json.dumps(script))
    return module


def test_real_mp3_offsets_pauses_and_language_normalization(tts):
    assert len(tts.run()) == 2
    result = json.loads(tts.INPUT_PATH.read_text())
    it, en = result['audio_manifest_it'], result['audio_manifest_en']
    assert it['chapters'][1]['start_ms'] == 1700
    assert en['chapters'][1]['start_ms'] == 1600
    assert it['duration_ms'] > en['duration_ms']
    assert 'Bitcoin' in tts.PiperVoice.texts[0]
    assert 'percento' in tts.PiperVoice.texts[0]
    assert all(c['end_ms'] <= it['duration_ms'] for c in it['chapters'])
    # Narrative survives a retry; each artifact is exported once, then measured.
    assert len(tts.run()) == 2
    assert json.loads(tts.INPUT_PATH.read_text())['audio_narrative_it']


def test_failed_export_never_persists_fresh_manifest(tts, monkeypatch):
    def fail(*a, **kw): raise OSError('synthetic export failure')
    monkeypatch.setattr(tts.AudioSegment, 'export', fail)
    assert tts.run() == []
    result = json.loads(tts.INPUT_PATH.read_text())
    assert 'audio_manifest_it' not in result and 'audio_manifest_en' not in result
    assert result['audio_narrative_it'][0]['text'] == 'BTC +2%'
