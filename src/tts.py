#!/usr/bin/env python3
"""
tts.py — Generazione Audio con Edge-TTS
Converte il briefing in MP3 usando voci neurali Microsoft.
Gestisce chunking per testi lunghi (>2800 chars) con merge pydub.
Output: docs/audio/briefing_YYYYMMDD.mp3
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import edge_tts
from pydub import AudioSegment

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
INPUT_PATH = ROOT / 'data' / 'briefing_today.json'
OUTPUT_DIR = ROOT / 'docs' / 'audio'

VOICE_IT = 'it-IT-DiegoNeural'
VOICE_EN = 'en-US-GuyNeural'
MAX_CHUNK_CHARS = 2800
SILENCE_BETWEEN_SECTIONS_MS = 500


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def briefing_to_text(briefing: dict, lang: str = 'it') -> str:
    """Converte il briefing JSON in testo leggibile per TTS."""
    parts = []
    date = briefing.get('date', datetime.now(timezone.utc).strftime('%Y-%m-%d'))

    if lang == 'it':
        parts.append(f'Briefing finanziario e geopolitico del {date}.')
        sentiment = briefing.get('sentiment', {})
        if sentiment.get('label'):
            label_map = {'risk_on': 'risk on', 'risk_off': 'risk off', 'neutral': 'neutrale'}
            parts.append(f'Sentiment di mercato: {label_map.get(sentiment["label"], sentiment["label"])}.')
            if sentiment.get('reason_it'):
                parts.append(sentiment['reason_it'])
    else:
        parts.append(f'Financial and geopolitical briefing for {date}.')
        sentiment = briefing.get('sentiment', {})
        if sentiment.get('label'):
            parts.append(f'Market sentiment: {sentiment["label"].replace("_", " ")}.')
            if sentiment.get('reason_en'):
                parts.append(sentiment['reason_en'])

    parts.append('')  # Pausa

    # Market data
    market = briefing.get('market_data', {})
    if market:
        if lang == 'it':
            parts.append('Dati di mercato.')
        else:
            parts.append('Market data.')

        labels = {
            'eur_usd': 'Euro Dollaro',
            'vix': 'VIX',
            'btp_10y': 'BTP decennale',
            'gold': 'Oro',
            'oil_brent': 'Brent',
            'sp500_futures': 'S&P 500 Futures',
        }
        for key, label in labels.items():
            val = market.get(key, '')
            if val:
                parts.append(f'{label}: {val}.')
        parts.append('')

    # Sezioni
    section_names_it = {
        'mercati': 'Mercati',
        'geopolitica': 'Geopolitica',
        'macro_economia': 'Macroeconomia',
        'energia': 'Energia',
    }
    section_names_en = {
        'mercati': 'Markets',
        'geopolitica': 'Geopolitics',
        'macro_economia': 'Macroeconomics',
        'energia': 'Energy',
    }

    for section in briefing.get('sections', []):
        sec_name = section.get('name', '')
        if lang == 'it':
            parts.append(f'Sezione: {section_names_it.get(sec_name, sec_name)}.')
        else:
            parts.append(f'Section: {section_names_en.get(sec_name, sec_name)}.')

        for item in section.get('items', []):
            title_key = f'title_{lang}'
            summary_key = f'summary_{lang}'
            title = item.get(title_key, item.get('title_it', ''))
            summary = item.get(summary_key, item.get('summary_it', ''))
            source = item.get('source_name', '')

            parts.append(f'{title}.')
            if summary:
                parts.append(summary)
            if source:
                if lang == 'it':
                    parts.append(f'Fonte: {source}.')
                else:
                    parts.append(f'Source: {source}.')
            parts.append('')

    if lang == 'it':
        parts.append('Fine del briefing. Buona giornata.')
    else:
        parts.append('End of briefing. Have a good day.')

    return '\n'.join(parts)


def split_text(text: str, max_chars: int = MAX_CHUNK_CHARS) -> list[str]:
    """Divide il testo in chunk rispettando i confini di frase."""
    if len(text) <= max_chars:
        return [text]

    chunks = []
    current = ''

    for line in text.split('\n'):
        # Se aggiungere questa riga supera il max, salva il chunk corrente
        if len(current) + len(line) + 1 > max_chars and current:
            chunks.append(current.strip())
            current = ''
        current += line + '\n'

    if current.strip():
        chunks.append(current.strip())

    return chunks


async def generate_chunk_audio(text: str, voice: str, output_path: str):
    """Genera audio per un singolo chunk."""
    communicate = edge_tts.Communicate(text, voice)
    await communicate.save(output_path)


async def generate_audio(text: str, voice: str, output_path: Path):
    """Genera l'audio completo con chunking e merge binario."""
    chunks = split_text(text)

    if len(chunks) == 1:
        # File singolo, niente merge necessario
        await generate_chunk_audio(chunks[0], voice, str(output_path))
        return

    # Genera chunk individuali
    tmp_files = []
    for i, chunk in enumerate(chunks):
        tmp_path = output_path.parent / f'_chunk_{i}.mp3'
        await generate_chunk_audio(chunk, voice, str(tmp_path))
        tmp_files.append(tmp_path)
        logger.info(f'   Chunk {i+1}/{len(chunks)} generato')

    # Merge binario mp3 (evita dipendenza da pydub e ffmpeg)
    with open(output_path, 'wb') as outfile:
        for tmp_file in tmp_files:
            with open(tmp_file, 'rb') as infile:
                outfile.write(infile.read())

    # Cleanup temporanei
    for tmp_file in tmp_files:
        tmp_file.unlink(missing_ok=True)


def run():
    """Pipeline TTS: carica briefing → genera audio MP3."""
    if not INPUT_PATH.exists():
        logger.error(f'❌ File non trovato: {INPUT_PATH}. Esegui prima summarizer.py')
        return None

    with open(INPUT_PATH, 'r', encoding='utf-8') as f:
        briefing = json.load(f)

    import yaml
    config_path = ROOT / 'config.yml'
    audio_lang = 'it'
    if config_path.exists():
        with open(config_path, 'r') as cf:
            cfg = yaml.safe_load(cf)
            audio_lang = cfg.get('output', {}).get('audio', {}).get('language', 'it')

    voice = VOICE_IT if audio_lang == 'it' else VOICE_EN
    text = briefing_to_text(briefing, lang=audio_lang)

    date_str = briefing.get('date', datetime.now(timezone.utc).strftime('%Y-%m-%d'))
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_file = OUTPUT_DIR / f'briefing_{date_str.replace("-", "")}.mp3'

    logger.info(f'🎙️ Generazione audio ({audio_lang})...')
    logger.info(f'   Testo: {len(text)} chars, voce: {voice}')

    asyncio.run(generate_audio(text, voice, output_file))

    size_kb = output_file.stat().st_size / 1024
    logger.info(f'✅ Audio generato: {output_file} ({size_kb:.0f} KB)')

    return str(output_file)


if __name__ == '__main__':
    run()
