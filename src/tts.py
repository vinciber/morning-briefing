#!/usr/bin/env python3
import json, logging, yaml, io
from datetime import datetime, timezone
from pathlib import Path
from gtts import gTTS
from pydub import AudioSegment

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
INPUT_PATH = ROOT / 'data' / 'briefing_today.json'
OUTPUT_DIR = ROOT / 'docs' / 'audio'
MAX_CHUNK_CHARS = 3000

def briefing_to_text(briefing, lang='it'):
    parts = []
    date = briefing.get('date', datetime.now(timezone.utc).strftime('%Y-%m-%d'))
    if lang == 'it':
        parts.append(f'Briefing finanziario e geopolitico del {date}.')
    else:
        parts.append(f'Financial and geopolitical briefing for {date}.')

    sentiment = briefing.get('sentiment', {})
    label_map_it = {'risk_on': 'risk on', 'risk_off': 'risk off', 'neutral': 'neutrale'}
    if sentiment.get('label'):
        if lang == 'it':
            parts.append(f'Sentiment: {label_map_it.get(sentiment["label"], sentiment["label"])}.')
            if sentiment.get('reason_it'):
                parts.append(sentiment['reason_it'])
        else:
            parts.append(f'Sentiment: {sentiment["label"].replace("_", " ")}.')
            if sentiment.get('reason_en'):
                parts.append(sentiment['reason_en'])
    parts.append('')

    market = briefing.get('market_data', {})
    if market:
        parts.append('Dati di mercato.' if lang == 'it' else 'Market data.')
        labels_it = {'eur_usd': 'Euro Dollaro', 'vix': 'VIX', 'btp_10y': 'BTP decennale',
                     'gold': 'Oro', 'oil_brent': 'Brent', 'sp500_futures': 'S&P 500'}
        labels_en = {'eur_usd': 'Euro Dollar', 'vix': 'VIX', 'btp_10y': 'BTP ten year',
                     'gold': 'Gold', 'oil_brent': 'Brent crude', 'sp500_futures': 'S&P 500'}
        labels = labels_it if lang == 'it' else labels_en
        for key, label in labels.items():
            val = market.get(key, '')
            if val and val != 'N/A':
                parts.append(f'{label}: {val}.')
        parts.append('')

    section_names = {
        'it': {'mercati': 'Mercati', 'geopolitica': 'Geopolitica',
               'macro_economia': 'Macroeconomia', 'energia': 'Energia'},
        'en': {'mercati': 'Markets', 'geopolitica': 'Geopolitics',
               'macro_economia': 'Macroeconomics', 'energia': 'Energy'}
    }
    for section in briefing.get('sections', []):
        sec_name = section.get('name', '')
        label = section_names[lang].get(sec_name, sec_name)
        parts.append(f'{"Sezione" if lang == "it" else "Section"}: {label}.')
        for item in section.get('items', []):
            title = item.get(f'title_{lang}', item.get('title_it', ''))
            summary = item.get(f'summary_{lang}', item.get('summary_it', ''))
            source = item.get('source_name', '')
            if title:
                parts.append(f'{title}.')
            if summary:
                parts.append(summary)
            if source:
                parts.append(f'{"Fonte" if lang == "it" else "Source"}: {source}.')
            parts.append('')

    parts.append('Fine del briefing. Buona giornata.' if lang == 'it' else 'End of briefing. Have a good day.')
    return '\n'.join(parts)

def split_text(text, max_chars=MAX_CHUNK_CHARS):
    if len(text) <= max_chars:
        return [text]
    chunks, current = [], ''
    for line in text.split('\n'):
        if len(current) + len(line) + 1 > max_chars and current:
            chunks.append(current.strip())
            current = ''
        current += line + '\n'
    if current.strip():
        chunks.append(current.strip())
    return chunks

def run():
    if not INPUT_PATH.exists():
        logger.error(f'❌ File non trovato: {INPUT_PATH}')
        return None

    with open(INPUT_PATH, 'r', encoding='utf-8') as f:
        briefing = json.load(f)

    audio_lang = 'it'
    config_path = ROOT / 'config.yml'
    if config_path.exists():
        with open(config_path, 'r') as cf:
            cfg = yaml.safe_load(cf)
            audio_lang = cfg.get('output', {}).get('audio', {}).get('language', 'it')

    lang_code = 'it' if audio_lang == 'it' else 'en'
    text = briefing_to_text(briefing, lang=audio_lang)
    date_str = briefing.get('date', datetime.now(timezone.utc).strftime('%Y-%m-%d'))
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_file = OUTPUT_DIR / f'briefing_{date_str.replace("-", "")}.mp3'

    logger.info(f'🎙️ Generazione audio ({audio_lang}) con gTTS...')
    logger.info(f'   Testo: {len(text)} chars')

    chunks = split_text(text)

    if len(chunks) == 1:
        tts = gTTS(text=chunks[0], lang=lang_code, slow=False)
        tts.save(str(output_file))
    else:
        combined = AudioSegment.empty()
        silence = AudioSegment.silent(duration=500)
        for i, chunk in enumerate(chunks):
            tts = gTTS(text=chunk, lang=lang_code, slow=False)
            mp3_fp = io.BytesIO()
            tts.write_to_fp(mp3_fp)
            mp3_fp.seek(0)
            segment = AudioSegment.from_mp3(mp3_fp)
            combined += segment + silence
            logger.info(f'   Chunk {i+1}/{len(chunks)} generato')
        combined.export(str(output_file), format='mp3')

    size_kb = output_file.stat().st_size / 1024
    logger.info(f'✅ Audio generato: {output_file} ({size_kb:.0f} KB)')
    return str(output_file)

if __name__ == '__main__':
    run()
