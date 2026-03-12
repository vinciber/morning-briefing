#!/usr/bin/env python3
import json, logging, yaml, wave, io, os
from datetime import datetime, timezone
from pathlib import Path
from piper.voice import PiperVoice
from pydub import AudioSegment

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
INPUT_PATH = ROOT / 'data' / 'briefing_today.json'
OUTPUT_DIR = ROOT / 'docs' / 'audio'
MODEL_DIR = ROOT / 'models'

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
        labels = labels_it
        for key, label in labels.items():
            item = market.get(key, {})
            val = item.get('value', 'N/A') if isinstance(item, dict) else item
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

    text = briefing_to_text(briefing, lang=audio_lang)
    date_str = briefing.get('date', datetime.now(timezone.utc).strftime('%Y-%m-%d'))
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    temp_wav = OUTPUT_DIR / "temp_briefing.wav"
    output_mp3 = OUTPUT_DIR / f'briefing_{date_str.replace("-", "")}.mp3'

    # Scegli modello
    if audio_lang == 'it':
        model_name = "it_IT-paola-medium.onnx"
    else:
        model_name = "en_US-ryan-medium.onnx"
    
    model_path = MODEL_DIR / model_name
    if not model_path.exists():
        logger.error(f'❌ Modello Piper non trovato in {model_path}')
        return None

    logger.info(f'🎙️ Generazione audio ({audio_lang}) con Piper TTS...')
    try:
        voice = PiperVoice.load(str(model_path))
        with wave.open(str(temp_wav), "wb") as wav_file:
            voice.synthesize(text, wav_file)
        
        # Converti WAV in MP3 per compatibilità web e dimensioni
        segment = AudioSegment.from_wav(str(temp_wav))
        segment.export(str(output_mp3), format="mp3", bitrate="128k")
        
        # Pulisci temp
        if temp_wav.exists():
            temp_wav.unlink()

        size_kb = output_mp3.stat().st_size / 1024
        logger.info(f'✅ Audio generato: {output_mp3} ({size_kb:.0f} KB)')
        return str(output_mp3)
    except Exception as e:
        logger.error(f'❌ Errore Piper: {e}')
        return None

if __name__ == '__main__':
    run()
