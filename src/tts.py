#!/usr/bin/env python3
import json, logging, yaml, wave, io, os, re
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

def normalize_for_tts(text: str) -> str:
    """Sostituzioni per una migliore pronuncia finanziaria in italiano."""
    # Tronca decimali a 2 cifre: 27.1900 -> 27.19
    text = re.sub(r'(\d+)\.(\d{2})\d+', r'\1.\2', text)

    replacements = {
        'VIX':    'Vix',
        'S&P 500': 'S e P 500',
        'S&P':    'S e P',
        'DXY':    'D X Y',
        'TLT':    'T L T',
        'BTP':    'B T P',
        'BCE':    'B C E',
        'Fed':    'Fed',
        'FOMC':   'F O M C',
        'GDP':    'G D P',
        'PIL':    'Pil',
        'PCE':    'P C E',
        'CPI':    'C P I',
        'NFP':    'N F P',
        'ETF':    'E T F',
        'QE':     'Q E',
        'QT':     'Q T',
        '%':      ' percento',
        '$':      ' dollari ',
        '€':      ' euro ',
    }
    for k, v in replacements.items():
        text = text.replace(k, v)
    return text

def briefing_to_text(briefing, lang='it'):
    """Recupera lo script audio pre-generato dall'AI."""
    text = briefing.get(f'audio_script_{lang}', '')
    if lang == 'it':
        text = normalize_for_tts(text)
    return text

def run():
    if not INPUT_PATH.exists():
        logger.error(f'❌ File non trovato: {INPUT_PATH}')
        return None

    with open(INPUT_PATH, 'r', encoding='utf-8') as f:
        briefing = json.load(f)

    date_str = briefing.get('date', datetime.now(timezone.utc).strftime('%Y-%m-%d'))
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    generated_files = []
    
    for lang in ['it', 'en']:
        text = briefing_to_text(briefing, lang=lang)
        temp_wav = OUTPUT_DIR / f"temp_{lang}.wav"
        
        # Nome file: briefing_YYYYMMDD.mp3 per IT, briefing_YYYYMMDD_en.mp3 per EN
        suffix = f'_{lang}' if lang != 'it' else ''
        output_mp3 = OUTPUT_DIR / f'briefing_{date_str.replace("-", "")}{suffix}.mp3'

        model_name = "it_IT-paola-medium.onnx" if lang == 'it' else "en_US-ryan-medium.onnx"
        model_path = MODEL_DIR / model_name
        
        if not model_path.exists():
            logger.warning(f'⚠️ Modello {lang} non trovato in {model_path}, skip.')
            continue

        logger.info(f'🎙️ Generazione audio ({lang}) con Piper TTS...')
        try:
            voice = PiperVoice.load(str(model_path))
            
            # Dividi in paragrafi per inserire silenzi naturali
            paragraphs = [p.strip() for p in text.split('\n\n') if p.strip()]
            combined = AudioSegment.empty()
            silence = AudioSegment.silent(duration=600)

            for i, p in enumerate(paragraphs):
                p_wav = io.BytesIO()
                with wave.open(p_wav, "wb") as wav_file:
                    voice.synthesize(p, wav_file)
                
                p_wav.seek(0)
                p_segment = AudioSegment.from_wav(p_wav)
                combined += p_segment
                
                if i < len(paragraphs) - 1:
                    combined += silence
            
            combined.export(str(output_mp3), format="mp3", bitrate="128k")
            
            size_kb = output_mp3.stat().st_size / 1024
            logger.info(f'✅ Audio {lang} generato: {output_mp3} ({size_kb:.0f} KB)')
            generated_files.append(str(output_mp3))
        except Exception as e:
            logger.error(f'❌ Errore Piper ({lang}): {e}')

    return generated_files

if __name__ == '__main__':
    run()
