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
    """Recupera lo script audio pre-generato dall'AI."""
    return briefing.get(f'audio_script_{lang}', '')

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
            with wave.open(str(temp_wav), "wb") as wav_file:
                voice.synthesize(text, wav_file)
            
            segment = AudioSegment.from_wav(str(temp_wav))
            segment.export(str(output_mp3), format="mp3", bitrate="128k")
            
            if temp_wav.exists():
                temp_wav.unlink()

            size_kb = output_mp3.stat().st_size / 1024
            logger.info(f'✅ Audio {lang} generato: {output_mp3} ({size_kb:.0f} KB)')
            generated_files.append(str(output_mp3))
        except Exception as e:
            logger.error(f'❌ Errore Piper ({lang}): {e}')

    return generated_files

if __name__ == '__main__':
    run()
