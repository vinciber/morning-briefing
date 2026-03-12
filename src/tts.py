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
    """Trasforma il briefing JSON in un testo narrativo strutturato per TTS."""
    parts = []
    date = briefing.get('date', '')
    
    # 1. Introduzione
    if lang == 'it':
        parts.append(f"Buongiorno. È il {date}. Questo è il vostro Morning Briefing Finanziario e Geopolitico.")
    else:
        parts.append(f"Good morning. It is {date}. This is your Financial and Geopolitical Morning Briefing.")
    parts.append("")

    # 2. Sentiment e Macro Outlook (Il "Punto della Situazione")
    sentiment = briefing.get('sentiment', {})
    if lang == 'it':
        label = sentiment.get('label', 'neutral').replace('_', ' ')
        parts.append(f"Iniziamo con il sentiment di mercato, oggi classificato come {label}.")
    else:
        label = sentiment.get('label', 'neutral').replace('_', ' ')
        parts.append(f"Starting with market sentiment, currently rated as {label}.")
    
    reason = sentiment.get(f'reason_{lang}', '')
    if reason:
        parts.append(reason)
    parts.append("")

    # 3. Market Impact Summary
    impact = briefing.get('market_impact_summary', {})
    impact_text = impact.get(lang, '') if isinstance(impact, dict) else ''
    if impact_text:
        parts.append(impact_text)
    parts.append("")

    # 4. Sezioni Dettagliate
    section_labels = {
        'it': {
            'mercati': 'Passiamo all\'analisi dei mercati e degli asset finanziari.',
            'geopolitica': 'Sul fronte geopolitico, i riflettori sono puntati sui seguenti eventi.',
            'macro_economia': 'Per quanto riguarda la macroeconomia e le politiche monetarie.',
            'energia': 'Infine, uno sguardo al settore energetico e delle materie prime.'
        },
        'en': {
            'mercati': 'Moving on to market analysis and financial assets.',
            'geopolitica': 'On the geopolitical front, the focus is on the following events.',
            'macro_economia': 'Regarding macroeconomics and monetary policy.',
            'energia': 'Finally, a look at the energy and commodities sector.'
        }
    }

    for section in briefing.get('sections', []):
        sec_name = section.get('name', '')
        if sec_name not in section_labels[lang]:
            continue
            
        # Filtriamo solo item molto rilevanti per l'audio
        items = [i for i in section.get('items', []) if i.get('relevance_score', 0) >= 3]
        if not items:
            continue

        parts.append(section_labels[lang][sec_name])
        parts.append("")

        for item in items:
            title = item.get(f'title_{lang}', '')
            summary = item.get(f'summary_{lang}', '')
            if title:
                parts.append(f"{title}.")
            if summary:
                parts.append(summary)
            parts.append("")

    # 5. Chiusura
    if lang == 'it':
        parts.append("Il briefing si conclude qui. Restate sintonizzati per ulteriori aggiornamenti. Buona giornata di trading e lavoro.")
    else:
        parts.append("That concludes the briefing. Stay tuned for further updates. Have a productive day.")

    return "\n".join(parts)
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
