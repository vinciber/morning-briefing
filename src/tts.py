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
    """Normalizza testo per sintesi vocale naturale."""

    # 1. ABBREVIAZIONI FINANZIARIE → forma parlata
    abbreviations = {
        'S&P 500':        "Standard and Poor's 500",
        'S&P500':         "Standard and Poor's 500",
        'S&P':            "Standard and Poor's",
        'EUR/USD':        'cambio euro dollaro',
        'EUR/GBP':        'cambio euro sterlina',
        'USD/JPY':        'cambio dollaro yen',
        'GBP/USD':        'cambio sterlina dollaro',
        'VIX':            'indice Vix',
        'DXY':            'indice del dollaro',
        'TLT':            'ETF obbligazionario Treasury',
        'BTP':            'BTP',
        'BCE':            'Banca Centrale Europea',
        'Fed':            'Federal Reserve',
        'FOMC':           'Comitato della Federal Reserve',
        'GDP':            'PIL',
        'PCE':            'indice PCE',
        'CPI':            'inflazione CPI',
        'NFP':            'Non-Farm Payrolls',
        'BOJ':            'Banca del Giappone',
        'QE':             'quantitative easing',
        'QT':             'quantitative tightening',
        'ETF':            'ETF',
        'STOXX 600':      'indice Stoxx seicento',
        'STOXX600':       'indice Stoxx seicento',
        'NIKKEI':         'indice Nikkei',
        'BTC':            'Bitcoin',
        'btc':            'Bitcoin',
        'bps':            'punti base',
        'bp':             'punti base',
    }
    for abbr, expanded in abbreviations.items():
        text = text.replace(abbr, expanded)

    # 2. SIMBOLI VALUTA → forma parlata
    def replace_usd_thousands(m):
        num_str = m.group(1).replace(',', '')
        try:
            val = float(num_str)
            if val >= 1_000_000:
                return f'{val/1_000_000:.1f} milioni di dollari'
            elif val >= 1_000:
                thousands = int(val // 1000)
                remainder = int(val % 1000)
                if remainder:
                    return f'{thousands}mila {remainder} dollari'
                else:
                    return f'{thousands}mila dollari'
            else:
                return f'{val:.0f} dollari'
        except Exception:
            return m.group(0)

    text = re.sub(r'\$([0-9,]+(?:\.[0-9]+)?)', replace_usd_thousands, text)
    text = re.sub(r'€([0-9,.]+)', r'\1 euro', text)

    # 3. PERCENTUALI CON SEGNO → forma parlata
    def replace_pct_signed(m):
        sign = m.group(1)
        integer = m.group(2)
        decimal = m.group(3)
        sign_word = 'più ' if sign == '+' else 'meno ' if sign == '-' else ''
        return f'{sign_word}{integer} virgola {decimal} percento'

    text = re.sub(r'([+-])(\d+)\.(\d+)%', replace_pct_signed, text)

    def replace_pct_unsigned(m):
        integer = m.group(1)
        decimal = m.group(2)
        return f'{integer} virgola {decimal} percento'

    text = re.sub(r'(\d+)\.(\d+)%', replace_pct_unsigned, text)

    def replace_pct_int(m):
        sign = m.group(1)
        integer = m.group(2)
        sign_word = 'più ' if sign == '+' else 'meno ' if sign == '-' else ''
        return f'{sign_word}{integer} percento'

    text = re.sub(r'([+-]?)(\d+)%', replace_pct_int, text)

    # 4. NUMERI CON SEPARATORE MIGLIAIA → forma parlata
    def replace_thousands(m):
        num_str = m.group(0).replace(',', '')
        try:
            val = int(num_str)
            if val >= 1_000_000:
                return f'{val // 1_000_000} milioni'
            elif val >= 1_000:
                thousands = val // 1000
                remainder = val % 1000
                if remainder:
                    return f'{thousands}mila {remainder}'
                else:
                    return f'{thousands}mila'
            else:
                return str(val)
        except Exception:
            return m.group(0)

    text = re.sub(r'\b\d{1,3}(?:,\d{3})+\b', replace_thousands, text)

    # 5. RENDIMENTI CON % ATTACCATA
    text = re.sub(r'(\d+)\.(\d{2})\d*%', r'\1 virgola \2 percento', text)

    # 6. TRONCA DECIMALI A 2 CIFRE
    text = re.sub(r'(\d+)\.(\d{2})\d+', r'\1.\2', text)

    # 7. PUNTI DECIMALI RESIDUI → virgola per TTS italiano
    text = re.sub(r'(\d+)\.(\d+)', r'\1 virgola \2', text)

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
