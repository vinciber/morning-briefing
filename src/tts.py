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
    """Normalizza testo per sintesi vocale naturale italiana."""

    # 0. PREZZI CON DOLLARO + MIGLIAIA → forma parlata PRIMA di tutto
    # $70,646 → "settantamila 646 dollari"
    # $5,061.70 → "cinquemila 61 dollari"
    # $103.14 → "103 dollari"
    def replace_usd(m):
        num_str = m.group(1).replace(',', '')
        try:
            val = float(num_str)
            if val >= 1_000_000:
                milioni = val / 1_000_000
                return f'{milioni:.1f} milioni di dollari'.replace('.', ' virgola ')
            elif val >= 1_000:
                thousands = int(val // 1000)
                remainder = int(val % 1000)
                thousands_word = _number_to_italian(thousands) + 'mila'
                if remainder:
                    return f'{thousands_word} {remainder} dollari'
                else:
                    return f'{thousands_word} dollari'
            else:
                int_part = int(val)
                dec_part = round((val - int_part) * 100)
                if dec_part:
                    return f'{int_part} dollari e {dec_part} centesimi'
                else:
                    return f'{int_part} dollari'
        except Exception:
            return m.group(0)

    text = re.sub(r'\$([0-9,]+(?:\.[0-9]{1,2})?)', replace_usd, text)

    # 1. ACRONIMI E ABBREVIAZIONI → forma parlata
    # Ordine importante: prima le forme più lunghe
    abbreviations = [
        ("Standard and Poor's 500",  "Standard and Poor's 500"),  # già espanso
        ("S&P 500",                  "Standard and Poor's 500"),
        ("S&P500",                   "Standard and Poor's 500"),
        ("S&P",                      "Standard and Poor's"),
        ("EUR/USD",                  "cambio euro dollaro"),
        ("EUR/GBP",                  "cambio euro sterlina"),
        ("USD/JPY",                  "cambio dollaro yen"),
        ("GBP/USD",                  "cambio sterlina dollaro"),
        ("STOXX 600",                "indice Stoxx seicento"),
        ("STOXX600",                 "indice Stoxx seicento"),
        ("NIKKEI",                   "indice Nikkei"),
        ("VIX",                      "indice Vix"),
        ("DXY",                      "indice del dollaro"),
        ("TLT",                      "ETF obbligazionario Treasury"),
        ("BCE",                      "Banca Centrale Europea"),
        ("FOMC",                     "Comitato della Federal Reserve"),
        ("BOJ",                      "Banca del Giappone"),
        ("GDP",                      "PIL"),
        ("PCE",                      "indice PCE"),
        ("CPI",                      "inflazione CPI"),
        ("NFP",                      "Non-Farm Payrolls"),
        ("QE",                       "quantitative easing"),
        ("QT",                       "quantitative tightening"),
        ("BTC",                      "Bitcoin"),
        ("btc",                      "Bitcoin"),
        ("bps",                      "punti base"),
        ("bp",                       "punti base"),
        ("BTP",                      "BTP"),
        ("Fed",                      "Federal Reserve"),
    ]
    for abbr, expanded in abbreviations:
        text = text.replace(abbr, expanded)

    # 2. EURO SIMBOLO
    text = re.sub(r'€([0-9,.]+)', r'\1 euro', text)

    # 3. PERCENTUALI CON SEGNO → forma parlata
    def replace_pct_signed_dec(m):
        sign = m.group(1)
        integer = m.group(2)
        decimal = m.group(3)
        sign_word = 'più ' if sign == '+' else 'meno ' if sign == '-' else ''
        return f'{sign_word}{integer} virgola {decimal} percento'
    text = re.sub(r'([+-])(\d+)\.(\d+)%', replace_pct_signed_dec, text)

    def replace_pct_dec(m):
        return f'{m.group(1)} virgola {m.group(2)} percento'
    text = re.sub(r'(\d+)\.(\d+)%', replace_pct_dec, text)

    def replace_pct_int(m):
        sign_word = 'più ' if m.group(1) == '+' else 'meno ' if m.group(1) == '-' else ''
        return f'{sign_word}{m.group(2)} percento'
    text = re.sub(r'([+-]?)(\d+)%', replace_pct_int, text)

    # 4. NUMERI CON SEPARATORE MIGLIAIA RIMASTI (es. 53,820)
    def replace_thousands(m):
        num_str = m.group(0).replace(',', '')
        try:
            val = int(num_str)
            if val >= 1_000_000:
                return f'{val // 1_000_000} milioni'
            elif val >= 10_000:
                thousands = val // 1000
                remainder = val % 1000
                word = _number_to_italian(thousands) + 'mila'
                return f'{word} {remainder}' if remainder else word
            else:
                return str(val)
        except Exception:
            return m.group(0)
    text = re.sub(r'\b\d{1,3}(?:,\d{3})+\b', replace_thousands, text)

    # 5. RENDIMENTI CON % (es. 4.2850%)
    text = re.sub(r'(\d+)\.(\d{2})\d*%', r'\1 virgola \2 percento', text)

    # 6. TRONCA DECIMALI A 2 CIFRE RESIDUI
    text = re.sub(r'(\d+)\.(\d{2})\d+', r'\1.\2', text)

    # 7. PUNTI DECIMALI RESIDUI → virgola
    text = re.sub(r'(\d+)\.(\d+)', r'\1 virgola \2', text)

    return text


def _number_to_italian(n: int) -> str:
    """Converte numeri piccoli in parole italiane per 'Xmila'."""
    words = {
        1: 'un', 2: 'due', 3: 'tre', 4: 'quattro', 5: 'cinque',
        6: 'sei', 7: 'sette', 8: 'otto', 9: 'nove', 10: 'dieci',
        11: 'undici', 12: 'dodici', 13: 'tredici', 14: 'quattordici',
        15: 'quindici', 16: 'sedici', 17: 'diciassette', 18: 'diciotto',
        19: 'diciannove', 20: 'venti', 30: 'trenta', 40: 'quaranta',
        50: 'cinquanta', 60: 'sessanta', 70: 'settanta', 80: 'ottanta',
        90: 'novanta',
    }
    if n in words:
        return words[n]
    if n < 100:
        tens = (n // 10) * 10
        ones = n % 10
        return words[tens] + words[ones]
    return str(n)


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
