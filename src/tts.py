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

    # 0. RIMUOVI ACRONIMI TRA PARENTESI E VARIANTI — evita doppioni
    parenthetical_acronyms = [
        'EUR/USD', 'DXY', 'VIX', 'TLT', 'BTP', 'BCE', 'Fed',
        'FOMC', 'BOJ', 'GDP', 'PCE', 'CPI', 'NFP', 'BTC', 'S&P 500', 'S&P',
        'STOXX 600', 'NIKKEI', 'QE', 'QT', 'ETF',
        'USA', 'NATO', 'IMF', 'WTO', 'OPEC', 'SWIFT',
    ]
    for acronym in parenthetical_acronyms:
        # Rimuove (ACRONIMO)
        text = re.sub(r'\s*\(\s*' + re.escape(acronym) + r'\s*\)', '', text)
        # Rimuove "noto come ACRONIMO", "conosciuto come ACRONIMO", ecc.
        text = re.sub(r',?\s*(noto come|conosciuto come|detto|chiamato)\s+' + re.escape(acronym), '', text, flags=re.IGNORECASE)

    # 1. ACRONIMI E NOMI INGLESI — pronuncia naturale
    acronym_words = [
        # Pronuncia come parola
        ('USA',    'Usa'),
        ('NATO',   'Nato'),
        ('OPEC',   'Opek'),
        ('SWIFT',  'Swift'),
        ('IMF',    'Fondo Monetario Internazionale'),
        ('WTO',    'Organizzazione Mondiale del Commercio'),
        ('USA',    'America'),
        ('U.S.A.',  'America'),
        ('FED',    'Fed'),
        ('FOMC',   'Comitato della Federal Reserve'),
        # Nomi propri inglesi — lasciare in inglese
        ('BlackRock',   'BlackRock'),
        ('Goldman Sachs', 'Goldman Sachs'),
        ('JPMorgan',    'Jay Pi Morgan'),
        ('Morgan Stanley', 'Morgan Stanley'),
        ('Citigroup',   'Citigroup'),
        # Nomi propri e termini inglesi comuni — pronuncia naturale
        ('Trump',       'Tramp'),
        ('Biden',       'Baiden'),
        ('Powell',      'Pauel'),
        ('Lagarde',     'Lagard'),
        # Indici — pronuncia estesa
        ('Nikkei',      'Nikkei'),
        ('Shanghai',    'Shanghai'),
        ('Hang Seng',   'Hang Seng'),
        # Crypto
        ('Ethereum',    'Ethereum'),
        ('Solana',      'Solana'),
        ('Ripple',      'Ripple'),
    ]
    for orig, replacement in acronym_words:
        text = text.replace(orig, replacement)

    # 2. PREZZI CON DOLLARO + MIGLIAIA → forma parlata PRIMA di tutto
    def replace_usd(m):
        num_str = m.group(1).replace(',', '')
        suffix = m.group(2)
        try:
            val = float(num_str)
            
            # Gestione suffissi espliciti (es. $45.1M, $1.2B)
            if suffix:
                s = suffix.lower()
                if s in ('m', 'million', 'milioni'):
                    val_str = f"{val:g}".replace('.', ' virgola ')
                    unit = 'milione' if val == 1 else 'milioni'
                    return f'{val_str} {unit} di dollari'
                if s in ('b', 'billion', 'miliardi'):
                    val_str = f"{val:g}".replace('.', ' virgola ')
                    unit = 'miliardo' if val == 1 else 'miliardi'
                    return f'{val_str} {unit} di dollari'

            # Gestione numeri estesi (es. $1,000,000)
            if val >= 1_000_000_000:
                miliardi = val / 1_000_000_000
                val_str = f"{miliardi:g}".replace('.', ' virgola ')
                unit = 'miliardo' if miliardi == 1 else 'miliardi'
                return f'{val_str} {unit} di dollari'
            elif val >= 1_000_000:
                milioni = val / 1_000_000
                val_str = f"{milioni:g}".replace('.', ' virgola ')
                unit = 'milione' if milioni == 1 else 'milioni'
                return f'{val_str} {unit} di dollari'
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

    text = re.sub(
        r'\$(-?[0-9,]+(?:\.[0-9]+)?)(?:\s*(million|billion|milioni|miliardi|M|B)(?!\w))?', 
        replace_usd, 
        text, 
        flags=re.IGNORECASE
    )

    # 2.5. HANDLING YEARS (20Y -> 20 anni)
    text = re.sub(r'\b(\d+)Y\b', r'\1 anni', text)

    # 3. ACRONIMI E ABBREVIAZIONI → forma parlata
    abbreviations = [
        ("Standard and Poor's 500",  "Standard and Poor's 500"),
        ("S&P 500",                  "Standard and Poor's 500"),
        ("S&P500",                   "Standard and Poor's 500"),
        ("L'S&P 500",                "Lo Standard and Poor's 500"),
        ("L'S&P",                    "Lo Standard and Poor's"),
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
        ("USA",                      "Usa"),
        ("U.S.A.",                    "Usa"),
        ("EU",                       "Europa"),
        ("E.U.",                      "Europa"),
        ("UK",                       "Regno Unito"),
        ("U.K.",                      "Regno Unito"),
        ("ECB",                      "Banca Centrale Europea"),
        ("treasury",                 "trèsiuri"),
        ("growth",                   "grouth"),
        ("yield",                    "ild"),
        ("hawkish",                  "ho-kish"),
        ("dovish",                   "da-vish"),
        ("rally",                    "ralli"),
        ("trend",                    "trend"),
        ("recession",                "recescion"),
        ("bearish",                  "berish"),
        ("bullish",                  "bullish"),
        ("briefing",                 "brifing"),
        ("sentiment",                "sèntiment"),
        ("Binance",                  "Bainans"),
        ("deep dive",                "dip daiv"),
        ("deep_dive",                "dip daiv"),
        ("Fear & Greed",             "fiar end grid"),
        ("Fear and Greed",           "fiar end grid"),
        ("Extreme Fear",             "estrema paura"),
        ("Extreme fear",             "estrema paura"),
        ("Extreme",                  "estrema"),
        ("fear",                     "paura"),
        ("greed",                    "grid"),
        ("Bybit",                    "Baibit"),
        ("Coinbase",                 "Coin-beis"),
    ]
    for abbr, expanded in abbreviations:
        text = text.replace(abbr, expanded)

    # 3. EURO SIMBOLO
    text = re.sub(r'€([0-9,.]+)', r'\1 euro', text)

    # 4. PERCENTUALI CON SEGNO → forma parlata
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

    # 5. NUMERI CON SEPARATORE MIGLIAIA RIMASTI (es. 53,820)
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

    # 6. RENDIMENTI CON % (es. 4.2850%)
    text = re.sub(r'(\d+)\.(\d{2})\d*%', r'\1 virgola \2 percento', text)

    # 7. TRONCA DECIMALI A 2 CIFRE RESIDUI
    text = re.sub(r'(\d+)\.(\d{2})\d+', r'\1.\2', text)

    # 8. PUNTI DECIMALI RESIDUI → virgola
    text = re.sub(r'(\d+)\.(\d+)', r'\1 virgola \2', text)

    # 9. OIL -> petrolio (se rimasto o tradotto male)
    # Gestione apostrofi italiani: l'olio -> il petrolio, dell'olio -> del petrolio, ecc.
    text = re.sub(r"\bl'olio\b", "il petrolio", text, flags=re.IGNORECASE)
    text = re.sub(r"\bdell'olio\b", "del petrolio", text, flags=re.IGNORECASE)
    text = re.sub(r"\ball'olio\b", "al petrolio", text, flags=re.IGNORECASE)
    text = re.sub(r"\bsull'olio\b", "sul petrolio", text, flags=re.IGNORECASE)
    text = re.sub(r"\bolio\b", "petrolio", text, flags=re.IGNORECASE)

    # 10. S&P 500 -> lo Standard & Poor's (correzione articolo)
    text = re.sub(r"\bl'Standard\b", "lo Standard", text, flags=re.IGNORECASE)
    text = re.sub(r"\bdell'Standard\b", "dello Standard", text, flags=re.IGNORECASE)
    text = re.sub(r"\ball'Standard\b", "allo Standard", text, flags=re.IGNORECASE)
    text = re.sub(r"\bsull'Standard\b", "sullo Standard", text, flags=re.IGNORECASE)

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

def normalize_for_tts_en(text: str) -> str:
    """Normalizza testo per sintesi vocale naturale in inglese."""
    
    # 1. Coppie di valute, Simboli e Acronimi fastidiosi
    replacements = [
        ("&", "and"),                      # "Fear & Greed" -> "Fear and Greed"
        ("EUR/USD", "Euro to US Dollar"),
        ("(DXY)", "Dollar Index"),
        ("DXY", "Dollar Index"),
        ("YoY", "Year over Year"),
        ("QoQ", "Quarter over Quarter"),
        ("S&P 500", "S and P 500"),
        ("BTC", "Bitcoin"),
    ]
    for orig, rep in replacements:
        text = text.replace(orig, rep)

    # 2. Gestione dei grandi numeri con il Dollaro ($22.7T, $66K)
    def replace_big_usd_en(m):
        num = m.group(1)
        suffix = m.group(2).upper()
        if suffix == 'T':
            return f"{num} trillion dollars"
        elif suffix == 'B':
            return f"{num} billion dollars"
        elif suffix == 'M':
            return f"{num} million dollars"
        elif suffix == 'K':
            return f"{num} thousand dollars"
        return m.group(0)

    text = re.sub(r'\$([0-9.,]+)([TBMK])\b', replace_big_usd_en, text, flags=re.IGNORECASE)

    # 3. Gestione dei dollari semplici (es. $112.57 -> 112.57 dollars)
    text = re.sub(r'\$([0-9.,]+)', r'\1 dollars', text)
    
    # 4. Percentuali (es. 2.16% -> 2.16 percent) - Vitale per le sequenze!
    text = re.sub(r'([0-9.,]+)%', r'\1 percent', text)

    # 5. Simboli rimasti (come /oz per l'oro)
    text = text.replace("/oz", " per ounce")
    text = text.replace("/barrel", " per barrel")

    # 6. Pulizia di spazi doppi accidentali che potrebbero far balbettare l'audio
    text = re.sub(r'\s+', ' ', text).strip()

    return text
    

def briefing_to_text(briefing, lang='it'):
    """Recupera lo script audio pre-generato dall'AI."""
    text = briefing.get(f'audio_script_{lang}', '')
    if lang == 'it':
        text = text.replace('$/oz', '/oz')  # Prevents "$45/oz" from breaking IT regex
        text = normalize_for_tts(text)
    elif lang == 'en':
        text = normalize_for_tts_en(text)
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
                    voice.synthesize(p, wav_file, length_scale=1.1)
                
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
