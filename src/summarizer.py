#!/usr/bin/env python3
"""
summarizer.py — AI Processing con Groq Llama 4
Legge data/fetched_articles.json, invia batch a Groq,
produce briefing strutturato JSON bilingue con sentiment.
Output: data/briefing_today.json
"""

import os
import sys
import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
import time

load_dotenv()

from groq import Groq

# Ground Truth Macroeconomico (Costanti aggiornate manualmente ogni 6-8 settimane)
MACRO_GROUND_TRUTH = {
    'ECB': {
        'last_meeting': '19 Marzo 2026',
        'next_meeting': '30 Aprile 2026',
        'main_rate': '2.15%',
        'deposit_rate': '2.00%',
        'result': 'Tassi invariati (Main: 2.15%, Deposit: 2.00%)'
    },
    'FED': {
        'last_meeting': '18 Marzo 2026',
        'next_meeting': '07 Maggio 2026',
        'rate_range': '3.50% - 3.75%',
        'result': 'Tassi invariati (3.50% - 3.75%)'
    }
}

def _format_value(val: str) -> str:
    """Tronca decimali a 2 cifre: 27.1900 → 27.19"""
    val_str = str(val)
    if '.' in val_str:
        return re.sub(r'(\d+)\.(\d{2})\d+', r'\1.\2', val_str)
    return val_str

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
INPUT_PATH = ROOT / 'data' / 'fetched_articles.json'
MARKET_DATA_PATH = ROOT / 'data' / 'market_data.json'
HISTORY_PATH = ROOT / 'docs' / 'api' / 'today.json'
OUTPUT_PATH = ROOT / 'data' / 'briefing_today.json'

GROQ_API_KEY = os.environ.get('GROQ_API_KEY', '')

MACRO_GROUND_TRUTH = {
    'ECB': {
        'main_rate': '2.15%',     # Refi (Mutui)
        'deposit_rate': '2.00%',  # DFR (Monitorato dai mercati)
        'last_meeting': '2026-04-02',
        'next_meeting': '2026-04-30',
        'stance_it': 'I tassi sono stati mantenuti invariati nell\'ultima riunione.',
        'stance_en': 'Rates were kept unchanged in the last meeting.'
    },
    'FED': {
        'rate_range': '3.50% - 3.75%',
        'last_meeting': '2026-03-18',
        'next_meeting': '2026-05-07',
        'stance_it': 'La Fed ha mantenuto i tassi fermi segnalando cautela.',
        'stance_en': 'The Fed kept rates steady signaling caution.'
    }
}

SYSTEM_PROMPT = """
Sei un analyst quantitativo senior con lo stile di Vito Lops (Il Sole 24 Ore).
Produci un briefing mattutino JSON con quattro componenti:

1. SENTIMENT di mercato
2. MARKET IMPACT SUMMARY
3. AUDIO SCRIPT per podcast (7-8 minuti)
4. ARTICLE IMPACTS — giudizio per ogni articolo

REGOLA CRITICA — MACRO TRUTH (BCE/FED):
- Per la BCE (ECB), il tasso monitorato dai mercati è il DEPOSIT FACILITY RATE (2.00%).
- Il tasso principale di rifinanziamento (Refi) è al 2.15%, ma viene citato solo per il contesto dei mutui immobiliari.
- Usa SOLO le date del [MACRO GROUND TRUTH] per riferirti a riunioni passate o future.
- Tassi BCE: Ultima 2 Aprile, Prossima 30 Aprile. Tasso attuale (Depositi) 2.00%.

REGOLA CRITICA — market_impact.direction:
"direction" indica l'impatto netto sul SENTIMENT DI MERCATO, NON la direzione del prezzo.

TABELLA OBBLIGATORIA:
  VIX in aumento             → "bearish"
  VIX in calo                → "bullish"
  Petrolio in spike          → "bearish" (inflazione, recessione)
  Petrolio in calo           → "bullish" per equity
  DXY forte                  → "bearish" per risk assets ed EM
  DXY debole                 → "bullish" per commodities e EM
  TLT in calo (tassi salgono)→ "bearish"
  TLT in salita              → "bullish"
  Gold in salita             → "mixed"
  Fed hawkish / tassi alti   → "bearish"
  Crisi geopolitica / guerra → "bearish"
  De-escalation              → "bullish"
  PIL/occupazione positivi   → "bullish"
  Inflazione sopra attese    → "bearish"
  Inflazione sotto attese    → "bullish"
  Sanzioni / blocco commercio→ "bearish"
  Accordo commerciale        → "bullish"

FRAMEWORK MERCATI:
- VIX>20 = mercato difensivo, VIX>30 = panico
- TLT compressione = bussola macro
- Oro + tassi reali positivi = debasement
- DXY forte + M2 contracting = no risk-on
- M2: dato mensile con lag 4-6 settimane, usare solo per trend strutturale

SCORING QUANTITATIVO (OBBLIGATORIO per "score" 1-10):
Lo score deve riflettere i DATI NUMERICI, non solo il contesto narrativo/geopolitico.
- VIX < 18 E S&P positivo → score risk_off MAX 3, label "neutral" o "risk_on"
- VIX 18-25 → score risk_off MAX 6
- VIX > 30 → score risk_off MIN 7
- S&P green > +1% → score risk_off MAX 4 (a meno di VIX > 30)
- Se VIX < 20 E S&P positivo E DXY stabile → il label DEVE essere "neutral" o "risk_on"
Il geopolitico contribuisce al massimo 2 punti — i mercati prezzano il rischio meglio delle notizie.

STILE: calmo, didattico, preciso. Cita sempre valori numerici specifici.
Termini da usare quando pertinenti:
compressione, debasement, stagflazione, risk-on/risk-off,
soft landing disinflazionistico, repressione finanziaria, mean reverting.

REGOLA LINGUAGGIO GEOPOLITICO:
- Usare il linguaggio dei fatti, non diplomatico. 
- Se gli articoli parlano di "war", "bombing", "conflict" → scrivere "guerra", "conflitto in corso", "bombardamenti"
- MAI attenuare con "potenziale", "possibile", "rischio di" se l'evento è già in corso
- Esempio SBAGLIATO: "Iran e Israele coinvolti in un potenziale conflitto"
- Esempio CORRETTO: "la guerra tra USA-Israele e Iran, al sedicesimo giorno, continua a pesare sui mercati"

REGOLA TEMPORALE: Se i mercati sono chiusi oggi o lo sono stati ieri (weekend/festività), non usare MAI il termine "ieri" o "yesterday" per riferirti ai dati dell'ultima sessione. Usa invece termini come "alla chiusura passata", "nella seduta di venerdì" o simili.

REGOLA DATI MACRO (EVITA ALLUCINAZIONI): Se nel contesto vedi un dato etichettato come "DATO STORICO/CONSOLIDATO" (es. rilasciato più di 15 giorni fa), NON usare MAI espressioni come "sono stati rilasciati di recente dati" o "oggi è uscito". Trattalo esclusivamente come contesto di sfondo o base di partenza.

OUTPUT JSON — struttura esatta:
{
  "date": "YYYY-MM-DD",
  "sentiment": {
    "label": "risk_on | risk_off | neutral",
    "score": 1-10,
    "reason_it": "3-4 righe narrative. Almeno 3 asset con valori numerici. Tono Bloomberg Intelligence. Mai generico. Collega i dati agli scenari macro. RISPETTA LA REGOLA TEMPORALE.",
    "reason_en": "Same in English. RESPECT TEMPORAL RULE."
  },
  "market_impact_summary": {
    "it": "4-5 righe. Almeno 3 asset class con variazioni numeriche. Usa il framework di lettura mercati. RISPETTA LA REGOLA TEMPORALE.",
    "en": "Same in English. RESPECT TEMPORAL RULE."
  },
  "audio_script_it": "Script completo gestito in segmenti (A, B, C).",
  "audio_script_en": "Script completo gestito in segmenti (A, B, C).",
  "article_impacts": [
    {
      "url": "url esatto dell'articolo",
      "title_it": "Titolo tradotto in italiano — OBBLIGATORIO anche se fonte è inglese. NON copiare l'originale inglese.",
      "summary_it": "Sintesi in italiano (40-60 parole). OBBLIGATORIO tradurre anche se fonte è inglese.",
      "direction": "bearish | bullish | mixed",
      "magnitude": "high | medium | low",
      "assets_affected": ["S&P 500", "Brent"]
    }
  ]
}

REGOLA TRADUZIONE: Per ogni articolo in article_impacts, title_it e summary_it DEVONO essere in italiano.
Se la fonte è in inglese, DEVI tradurre. Non è accettabile restituire title_it uguale al titolo inglese originale.
summary_it deve contenere la sintesi del contenuto dell'articolo, non solo il titolo.
"""

    

AUDIO_FINANCE_PROMPT = """Sei un conduttore radiofonico finanziario italiano. Stile: conciso, direzionale, zero filler.
Scrivi lo script audio per la prima parte del podcast (MERCATI TRADIZIONALI E MACRO).
LUNGHEZZA: 350-500 parole (preferisci brevità e densità. Se non ci sono eventi rilevanti per una sezione, riducila a 2-3 frasi. MAI riempire con contenuto generico o ripetitivo).

STRUTTURA:
1. APERTURA (30 parole max): Saluto professionale secco.
   "Buongiorno, benvenuti al Morning Briefing di Price Alert."
2. CONTESTO ASIATICO (80 parole): Chiusura Nikkei e Shanghai — solo direzione e percentuale.
   Esempio: "Il Nikkei ha chiuso in calo dell'1.2%, Shanghai in rialzo dello 0.5%."
   Poi collegamento: "Questo suggerisce un'apertura debole per i mercati europei" o simile.
3. MERCATI OCCIDENTALI (150 parole): S&P 500, VIX, DXY, oro, petrolio, US 10Y Yield, BTP 10Y.
   REGOLA CHIAVE: cita direzione + variazione percentuale, e AGGIUNGI una frase narrativa per contesto (es. "mossa legata ai timori sull'inflazione", "dopo i dati macro deludenti").
   Per i prezzi assoluti: SOLO alle soglie psicologiche VERE (es. "l'oro sopra i 4500 dollari", "VIX sotto quota 20", "petrolio sotto i 100 dollari"). MAI soglie arbitrarie tipo "sotto i 101 dollari".
   REGOLA TEMPORALE: Se i mercati sono chiusi oggi, usa "nella seduta di [GIORNO]" o "alla chiusura passata", MAI "ieri".
4. GEOPOLITICA (100 parole): Solo eventi con impatto concreto sui prezzi. Aggiungi 1 frase di contesto/conseguenza.
5. MACRO E BANCHE CENTRALI (80 parole): Solo dati freschi o attesi a breve.
   IMPORTANTE: Se nel contesto è indicato "MACRO OGGI: nessuno" o non ci sono rilasci programmati oggi, di' "nessun dato macro rilevante in uscita oggi" — NON inventare PIL o CPI.

VIETATO ASSOLUTO:
- NON parlare di Bitcoin, Altcoin, flussi ETF o Fear & Greed.
- NON chiudere il podcast.
- NON fare elenchi puntati.
- NON ripetere lo stesso dato in più sezioni.
- NON inventare dati macro non presenti nel contesto.

NUMERI — REGOLE FORMATO (critico per TTS):
- Usa variazioni percentuali nel parlato (es. "in calo dell'1.5%"). Niente zeri inutili: "2%" non "2,00%", "4,3%" non "4,30%".
- Decimali con virgola in italiano (es. "4,32%"), NON punto.
- Soglia 0,00% → scrivi "pressoché invariato" o "stabile".
- Per grandi numeri: scrivi "settantaduemila dollari", MAI "72,000" o "72.000" (il TTS li legge male).
- Se devi citare un prezzo, scrivi il numero in lettere o in cifre senza separatori di migliaia.
- Percentuali: massimo un decimale (es. 2.7%, non 2.69%).
- ORO: "l'oncia" invece di "/oz".
"""

AUDIO_CRYPTO_PROMPT = """Sei un analista di digital assets. Stile: conciso, direzionale.
Scrivi lo script audio per la sezione CRIPTOVALUTE del podcast.
LUNGHEZZA: 200-300 parole (preferisci brevità. Se non ci sono movimenti significativi, riduci).

FORMATO OUTPUT OBBLIGATORIO: restituisci SOLO un oggetto JSON con la chiave "audio_script_it" il cui valore è una STRINGA di testo continuo (NO liste, NO oggetti nidificati, NO dizionari).

TRANSITION OBBLIGATORIA (prima frase): "Passiamo ora al comparto degli asset digitali..."

STRUTTURA (scrivi come testo narrativo continuo, non come lista):
1. BITCOIN (80 parole): Direzione, variazione %, flussi ETF se significativi. Aggiungi una frase narrativa che colleghi il movimento al contesto.
   Cita il prezzo solo per soglie psicologiche (es. "Bitcoin si mantiene sopra gli ottantamila dollari").
2. ALTCOINS (80 parole): Ethereum, Solana, BNB — solo se ci sono movimenti rilevanti (>2%).
3. SENTIMENT (50 parole): Fear & Greed valore, classe, e 1 frase di interpretazione ("suggerisce cautela sugli investitori").

REGOLE:
- MAI l'articolo determinativo davanti a Bitcoin.
- NON ripetere dati della sezione macro.
- NON chiudere il podcast (verrà fatto nella sezione CHIUSURA).

NUMERI — REGOLE FORMATO (critico per TTS):
- Per grandi soglie tonde: scrivi in lettere (es. "ottantamila dollari"). Per prezzi specifici: usa il numero con virgola decimale ("85,49 dollari"). MAI "80,000" o "80.000".
- Percentuali: massimo un decimale, virgola decimale italiana ("0,4%" non "0.4%").
- Valore 0% (0,00%) → "pressoché invariato" o "stabile". NON "zero virgola zero zero percento".
"""

AUDIO_FINANCE_PROMPT_EN = """You are a concise financial radio presenter. Style: directional, no filler.
Write the audio script for the first part of the podcast (TRADITIONAL MARKETS & MACRO).
LENGTH: 300-450 words (prefer brevity. If a section lacks notable events, keep it to 2-3 sentences).

OPENING: "Good morning, welcome to the Price Alert Morning Briefing."

STRUCTURE:
1. ASIAN CLOSE (60 words): Nikkei, Shanghai direction + % change. Link to European/US outlook.
2. WESTERN MARKETS (150 words): S&P 500, VIX, DXY, gold, oil, US 10Y Yield, BTP 10Y — DIRECTION and % only.
   Mention absolute prices ONLY at key psychological thresholds (e.g. "gold holds above $4,500/oz", "VIX dropped below 20", "oil below $100"). Use the REAL threshold, not arbitrary numbers.
   TEMPORAL RULE: If markets were closed, say "at the last close" or "on [Friday]", NEVER "yesterday".
   ADD 1 narrative sentence per block linking move to a driver (e.g. "driven by recession fears").
3. GEOPOLITICS & MACRO (100 words): Only events with direct price impact.
   IMPORTANT: If the context shows "MACRO TODAY: none" or no scheduled releases for today, say "no major data releases scheduled today" — DO NOT invent macro data.

NUMBERS — FORMAT (critical for TTS):
- Use numeric form: "$4,703", "+0.41%", "98.79". Commas OK for thousands.
- NEVER spell numbers in words (avoid "seventy-seven thousand" unless a clean round threshold).
- Percentages: max 1 decimal. No trailing zeros ("2%" not "2.00%"). If 0.00%, say "essentially flat".

PROHIBITED:
- Do NOT mention Cryptocurrencies, Bitcoin, ETF flows, or Fear & Greed.
- Do NOT close the podcast.
- Do NOT use bullet points.
- Do NOT invent macro data releases not listed in the context.
"""

AUDIO_CRYPTO_PROMPT_EN = """You are a concise digital assets analyst.
Write the audio script for the CRYPTO section of the podcast.
LENGTH: 200-300 words (prefer brevity).

TRANSITION: "Let's pivot to the cryptocurrency markets..."

STRUCTURE:
1. BITCOIN (80 words): Direction, % change, ETF flows only if significant. Mention price only at key thresholds.
2. ALTCOINS (80 words): ETH, SOL, BNB — only if notable moves (>2%).
3. SENTIMENT (50 words): Fear & Greed value and class — add 1 interpretation sentence.

NUMBERS — FORMAT RULES (critical for TTS quality):
- Use numeric form with decimals normally: "$85.49", "+0.41%", "$4,703" (commas OK for thousands in EN).
- Round thousands should use words only for clean round numbers at key thresholds (e.g. "above eighty thousand dollars" if BTC crosses $80k).
- NEVER spell out every number in words ("eighty-five point four nine dollars" — FORBIDDEN, makes TTS mumble).
- Percentages: max 1 decimal. If value is 0.00%, say "essentially flat" instead.
- No trailing zeros: write "2%" not "2.00%", "4.3%" not "4.30%".
"""

AUDIO_CLOSE_PROMPT = """CHIUSURA OBBLIGATORIA:
- Focus: outlook per domani e cosa monitorare.
- Saluto finale professionale: "Grazie per l'attenzione e a domani", "Un saluto da Price Alert", etc.
- NON terminare MAI con "buon trading".
"""


def _merge_article_impacts(articles: list, article_impacts: list) -> list:
    """
    Merge article_impacts dal LLM negli articoli raw per URL.
    Aggiunge market_impact a ogni articolo che ha un match.
    """
    # Costruisci lookup per URL
    impacts_by_url = {}
    for impact in article_impacts:
        url = impact.get('url', '').strip()
        if url:
            impacts_by_url[url] = {
                'title_it': impact.get('title_it'),
                'title_en': impact.get('title_en'),
                'summary_it': impact.get('summary_it'),
                'summary_en': impact.get('summary_en'),
                'direction': impact.get('direction', 'mixed'),
                'magnitude': impact.get('magnitude', 'low'),
                'assets_affected': impact.get('assets_affected', []),
            }

    matched = 0
    for art in articles:
        url = art.get('url', '').strip()
        if url in impacts_by_url:
            impact = impacts_by_url[url]
            art['title_it'] = impact.get('title_it') or art.get('title')
            art['title_en'] = impact.get('title_en') or art.get('title')
            art['summary_it'] = impact.get('summary_it') or art.get('snippet')
            art['summary_en'] = impact.get('summary_en') or art.get('snippet')
            art['market_impact'] = {
                'direction': impact.get('direction'),
                'magnitude': impact.get('magnitude'),
                'assets_affected': impact.get('assets_affected'),
            }
            matched += 1
        else:
            # Fallback rule-based per null
            cat = art.get('category', '').lower()
            art['title_it'] = art.get('title')
            art['title_en'] = art.get('title')
            art['summary_it'] = art.get('snippet')
            art['summary_en'] = art.get('snippet')
            art['market_impact'] = {
                'direction': 'bearish' if cat in ('geopolitica', 'energia', 'macro') else 'mixed',
                'magnitude': 'low',
                'assets_affected': [],
            }

    logger.info(f'🎯 market_impact: {matched}/{len(articles)} articoli matchati via URL, {len(articles)-matched} via fallback')
    return articles


def run():
    """Pipeline principale: carica articoli + market + history → Groq → salva briefing JSON."""
    if not GROQ_API_KEY:
        logger.error('❌ GROQ_API_KEY non configurata!')
        sys.exit(1)

    if not INPUT_PATH.exists():
        logger.error(f'❌ File non trovato: {INPUT_PATH}')
        return None

    with open(INPUT_PATH, 'r', encoding='utf-8') as f:
        articles = json.load(f)

    if not articles:
        logger.warning('⚠️ Nessun articolo da processare')
        return None

    # Filtra articoli con score troppo basso (rumore) salvando i protetti
    # (PIMCO e fonti settimanali saltano il filtro del 0.3)
    PROTECTED_SOURCES = ['BlackRock Investment Institute', 'Goldman Sachs Insights', 'PIMCO Insights']
    articles = [a for a in articles if a.get('relevance_score', 0) >= 0.3 or a.get('source') in PROTECTED_SOURCES]
    logger.info(f'📰 Articoli post-filtro (>= 0.3 o protetti): {len(articles)}')

    # Definisci weekly sources per i check post-json
    weekly_sources = ['BlackRock Investment Institute', 'Goldman Sachs Insights']

    # Costruisci contesto mercati
    market_context = ""
    md = {}
    if MARKET_DATA_PATH.exists():
        with open(MARKET_DATA_PATH, 'r', encoding='utf-8') as f:
            md = json.load(f)
        
        # INIEZIONE HARD - MACRO TRUTH (BCE/FED)
        # Sovrascriviamo eventuali errori da FRED con i dati del GROUND TRUTH
        if 'macro_calendar_eu' not in md: md['macro_calendar_eu'] = {}
        # Usiamo il DEPOSIT RATE come tasso primario perché è quello che monitorano i mercati
        md['macro_calendar_eu']['ecb_rate'] = {
            'label': 'Tasso BCE (Depositi)',
            'label_it': 'Tasso BCE (Depositi)',
            'label_en': 'ECB Rate (Deposit)',
            'value': MACRO_GROUND_TRUTH['ECB']['deposit_rate'],
            'release_date': '2026-04-02', 
            'status': 'released',
            'next_release': '2026-04-30',
            'region': 'EU'
        }
        md['macro_calendar_eu']['ecb_refi_rate'] = {
            'label': 'Tasso BCE (Refi/Mutui)',
            'label_it': 'Tasso BCE (Refi/Mutui)',
            'label_en': 'ECB Rate (Refi)',
            'value': MACRO_GROUND_TRUTH['ECB']['main_rate'],
            'status': 'released',
            'region': 'EU'
        }
        
        if 'macro_calendar' not in md: md['macro_calendar'] = {}
        md['macro_calendar']['fed_funds'] = {
            'label': 'Tasso Fed Funds',
            'label_it': 'Tasso Fed Funds',
            'label_en': 'Fed Funds Rate',
            'value': MACRO_GROUND_TRUTH['FED']['rate_range'],
            'release_date': '2026-03-18',
            'status': 'released',
            'next_release': '2026-05-07'
        }

        lines = []
        labels = {
            'eur_usd':   'EUR/USD',
            'dxy':       'Dollar Index (DXY)',
            'sp500':     'S&P 500',
            'vix':       'VIX',
            'tlt':       'TLT Bond USA 20Y',
            'us_10y':    'US 10Y Yield',
            'gold':      'GOLD',
            'btcusd':    'Bitcoin',
            'oil_brent': 'BRENT',
            'stoxx_600': 'STOXX 600',
            'nikkei':    'NIKKEI (chiusura Asia — indicatore apertura Europa)',
            'shanghai':  'SHANGHAI (chiusura Asia — indicatore apertura Europa)',
            'hang_seng': 'HANG SENG (Hong Kong)',
            'btp_10y':   'BTP 10Y',
            'global_m2': 'Global M2 Liquidity (proxy mensile)',
            'btc_etf_flow': 'BTC ETF Daily Net Inflow',
        }
        for key, label in labels.items():
            item = md.get(key, {})
            val = _format_value(item.get('value', 'N/A'))
            chg = _format_value(item.get('change', 'N/A'))
            if val and val != 'N/A':
                lines.append(f"  {label}: {val} ({chg})")

        # Aggiungi Crypto Data
        crypto = md.get('crypto', {})
        if crypto:
            lines.append('\nCRYPTO MARKET DATA:')
            fg = crypto.get('fear_greed', {})
            lines.append(f"  Fear & Greed Index: {fg.get('value', 'N/A')} ({fg.get('class', 'N/A')})")
            prices = crypto.get('prices', {})
            for ticker, pinfo in prices.items():
                lines.append(f"  {ticker}: {pinfo.get('value', 'N/A')} ({pinfo.get('change', 'N/A')})")

        # Aggiungi macro calendar al contesto
        macro = md.get('macro_calendar', {})
        if macro:
            lines.append('\nDATI MACRO USA:')
            for key, item in macro.items():
                label = item.get('label', key)
                if item.get('status') == 'released':
                    val = _format_value(item.get('value', 'N/A'))
                    prev = _format_value(item.get('previous', 'N/A'))
                    date = item.get('release_date', '')
                    
                    try:
                        release_dt = datetime.strptime(date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                        days_ago = (datetime.now(timezone.utc) - release_dt).days
                        if days_ago <= 14:
                            freshness = f"rilasciato {days_ago} giorni fa ⚡ RECENTE"
                        else:
                            freshness = f"rilasciato il {date} ({days_ago} giorni fa — DATO STORICO/CONSOLIDATO)"
                    except Exception:
                        freshness = f"rilasciato {date}"
                    
                    lines.append(f"  {label}: {val} (prec. {prev}) — {freshness}")
                elif item.get('status') == 'upcoming':
                    next_rel = item.get('next_release', 'N/A')
                    lines.append(f"  {label}: NON ANCORA RILASCIATO — prossima uscita {next_rel}")

        # Aggiungi macro EU
        macro_eu = md.get('macro_calendar_eu', {})
        if macro_eu:
            lines.append('\nDATI MACRO EUROZONA:')
            for key, item in macro_eu.items():
                label = item.get('label', key)
                if item.get('status') == 'released':
                    val  = _format_value(item.get('value', 'N/A'))
                    prev = _format_value(item.get('previous', 'N/A'))
                    date = item.get('release_date', '')
                    try:
                        release_dt = datetime.strptime(date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                        days_ago   = (datetime.now(timezone.utc) - release_dt).days
                        freshness  = f"rilasciato il {date} ({days_ago}gg fa — DATO STORICO/CONSOLIDATO)" \
                                     if days_ago > 14 else f"rilasciato {days_ago}gg fa ⚡ RECENTE"
                    except Exception:
                        freshness = f"rilasciato {date}"
                    lines.append(f"  {label}: {val} (prec. {prev}) — {freshness}")
                elif item.get('status') == 'upcoming':
                    lines.append(f"  {label}: NON RILASCIATO — prossima uscita {item.get('next_release')}")

        market_context = "DATI DI MERCATO ATTUALI:\n" + "\n".join(lines) + "\n\n"

    # Costruisci flag "MACRO OGGI" per prevenire hallucination sui dati macro
    today_iso = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    releases_today = []
    for key, item in (md.get('macro_calendar', {}) or {}).items():
        if item.get('next_release') == today_iso or item.get('release_date') == today_iso:
            releases_today.append(f"USA {item.get('label', key)}")
    for key, item in (md.get('macro_calendar_eu', {}) or {}).items():
        if item.get('next_release') == today_iso or item.get('release_date') == today_iso:
            releases_today.append(f"EU {item.get('label', key)}")
    macro_today_line = (
        f"\nMACRO OGGI ({today_iso}): {', '.join(releases_today)}\n"
        if releases_today else
        f"\nMACRO OGGI ({today_iso}): nessuno (NON inventare dati in uscita oggi)\n"
    )

    audio_market_context = market_context + macro_today_line

    # Carica history — solo titoli per non sprecare token
    history = {}
    if HISTORY_PATH.exists():
        try:
            with open(HISTORY_PATH, 'r', encoding='utf-8') as f:
                history = json.load(f)
        except Exception:
            pass

    client = Groq(api_key=GROQ_API_KEY)

    # Passa solo i campi essenziali al LLM per risparmiare token
    articles_slim = [
        {
            'url':             a.get('url', ''),
            'title':           a.get('title', ''),
            'snippet':         a.get('snippet', '')[:300],  # Max 300 chars
            'category':        a.get('category', ''),
            'source':          a.get('source', ''),
            'tier':            a.get('tier', 4),
            'relevance_score': a.get('relevance_score', 0),
        }
        for a in articles
    ]

    logger.info("\n=== ARTICOLI SELEZIONATI PER L'ANALISI LLM ===")
    for idx, a in enumerate(articles_slim, 1):
        logger.info(f"{idx:02d}. [{a['source']}] {a['title']} (Score: {a['relevance_score']})")
    logger.info("==============================================\n")

    articles_json = json.dumps(articles_slim, ensure_ascii=False)
    
    # Iniezione Ground Truth Macro
    macro_truth_str = f"""
[MACRO GROUND TRUTH - DATA REALE AL {datetime.now().strftime('%d %B %Y')}]
- ECB (BCE): Ultima riunione {MACRO_GROUND_TRUTH['ECB']['last_meeting']}, Prossima riunione {MACRO_GROUND_TRUTH['ECB']['next_meeting']}. {MACRO_GROUND_TRUTH['ECB']['stance_it']} {MACRO_GROUND_TRUTH['ECB']['deposit_rate']} (DFR) / {MACRO_GROUND_TRUTH['ECB']['main_rate']} (Refi).
- FED (USA): Ultima riunione {MACRO_GROUND_TRUTH['FED']['last_meeting']}, Prossima riunione {MACRO_GROUND_TRUTH['FED']['next_meeting']}. {MACRO_GROUND_TRUTH['FED']['stance_it']} {MACRO_GROUND_TRUTH['FED']['rate_range']}.
[FINE GROUND TRUTH]
"""
    
    user_prompt = f"{macro_truth_str}\n\n{market_context}ARTICOLI DA ANALIZZARE ({len(articles_slim)} totali):\n{articles_json}"

    if history:
        history_titles = [
            a.get('title', '')
            for a in history.get('articles', [])
            if a.get('title')
        ]
        if history_titles:
            user_prompt += (
                f"\n\nHISTORY TITOLI GIÀ PUBBLICATI (EVITA RIPETIZIONI):\n"
                + "\n".join(f"- {t}" for t in history_titles[:20])
            )
    
    # Context variables
    now = datetime.now(timezone.utc)
    is_monday = now.weekday() == 0
    weekly_sources = ['BlackRock Investment Institute', 'Goldman Sachs Insights']
    weekly_articles = [a for a in articles_slim if a.get('source') in weekly_sources]

    if is_monday and weekly_articles:
        user_prompt += f"\n\n⚠️ OGGI È LUNEDÌ — REPORT SETTIMANALI DISPONIBILI:\n"
        user_prompt += f"Sono presenti {len(weekly_articles)} articoli da BlackRock Investment Institute e Goldman Sachs Insights.\n"
        user_prompt += "Questi sono report istituzionali settimanali di altissima qualità (tier 1).\n"
        user_prompt += "OBBLIGATORIO: citarli nel sentiment e nel market_impact_summary.\n"
        user_prompt += "Nell'audio script dedicare almeno 2-3 frasi alle view istituzionali di BlackRock e Goldman.\n"

    # Weekend / Holiday Awareness
    is_weekend = now.weekday() >= 5 # 5=Sat, 6=Sun
    
    # 2026 Holidays (Major Markets)
    holidays_2026 = {
        "01-01": "Capodanno",
        "04-03": "Venerdì Santo",
        "04-06": "Lunedì dell'Angelo (Pasquetta)",
        "05-01": "Festa del Lavoro",
        "12-25": "Natale",
        "12-26": "Santo Stefano",
    }
    today_md = now.strftime("%m-%d")
    is_holiday = today_md in holidays_2026
    holiday_name = holidays_2026.get(today_md)

    holiday_warning_it = ""
    holiday_warning_en = ""

    if is_weekend or is_holiday:
        reason_it = "IL FINE SETTIMANA" if is_weekend else f"LA FESTIVITÀ DI {holiday_name.upper()}"
        reason_en = "THE WEEKEND" if is_weekend else f"THE {holiday_name.upper()} HOLIDAY"
        
        holiday_warning_it += f"\n\n⚠️ OGGI I MERCATI TRADIZIONALI SONO CHIUSI PER {reason_it}:\n"
        holiday_warning_it += f"Nota: Oggi le borse azionarie e obbligazionarie mondiali sono chiuse {'per il weekend' if is_weekend else 'per festività'}.\n"
        holiday_warning_it += "Nell'audio script (Parte Finance), menziona esplicitamente che i mercati tradizionali sono chiusi e passa rapidamente all'analisi degli asset digitali (Crypto) che sono aperti 24 ore su 24.\n"
        holiday_warning_it += "Esempio apertura: 'Mentre le borse mondiali osservano la consueta pausa festiva, i riflettori restano accesi sul comparto digitale...' o simili.\n"
        holiday_warning_it += "Concentrati sulla chiusura precedente per il contesto macro, ma dai priorità assoluta ai movimenti attuali di Bitcoin e delle crypto.\n"

        holiday_warning_en += f"\n\n⚠️ TODAY TRADITIONAL MARKETS ARE CLOSED FOR {reason_en}:\n"
        holiday_warning_en += f"Note: Today global stock and bond markets are closed {'for the weekend' if is_weekend else 'for a holiday'}.\n"
        holiday_warning_en += "In the audio script (Finance Part), explicitly mention that traditional markets are closed and quickly pivot to the analysis of digital assets (Crypto) which are open 24/7.\n"
        holiday_warning_en += "Example opening: 'While traditional markets observe their holiday break, the spotlight remains on digital assets...'\n"
        holiday_warning_en += "Focus on the previous close for macro context, but give absolute priority to current Bitcoin and crypto movements.\n"

        user_prompt += holiday_warning_it

    logger.info(f'🤖 Chiamata 1: Groq Llama 4 Analysis ({len(articles_slim)} articoli)...')
    try:
        # CHIAMATA 1 — Sentiment, Market Impact Summary, e Article Impacts
        response = client.chat.completions.create(
            model='meta-llama/llama-4-scout-17b-16e-instruct',
            messages=[
                {'role': 'system', 'content': SYSTEM_PROMPT},
                {'role': 'user',   'content': user_prompt},
            ],
            temperature=0.2,
            max_tokens=4096,
            response_format={'type': 'json_object'},
        )
        raw_text = response.choices[0].message.content.strip()
        briefing = json.loads(raw_text)

        # --- GENERAZIONE AUDIO SCRIPT ---
        today_str = datetime.now(timezone.utc).strftime('%d %B %Y')
        
        # Filtro articoli per weekly
        weekly_it = [a for a in articles_slim if a.get('source') in weekly_sources]
        other_it = [a for a in articles_slim if a.get('source') not in weekly_sources]
        news_it = weekly_it + other_it
        
        # Helper per chiamate audio
        def get_audio_part(system_p, user_p, lang_key, model='meta-llama/llama-4-scout-17b-16e-instruct'):
            # Forza JSON nel prompt utente
            full_user_p = f"{user_p}\n\nREQUISITO CORE: Restituisci SOLO un oggetto JSON con la chiave '{lang_key}'."
            resp = client.chat.completions.create(
                model=model,
                messages=[
                    {'role': 'system', 'content': system_p},
                    {'role': 'user',   'content': full_user_p},
                ],
                temperature=0.3,
                max_tokens=2048,
                response_format={'type': 'json_object'},
            )
            return json.loads(resp.choices[0].message.content)
            
        def clean_script(script_obj, key):
            """Estrae il testo pulito dallo script, gestendo strutture dict/list annidate dall'LLM."""
            if isinstance(script_obj, list) and len(script_obj) > 0:
                script_obj = script_obj[0]

            if not isinstance(script_obj, dict):
                return ""

            content = script_obj.get(key, "")

            def _flatten(val):
                if isinstance(val, str):
                    return val
                if isinstance(val, dict):
                    return "\n\n".join(_flatten(v) for v in val.values() if v)
                if isinstance(val, list):
                    return "\n\n".join(_flatten(v) for v in val if v)
                return ""

            text = _flatten(content)

            # Post-process audio TTS
            import re
            # Rimuovi zeri decimali inutili ("2.00%" → "2%", "4.30" → "4.3")
            text = re.sub(r'(\d+)\.0+(?=%|\b)', r'\1', text)
            text = re.sub(r'(\d+\.\d*?)0+(?=%|\b)', r'\1', text)
            text = re.sub(r'(\d+)\.(?=%|\s|$)', r'\1', text)
            # Rimuovi "ieri" (violazione regola temporale)
            text = re.sub(r'\bieri\b', 'nella seduta precedente', text, flags=re.IGNORECASE)
            text = re.sub(r'\byesterday\b', 'at the last close', text, flags=re.IGNORECASE)
            return text.strip()

        def _safe_get(obj, key, default=""):
            """Safely get a value from a dict OR a single-item list containing a dict."""
            if isinstance(obj, list) and len(obj) > 0:
                obj = obj[0]
            if isinstance(obj, dict):
                return obj.get(key, default)
            return default

        # 1. ITALIANO
        logger.info('🎙️ Generazione Audio IT (3 segmenti)...')
        
        # Access sentiment safely
        sentiment_obj = briefing.get('sentiment', {})
        sentiment_label = _safe_get(sentiment_obj, 'label', 'neutral')
        
        # Part A: Finance
        it_finance_user = f"DATA: {today_str}\nSENTIMENT: {sentiment_label}\nMERCATI:\n{audio_market_context}\nNOTIZIE PRINCIPALI:\n" + \
                         "\n".join(f"- {a['title']}" for a in news_it[:10])
        it_finance_user += holiday_warning_it
        it_finance = get_audio_part(AUDIO_FINANCE_PROMPT, it_finance_user, 'audio_script_it')
        
        # Part B: Crypto
        # Assicurati che lo split includa anche i dati ETF che sono prima di CRYPTO data ma rilevanti
        etf_flow_ctx = f"BTC ETF Daily Net Inflow: {md.get('btc_etf_flow', {}).get('value', 'N/A')}\n"
        # etf_flow_ctx già in formato grezzo — nessun preprocessing necessario
        
        crypto_ctx = (audio_market_context.split('CRYPTO MARKET DATA:')[1] if 'CRYPTO MARKET DATA:' in audio_market_context else audio_market_context)
        it_crypto_user = f"DATI CRYPTO ATTUALI (USA QUESTI VALORI):\n{etf_flow_ctx}{crypto_ctx}\nNOTIZIE CRYPTO:\n" + \
                        "\n".join(f"- {a['title']}" for a in news_it if a.get('category') == 'crypto')
        it_crypto = get_audio_part(AUDIO_CRYPTO_PROMPT, it_crypto_user, 'audio_script_it')
        
        # Part C: Close
        it_close = get_audio_part(AUDIO_CLOSE_PROMPT, "Genera chiusura per podcast finanziario italiano.", 'audio_script_it')
        
        # Merge IT
        briefing['audio_script_it'] = f"{clean_script(it_finance, 'audio_script_it')}\n\n{clean_script(it_crypto, 'audio_script_it')}\n\n{clean_script(it_close, 'audio_script_it')}"

        # 2. ENGLISH — traduzione da IT per garantire coerenza di struttura, dati e lunghezza
        logger.info('🎙️ Generazione Audio EN (traduzione da IT)...')

        translate_prompt = """You are a professional financial radio translator. Translate the Italian podcast script to English preserving:
- Identical structure, section order, and approximate length
- All numbers, percentages, and tickers exactly as in the source
- Same narrative tone (concise, directional)

NUMBER FORMATTING FOR EN TTS (critical):
- Convert Italian decimal commas to EN dots ("4,32%" → "4.32%", "85,49 dollari" → "$85.49").
- Use numeric form (e.g. "$4,703", "+0.41%"). Commas OK for thousands.
- NEVER spell numbers in words unless it's a clean round threshold (e.g. "above eighty thousand dollars").
- 0% / 0.00% → "essentially flat".
- Opening must be "Good morning, welcome to the Price Alert Morning Briefing."
- Keep the transition "Let's pivot to the cryptocurrency markets..." where the Italian has "Passiamo ora al comparto degli asset digitali..."

Return ONLY a JSON object with key "audio_script_en" containing the full English translation as a single string."""

        en_translate_user = f"ITALIAN SCRIPT TO TRANSLATE:\n\n{briefing['audio_script_it']}"
        en_full = get_audio_part(translate_prompt, en_translate_user, 'audio_script_en')
        briefing['audio_script_en'] = clean_script(en_full, 'audio_script_en')

        # Merge article_impacts negli articoli raw
        article_impacts = briefing.pop('article_impacts', [])
        articles_with_impact = _merge_article_impacts(articles, article_impacts)

        # Costruisci briefing finale canonico
        briefing['date'] = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        briefing['market_data_raw'] = md
        briefing['articles'] = articles_with_impact
        briefing.pop('macro_calendar', None)  # Solo dentro market_data_raw
        briefing.pop('sections', None)        # Non più usato
        briefing.pop('importance', None)

        # Log qualità output
        audio_words = len(briefing.get('audio_script_it', '').split())
        logger.info(f'✅ Briefing completato: {len(articles_with_impact)} articoli')
        logger.info(f'🎙️ Audio script IT: {audio_words} parole '
                    f'{"✅" if audio_words >= 500 else "⚠️ SOTTO 500"}')

        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
            json.dump(briefing, f, indent=2, ensure_ascii=False)

        return briefing

    except Exception as e:
        logger.error(f'❌ Errore durante summarizzazione: {e}')
        return None

if __name__ == '__main__':
    run()
