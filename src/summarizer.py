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
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

from groq import Groq

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
SYSTEM_PROMPT = """
Sei un analyst quantitativo senior con lo stile di Vito Lops (Il Sole 24 Ore).
Produci un briefing mattutino JSON con tre componenti:

1. SENTIMENT di mercato
2. MARKET IMPACT SUMMARY
3. AUDIO SCRIPT per podcast (7-8 minuti)

REGOLA CRITICA — relevance_score (0.1 - 1.0):
- Analizza gli articoli forniti. Se un articolo è marginale o rumore, assegna un punteggio basso (<0.5).
- Se un articolo è "market moving" o cambia il paradigma, assegna un punteggio alto (>0.8).
- DISTRIBUZIONE OBBLIGATORIA: Massimo 2 articoli con "relevance_score": 1.0 (rilevanza estrema), massimo 3 articoli tra 0.8 e 0.9.

REGOLA CRITICA — market_impact.direction:
"direction" indica l'impatto netto sul SENTIMENT, NON la direzione del prezzo.
  VIX in aumento          → SEMPRE "bearish"
  VIX in calo             → "bullish"
  Petrolio in spike       → "bearish" per equity
  Petrolio in calo        → "bullish" per equity
  DXY forte               → "bearish" per risk assets
  DXY debole              → "bullish" per commodities/EM
  TLT in calo             → "bearish"
  TLT in salita           → "bullish"
  Gold in salita          → "mixed"
  Fed hawkish             → "bearish"
  Crisi geopolitica       → "bearish"
  PIL/occupazione positivi → "bullish"
  Inflazione sopra attese → "bearish"

FRAMEWORK MERCATI:
- VIX>20 = mercato difensivo, VIX>30 = panico
- TLT compressione = bussola macro
- Oro + tassi reali positivi = debasement
- DXY forte + M2 contracting = no risk-on
- M2: dato mensile con lag 4-6 settimane, usare solo per trend strutturale

STILE: calmo, didattico, preciso. Cita sempre valori numerici specifici.
Termini: compressione, debasement, stagflazione, risk-on/risk-off,
soft landing disinflazionistico, repressione finanziaria, mean reverting.

OUTPUT JSON:
{
  "date": "YYYY-MM-DD",
  "sentiment": {
    "label": "risk_on | risk_off | neutral",
    "score": 1-10,
    "reason_it": "3-4 righe. Almeno 3 asset con valori numerici. Mai generico.",
    "reason_en": "same in English"
  },
  "market_impact_summary": {
    "it": "4-5 righe. Almeno 3 asset class con variazioni numeriche.",
    "en": "same in English"
  },
  "audio_script_it": "Script completo per podcast 7-8 minuti in italiano. MINIMO 800-1000 PAROLE. Tono Bloomberg radio. Mai elenchi puntati. Espandi ogni sezione analizzando le implicazioni e non limitarti a leggere i dati.",
  "audio_script_en": "same in English, minimum 800-1000 words. Expand sections to ensure length."
}
"""
# A TARGET: 900-1000 parole IT totali (audio 7-8 minuti).
# MAX TOKENS OUTPUT: 8000.


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

    # Costruisci contesto mercati
    market_context = ""
    md = {}
    if MARKET_DATA_PATH.exists():
        with open(MARKET_DATA_PATH, 'r', encoding='utf-8') as f: # Added encoding for consistency
            md = json.load(f)
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
            'nikkei':    'NIKKEI',
            'shanghai':  'SHANGHAI',
            'btp_10y':   'BTP 10Y',
            'global_m2': 'Global M2 Liquidity (proxy mensile)',
        }
        for key, label in labels.items():
            item = md.get(key, {})
            val = item.get('value', 'N/A')
            chg = item.get('change', 'N/A')
            if val and val != 'N/A':
                lines.append(f"  {label}: {val} ({chg})")
        
        # Aggiungi macro calendar al contesto
        macro = md.get('macro_calendar', {})
        if macro:
            lines.append('\nDATI MACRO USA:')
            for key, item in macro.items():
                label = item.get('label', key)
                if item.get('status') == 'released':
                    val = item.get('value', 'N/A')
                    prev = item.get('previous', 'N/A')
                    date = item.get('release_date', '')
                    lines.append(f"  {label}: {val} (prec. {prev}) — rilasciato {date}")
                elif item.get('status') == 'upcoming':
                    next_rel = item.get('next_release', 'N/A')
                    lines.append(f"  {label}: NON ANCORA RILASCIATO — prossima uscita {next_rel}")
                    
        market_context = "DATI DI MERCATO ATTUALI:\n" + "\n".join(lines) + "\n\n"

    # Carica history (briefing di ieri) per evitare ripetizioni
    history = {}
    if HISTORY_PATH.exists():
        try:
            with open(HISTORY_PATH, 'r', encoding='utf-8') as f:
                history = json.load(f)
        except Exception:
            pass

    logger.info(f'📰 Caricati {len(articles)} articoli.')

    client = Groq(api_key=GROQ_API_KEY)

    articles_json = json.dumps(articles, ensure_ascii=False)
    user_prompt = f"{market_context}ARTICOLI DA ANALIZZARE:\n{articles_json}"

    if history:
        # Passa solo i titoli della history per non sprecare token
        history_titles = []
        for art in history.get('articles', []):
            t = art.get('title_it', '')
            if t:
                history_titles.append(t)
        if history_titles:
            user_prompt += f"\n\nHISTORY TITOLI GIÀ PUBBLICATI (EVITA RIPETIZIONI):\n" \
                           + "\n".join(f"- {t}" for t in history_titles)

    logger.info('🤖 Chiamata a Groq Llama 4 Scout...')
    try:
        response = client.chat.completions.create(
            model='meta-llama/llama-4-scout-17b-16e-instruct',
            messages=[
                {'role': 'system', 'content': SYSTEM_PROMPT},
                {'role': 'user',   'content': user_prompt},
            ],
            temperature=0.2,
            max_tokens=8000,
            response_format={'type': 'json_object'},
        )
        raw_text = response.choices[0].message.content.strip()
        briefing = json.loads(raw_text)

        briefing['date'] = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        briefing['market_data_raw'] = md
        briefing.pop('macro_calendar', None) # Remove it if LLM generated it top-level
        briefing['articles'] = articles # Iniezione articoli raw per architettura feed

        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
            json.dump(briefing, f, ensure_ascii=False, indent=2)

        # Log stats
        logger.info(f'✅ Briefing salvato con {len(articles)} articoli raw.')

        return briefing

    except Exception as e:
        logger.error(f'❌ Errore Groq: {e}')
        return None


if __name__ == '__main__':
    run()
