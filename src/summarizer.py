#!/usr/bin/env python3
"""
summarizer.py — AI Processing con Gemini Flash
Legge data/fetched_articles.json, invia batch a Gemini 1.5 Flash,
produce briefing strutturato JSON bilingue con sentiment.
Output: data/briefing_today.json
"""

import os
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

SYSTEM_PROMPT = '''
Sei un analista finanziario e geopolitico senior. Ricevi una lista di
articoli in JSON e opzionalmente un briefing del giorno precedente per evitare ripetizioni.
Produci un briefing strutturato in formato JSON.

REGOLE ASSOLUTE:
1. Mantieni SEMPRE il campo 'source_url' per ogni notizia citata — è obbligatorio
2. Ogni summary deve essere fattico, non speculativo — cita solo fatti verificabili
3. Produci testo in ENTRAMBE le lingue: 'summary_it' e 'summary_en', 'title_it' e 'title_en'
4. Aggiungi sentiment: risk_on / risk_off / neutral con motivazione in entrambe le lingue
5. Rispondi SOLO con JSON valido, nessun testo extra (no backticks, no markdown)
6. Usa i dati di mercato forniti nel prompt se presenti. Se mancano, usa "N/A".
7. Ordina le notizie per importanza (5 = massima, 1 = minima)
8. Il campo 'importance' è un intero da 1 a 5
9. Le sezioni devono essere: mercati, geopolitica, macro_economia, energia
10. Per ogni item includi: title_it, title_en, summary_it, summary_en, source_name, source_url, importance
11. **FRESHNESS RULE**: Non ripetere notizie che erano già presenti nel briefing di ieri (fornito come history). Se una notizia è un aggiornamento importante di una vecchia storia, focalizzati solo sulle NOVITÀ.
12. ARTICOLI: produci TUTTI gli articoli rilevanti ricevuti, minimo 3 per sezione.
    Non tagliare notizie importanti — l'obiettivo è un briefing completo e ricco.
13. SUMMARY: ogni summary deve essere di almeno 3-4 frasi. Includi: fatto principale,
    contesto, implicazioni per mercati o geopolitica. Non essere generico.
14. FILTRO QUALITÁ: escludi solo comunicati puramente burocratici (nomine, procedure
    amministrative). Includi tutto ciò che ha rilevanza economica o geopolitica.
15. TITOLI: usa titoli descrittivi e specifici. Evita titoli generici come "Aggiornamento Mercati".

FORMATO OUTPUT ATTESO:
{
  "date": "YYYY-MM-DD",
  "sentiment": {
    "label": "risk_off",
    "reason_it": "...",
    "reason_en": "..."
  },
  "market_data": {
    "eur_usd": "1.0850",
    "vix": "18.5",
    "btp_10y": "3.65%",
    "gold": "2,150",
    "oil_brent": "82.30",
    "sp500_futures": "5,230"
  },
  "sections": [ ... ]
}
'''


def run():
    """Pipeline principale: carica articoli + market + history → Groq → salva briefing JSON."""
    if not GROQ_API_KEY:
        logger.error('❌ GROQ_API_KEY non configurata!')
        return None

    # Carica articoli
    if not INPUT_PATH.exists():
        logger.error(f'❌ File non trovato: {INPUT_PATH}')
        return None

    with open(INPUT_PATH, 'r', encoding='utf-8') as f:
        articles = json.load(f)

    if not articles:
        logger.warning('⚠️ Nessun articolo da processare')
        return None

    # Carica dati di mercato reali
    market_data = {}
    if MARKET_DATA_PATH.exists():
        with open(MARKET_DATA_PATH, 'r', encoding='utf-8') as f:
            market_raw = json.load(f)
            # Formattiamo per l'AI
            market_data = {
                "eur_usd": market_raw.get("eur_usd", {"value": "N/A", "change": "N/A"}),
                "vix": market_raw.get("vix", {"value": "N/A", "change": "N/A"}),
                "btp_10y": market_raw.get("btp_10y", {"value": "N/A", "change": "N/A"}),
                "gold": market_raw.get("gold", {"value": "N/A", "change": "N/A"}),
                "oil_brent": market_raw.get("oil_brent", {"value": "N/A", "change": "N/A"}),
                "sp500_futures": market_raw.get("sp500", {"value": "N/A", "change": "N/A"}),
                "stoxx_600": market_raw.get("stoxx_600", {"value": "N/A", "change": "N/A"}),
                "nikkei": market_raw.get("nikkei", {"value": "N/A", "change": "N/A"}),
                "shanghai": market_raw.get("shanghai", {"value": "N/A", "change": "N/A"}),
                "us_10y": market_raw.get("us_10y", {"value": "N/A", "change": "N/A"})
            }

    # Carica history (briefing di ieri)
    history = {}
    if HISTORY_PATH.exists():
        try:
            with open(HISTORY_PATH, 'r', encoding='utf-8') as f:
                history = json.load(f)
        except Exception:
            pass

    logger.info(f'📰 Caricati {len(articles)} articoli e {len(history.get("sections", []))} sezioni di history')

    # Configura Groq
    client = Groq(api_key=GROQ_API_KEY)

    # Prepara prompt
    user_prompt = f'''Ecco gli articoli di oggi ({datetime.now(timezone.utc).strftime('%Y-%m-%d')}):
{json.dumps(articles, ensure_ascii=False, indent=1)}

DATI DI MERCATO REALI (USA QUESTI):
{json.dumps(market_data, indent=1)}

HISTORY (NON RIPETERE QUESTE NOTIZIE):
{json.dumps(history, ensure_ascii=False, indent=1) if history else "Nessuna history disponibile."}
'''

    logger.info('🤖 Chiamata a Groq...')
    try:
        response = client.chat.completions.create(
            model='meta-llama/llama-4-scout-17b-16e-instruct',
            messages=[
                {'role': 'system', 'content': SYSTEM_PROMPT},
                {'role': 'user', 'content': user_prompt},
            ],
            temperature=0.2,
            max_tokens=16000,
            response_format={'type': 'json_object'},
        )
        raw_text = response.choices[0].message.content.strip()
        briefing = json.loads(raw_text)

        # Assicura che la data sia presente
        briefing['date'] = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        briefing['market_data_raw'] = market_data # Salviamo i dati raw per debugging

        # Salva output
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
            json.dump(briefing, f, ensure_ascii=False, indent=2)

        return briefing
    except Exception as e:
        logger.error(f'❌ Errore Groq: {e}')
        return None


if __name__ == '__main__':
    run()
