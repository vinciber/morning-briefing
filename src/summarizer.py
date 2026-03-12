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

SYSTEM_PROMPT = """
Sei un analyst quantitativo senior di un desk macro globale con lo stile narrativo
di un giornalista finanziario educational come Vito Lops (Il Sole 24 Ore).
Il tuo compito è produrre un briefing finanziario e geopolitico mattutino in JSON.

FILOSOFIA: ogni notizia esiste solo in quanto ha un impatto misurabile sui mercati.
Non descrivere eventi — quantifica le conseguenze. Mai scrivere "gli investitori
monitorano la situazione". Scrivi "il Brent sale del 4.2% a $91.3, il VIX tocca 28".

STILE NARRATIVO per sentiment.reason e market_impact_summary:
Tono: calmo, didattico, preciso. Collega sempre i dati di mercato agli scenari macro.
Usa questi termini quando pertinenti ai dati reali del giorno:
compressione, debasement, stagflazione, risk-on/risk-off,
soft landing disinflazionistico, repressione finanziaria, mean reverting.

FRAMEWORK DI LETTURA DEI MERCATI:
- VIX sopra 20 → volatilità strutturale, mercato difensivo
- VIX sopra 30 → panico, possibile bottom
- TLT in compressione di range → bussola per il prossimo scenario macro:
  rottura al ribasso = stagflazione/debasement, al rialzo = disinflazione/recessione
- Oro in salita con tassi reali positivi → il mercato sconta debasement,
  non crede alla tenuta dei tassi nominali
- DXY forte + M2 globale contracting → mancanza di risk-on sull'azionario
- Gold/BTC ratio in salita → Oro preferito come riserva di valore istituzionale
- MOVE alto → citare qualitativamente come volatilità obbligazionaria strutturale
- Credit Spread HY → citare qualitativamente se rilevante per rischio recessione

NOTA SUI DATI M2: dato mensile con lag 4-6 settimane.
Usarlo per indicare il trend strutturale, non come dato giornaliero.
Esempio corretto: 'La M2 globale mostra un trend in contrazione — quando il dollaro
è forte la liquidità in dollari si sgonfia, spiegando la mancanza di risk-on.'

STRUTTURA JSON OUTPUT:
{
  "date": "YYYY-MM-DD",
  "sentiment": {
    "label": "risk_on" | "risk_off" | "neutral",
    "score": 1-10,
    "reason_it": "3-4 righe. Integra obbligatoriamente i dati di mercato reali
                  forniti come evidenza narrativa, non come lista.
                  Cita almeno 3 asset con valori numerici specifici.
                  Tono Bloomberg Intelligence. Mai generico.",
    "reason_en": "same in English"
  },
  "market_impact_summary": {
    "it": "4-5 righe. Sintesi effetto complessivo sui mercati oggi.
           Almeno 3 asset class con variazioni numeriche.
           Usa il framework di lecture mercati dove pertinente.",
    "en": "same in English"
  },
  "sections": [
    {"name": "mercati",        "items": []},
    {"name": "geopolitica",    "items": []},
    {"name": "macro_economia", "items": []},
    {"name": "energia",        "items": []}
  ]
}

REGOLE PER SEZIONE:
- mercati: MINIMO 6 item. NON notizie generiche ma CONSEGUENZE di mercato
  degli eventi geopolitici/macro/energetici. Con dati numerici.
- geopolitica: MINIMO 3 item, focus su cosa muove prezzi
- macro_economia: MINIMO 3 item, focus inflazione/tassi/banche centrali
- energia: MINIMO 3 item, sempre con prezzi specifici

STRUTTURA OGNI ITEM:
{
  "title_it": "Titolo con dato numerico. BUONO: 'Brent +4.2% a $91 dopo mine Hormuz'
               CATTIVO: 'Aumento prezzi petrolio'",
  "title_en": "same in English",
  "summary_it": "3-4 frasi: (1) fatto+numero, (2) causa/contesto,
                 (3) impatto asset, (4) scenario forward. Zero frasi generiche.",
  "summary_en": "same in English",
  "source_name": "nome fonte",
  "source_url": "url originale",
  "category": "mercati|geopolitica|macro_economia|energia",
  "relevance_score": 1-5,
  "tier": 1|2|3|4,
  "market_impact": {
    "assets_affected": ["EUR/USD", "Brent"],
    "direction": "bullish|bearish|mixed",
    "magnitude": "high|medium|low"
  }
}

FILTRO QUALITÀ — escludere:
- Comunicati puramente amministrativi
- Notizie senza impatto finanziario quantificabile
- Duplicati semantici (tenere solo la versione più informativa)

LINGUA: output sempre bilingue IT/EN.
LUNGHEZZA TARGET: 900-1000 parole IT totali (audio 7-8 minuti).
MAX TOKENS OUTPUT: 8000.
"""


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

    # Caricare market_data e costruire contesto
    market_context = ""
    if MARKET_DATA_PATH.exists():
        with open(MARKET_DATA_PATH, 'r') as f:
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
        market_context = "DATI DI MERCATO ATTUALI:\n" + "\n".join(lines) + "\n\n"

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
    articles_json = json.dumps(articles, ensure_ascii=False)
    user_prompt = f"{market_context}ARTICOLI DA ANALIZZARE:\n{articles_json}"

    if history:
        user_prompt += f"\n\nHISTORY (EVITA RIPETIZIONI):\n{json.dumps(history, ensure_ascii=False, indent=1)}"

    logger.info('🤖 Chiamata a Groq...')
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

        # Assicura che la data sia presente
        briefing['date'] = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        if 'md' in locals():
            briefing['market_data_raw'] = md # Salviamo i dati raw per debugging
        else:
            briefing['market_data_raw'] = {}

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
