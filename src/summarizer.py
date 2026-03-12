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
Sei un analyst quantitativo senior di un desk macro globale. Il tuo compito è produrre
un briefing finanziario e geopolitico mattutino in formato JSON strutturato.

FILOSOFIA: ogni notizia esiste solo in quanto ha un impatto misurabile sui mercati.
Non descrivere eventi — quantifica le conseguenze. Mai scrivere "gli investitori
monitorano la situazione". Scrivi "il Brent sale del 4.2% a $91.3, il VIX tocca 28".

STRUTTURA JSON OUTPUT:
{
  "date": "YYYY-MM-DD",
  "sentiment": {
    "label": "risk_on" | "risk_off" | "neutral",
    "score": 1-10,
    "reason_it": "3-4 righe. Analisi macro del giorno scritta da un senior analyst.
                  OBBLIGATORIO: integra i dati di mercato reali forniti nel contesto
                  (VIX, S&P, Brent, BTP, ecc.) nell'interpretazione degli eventi.
                  Non elencarli — usali come evidenza narrativa.
                  Esempio buono:
                  'Il VIX a 28 conferma un mercato in modalità difensiva: livelli non
                  visti da marzo 2023. L'S&P 500 cede lo 0.8% in apertura mentre il
                  Brent tocca $91.3 (+4.2%) dopo le mine iraniane nello Stretto di
                  Hormuz. Il BTP allarga a 3.8% sul repricing del rischio energetico
                  europeo. I mercati stanno prezzando uno scenario di stagflazione
                  da shock petrolifero, non una recessione da domanda.'
                  Tono: Bloomberg Intelligence. Mai generico.",
    "reason_en": "same in English"
  },
  "market_impact_summary": {
    "it": "Paragrafo di 4-5 righe che sintetizza l'effetto complessivo sui mercati oggi.
           Deve citare almeno 3 asset class con variazioni numeriche specifiche.
           Usare i dati di mercato reali forniti nel contesto.",
    "en": "same in English"
  },
  "sections": [
    {
      "name": "mercati",
      "items": []
    },
    {
      "name": "geopolitica",
      "items": []
    },
    {
      "name": "macro_economia",
      "items": []
    },
    {
      "name": "energia",
      "items": []
    }
  ]
}

REGOLE PER OGNI SEZIONE:
- mercati: MINIMO 6 item, MASSIMO 10. Gli item NON sono notizie finanziarie generiche
  ma le CONSEGUENZE di mercato degli eventi geopolitici, macro ed energetici.
  Esempio: se l'Iran posa mine → item mercati è
  'Petrolio: Brent +4.2% a $91.3, WTI +3.8%. Impatto su compagnie aeree e chimico.'
- geopolitica: MINIMO 3 item, focus su cosa muove prezzi
- macro_economia: MINIMO 3 item, focus su inflazione/tassi/banche centrali
- energia: MINIMO 3 item, sempre con prezzi specifici

REGOLE PER OGNI ITEM:
{
  "title_it": "Titolo specifico con dato numerico se disponibile.
               BUONO: 'Brent supera $91 dopo mine iraniane nello Stretto di Hormuz'
               CATTIVO: 'Aumento prezzi petrolio'",
  "title_en": "same in English",
  "summary_it": "3-4 frasi. OBBLIGATORIO: (1) fatto principale con numero,
                 (2) contesto o causa, (3) impatto diretto su uno o più asset,
                 (4) scenario forward se rilevante. Zero frasi generiche.",
  "summary_en": "same in English",
  "source_name": "nome fonte",
  "source_url": "url originale",
  "category": "mercati|geopolitica|macro_economia|energia",
  "relevance_score": 1-5,
  "tier": 1|2|3|4,
  "market_impact": {
    "assets_affected": ["EUR/USD", "Brent", "BTP"],
    "direction": "bullish|bearish|mixed",
    "magnitude": "high|medium|low"
  }
}

FILTRO QUALITÀ — escludere:
- Comunicati puramente amministrativi (nomine, procedure burocratiche)
- Notizie senza impatto finanziario quantificabile
- Duplicati semantici (tenere solo la versione più informativa)

LINGUA: output sempre bilingue IT/EN per ogni campo testuale.
LUNGHEZZA TARGET: briefing sufficiente per audio di 7-8 minuti
(circa 900-1000 parole IT). Non essere prolisso — ogni frase deve aggiungere valore.
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

    # Caricare market_data e includerlo nel prompt
    market_context = ""
    if MARKET_DATA_PATH.exists():
        with open(MARKET_DATA_PATH, 'r') as f:
            md = json.load(f)
        lines = []
        labels = {
            'eur_usd': 'EUR/USD', 'sp500': 'S&P 500', 'vix': 'VIX',
            'gold': 'GOLD', 'oil_brent': 'BRENT', 'stoxx_600': 'STOXX 600',
            'nikkei': 'NIKKEI', 'shanghai': 'SHANGHAI',
            'btp_10y': 'BTP 10Y', 'us_10y': 'US 10Y'
        }
        for key, label in labels.items():
            item = md.get(key, {})
            val = item.get('value', 'N/A')
            chg = item.get('change', 'N/A')
            if val != 'N/A':
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
    articles_json = json.dumps(articles, ensure_ascii=False, indent=1)
    user_prompt = f"{market_context}ARTICOLI DA ANALIZZARE:\n{articles_json}"

    if history:
        user_prompt += f"\n\nHISTORY (EVITA RIPETIZIONI):\n{json.dumps(history, ensure_ascii=False, indent=1)}"

    logger.info('🤖 Chiamata a Groq...')
    try:
        response = client.chat.completions.create(
            model='llama-3.3-70b-versatile',
            messages=[
                {'role': 'system', 'content': SYSTEM_PROMPT},
                {'role': 'user', 'content': user_prompt},
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
