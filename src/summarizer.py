#!/usr/bin/env python3
"""
summarizer.py — AI Processing con Groq Llama 4
Legge data/fetched_articles.json, invia batch a Groq,
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

REGOLA CRITICA — market_impact.direction:
"direction" indica l'impatto netto sul SENTIMENT DI MERCATO, NON la direzione del prezzo.

TABELLA DI RIFERIMENTO OBBLIGATORIA:
  VIX in aumento          → SEMPRE "bearish" (più volatilità = più paura)
  VIX in calo             → "bullish"
  Spread BTP/Bund in allargamento → "bearish"
  Petrolio in spike da geopolitica → "bearish" per equity (inflazione/recessione)
  Petrolio in calo        → "bullish" per equity (meno inflazione)
  DXY forte               → "bearish" per risk assets ed emerging markets
  DXY debole              → "bullish" per commodities e EM
  TLT in calo (tassi salgono) → "bearish" (costo del denaro sale)
  TLT in salita (tassi scendono) → "bullish"
  Gold in salita          → "mixed" (hedge, non risk-on puro)
  Fed hawkish / tassi più alti → "bearish"
  Dati occupazione forti  → "bullish"
  PIL sopra attese        → "bullish"
  Crisi geopolitica       → "bearish"
  De-escalation geopolitica → "bullish"
  Inflazione sopra attese → "bearish"
  Inflazione sotto attese → "bullish"

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
           Usa il framework di lettura mercati dove pertinente.",
    "en": "same in English"
  },
  "sections": [
    {"name": "mercati",        "items": []},
    {"name": "geopolitica",    "items": []},
    {"name": "macro_economia", "items": []},
    {"name": "energia",        "items": []}
  ]
}

REGOLE PER SEZIONE — RISPETTA I MINIMI TASSATIVAMENTE:
- mercati: MINIMO 6 item. Non notizie generiche ma CONSEGUENZE di mercato
  degli eventi geopolitici/macro/energetici. Con dati numerici obbligatori.
- geopolitica: MINIMO 3 item, focus su cosa muove i prezzi
- macro_economia: MINIMO 3 item, focus inflazione/tassi/banche centrali
- energia: MINIMO 3 item, sempre con prezzi specifici

STRUTTURA OGNI ITEM:
{
  "title_it": "Titolo con dato numerico obbligatorio.
               BUONO: 'Brent +4.2% a $91 dopo mine nello Stretto di Hormuz'
               CATTIVO: 'Aumento prezzi petrolio'",
  "title_en": "same in English",
  "summary_it": "3-4 frasi: (1) fatto+numero, (2) causa/contesto,
                 (3) impatto su asset specifici con valori, (4) scenario forward.
                 Zero frasi generiche.",
  "summary_en": "same in English",
  "source_name": "nome fonte originale",
  "source_url": "url originale dell'articolo",
  "category": "mercati|geopolitica|macro_economia|energia",
  "relevance_score": 1-5,
  "tier": 1|2|3|4,
  "market_impact": {
    "assets_affected": ["EUR/USD", "Brent", "VIX"],
    "direction": "bullish|bearish|mixed",
    "magnitude": "high|medium|low"
  }
}

REGOLE relevance_score — DIFFERENZIA OBBLIGATORIAMENTE:
  5 → evento di sistema (guerra, default sovrano, Fed pivot, crash)
  4 → impatto diretto su almeno 2 asset class con variazione >2%
  3 → rilevante per un singolo mercato/settore
  2 → notizia di contesto, impatto indiretto
  1 → background, nessun impatto immediato misurabile
  MAX 2 item con score 5 per briefing. MAX 3 item con score 4.

FILTRO QUALITÀ — escludere:
- Comunicati puramente amministrativi senza impatto mercati
- Notizie senza dato numerico ricavabile
- Duplicati semantici (tenere solo la versione più informativa)

LINGUA: output sempre bilingue IT/EN per ogni campo.
LUNGHEZZA TARGET: 900-1000 parole IT totali (audio 7-8 minuti).
MAX TOKENS OUTPUT: 8000.
"""


def run():
    """Pipeline principale: carica articoli + market + history → Groq → salva briefing JSON."""
    if not GROQ_API_KEY:
        logger.error('❌ GROQ_API_KEY non configurata!')
        return None

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

    # Carica history (briefing di ieri) per evitare ripetizioni
    history = {}
    if HISTORY_PATH.exists():
        try:
            with open(HISTORY_PATH, 'r', encoding='utf-8') as f:
                history = json.load(f)
        except Exception:
            pass

    logger.info(f'📰 Caricati {len(articles)} articoli, '
                f'{len(history.get("sections", []))} sezioni di history')

    client = Groq(api_key=GROQ_API_KEY)

    articles_json = json.dumps(articles, ensure_ascii=False)
    user_prompt = f"{market_context}ARTICOLI DA ANALIZZARE:\n{articles_json}"

    if history:
        # Passa solo i titoli della history per non sprecare token
        history_titles = []
        for section in history.get('sections', []):
            for item in section.get('items', []):
                t = item.get('title_it', '')
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

        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
            json.dump(briefing, f, ensure_ascii=False, indent=2)

        # Log stats
        total_items = sum(len(s.get('items', [])) for s in briefing.get('sections', []))
        logger.info(f'✅ Briefing salvato: {total_items} item totali')
        for s in briefing.get('sections', []):
            logger.info(f'   {s["name"]}: {len(s.get("items", []))} item')

        return briefing

    except Exception as e:
        logger.error(f'❌ Errore Groq: {e}')
        return None


if __name__ == '__main__':
    run()
