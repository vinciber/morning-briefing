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
Produci un briefing mattutino JSON con quattro componenti:

1. SENTIMENT di mercato
2. MARKET IMPACT SUMMARY
3. AUDIO SCRIPT per podcast (7-8 minuti)
4. ARTICLE IMPACTS — giudizio per ogni articolo

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

STILE: calmo, didattico, preciso. Cita sempre valori numerici specifici.
Termini da usare quando pertinenti:
compressione, debasement, stagflazione, risk-on/risk-off,
soft landing disinflazionistico, repressione finanziaria, mean reverting.

OUTPUT JSON — struttura esatta:
{
  "date": "YYYY-MM-DD",
  "sentiment": {
    "label": "risk_on | risk_off | neutral",
    "score": 1-10,
    "reason_it": "3-4 righe narrative. Almeno 3 asset con valori numerici. Tono Bloomberg Intelligence. Mai generico. Collega i dati agli scenari macro.",
    "reason_en": "same in English"
  },
  "market_impact_summary": {
    "it": "4-5 righe. Almeno 3 asset class con variazioni numeriche. Usa il framework di lettura mercati.",
    "en": "same in English"
  },
  "audio_script_it": "Script completo per podcast 7-8 minuti in italiano. MINIMO 800 PAROLE OBBLIGATORIO — conta le parole, se sei sotto 800 espandi ogni sezione. Struttura: (1) Apertura sentiment + 3 dati chiave — 2 min. (2) Mercati asset per asset con numeri e implicazioni — 2 min. (3) Geopolitica e impatto prezzi — 1.5 min. (4) Macro/banche centrali/tassi — 1.5 min. (5) Chiusura: cosa monitorare domani — 1 min. Tono Bloomberg radio. Mai elenchi puntati — solo prosa narrativa fluida.",
  "audio_script_en": "Same structure in English. MINIMUM 800 WORDS MANDATORY.",
  "article_impacts": [
    {
      "url": "url esatto dell'articolo",
      "direction": "bearish | bullish | mixed",
      "magnitude": "high | medium | low",
      "assets_affected": ["S&P 500", "Brent"]
    }
  ]
}

REGOLE article_impacts:
- Includere TUTTI gli articoli ricevuti in input, uno per uno
- Usare l'URL esatto come chiave di matching
- direction segue TASSATIVAMENTE la tabella sopra
- magnitude: high = impatto immediato su >2 asset, medium = 1 asset, low = contesto/background
- assets_affected: lista degli asset direttamente impattati
- Se un articolo non ha un URL, NON includerlo in article_impacts.

REGOLE relevance_score — il modello NON modifica i relevance_score degli articoli.
Vengono passati dal fetcher e rimangono invariati.

LINGUA: reason_it/en e market_impact_summary sempre bilingue.
LUNGHEZZA audio: MINIMO 800 parole per lingua — TASSATIVO.
MAX TOKENS OUTPUT: 8000.
"""

AUDIO_SYSTEM_PROMPT = """Sei un conduttore radiofonico finanziario senior stile Bloomberg Radio.
Devi scrivere ESATTAMENTE uno script audio da 800-1000 parole in italiano.

REGOLE ASSOLUTE:
- MAI frasi come "speriamo che questo podcast sia stato utile" o "arrivederci"
- MAI elenchi puntati
- SEMPRE valori numerici specifici per ogni asset citato
- Tono: autorevole, didattico, mai banale

STRUTTURA OBBLIGATORIA (rispetta i tempi):
1. APERTURA (150 parole): sentiment del giorno + 3 dati chiave con numeri
2. MERCATI (250 parole): ogni asset con valore, variazione e implicazione macro
3. GEOPOLITICA (150 parole): eventi e impatto diretto sui prezzi
4. MACRO/BANCHE CENTRALI (150 parole): Fed, BCE, tassi, inflazione
5. CHIUSURA FORWARD-LOOKING (100 parole): cosa monitorare domani

CONTA LE PAROLE. Se sei sotto 800, espandi ogni sezione prima di rispondere."""


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
                'direction': impact.get('direction', 'mixed'),
                'magnitude': impact.get('magnitude', 'low'),
                'assets_affected': impact.get('assets_affected', []),
            }

    matched = 0
    for art in articles:
        url = art.get('url', '').strip()
        if url in impacts_by_url:
            art['market_impact'] = impacts_by_url[url]
            matched += 1
        else:
            # Fallback rule-based per null
            cat = art.get('category', '').lower()
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

    # Filtra articoli con score troppo basso (rumore)
    articles = [a for a in articles if a.get('relevance_score', 0) >= 0.3]
    logger.info(f'📰 Articoli dopo filtro quality >= 0.3: {len(articles)}')

    # Costruisci contesto mercati
    market_context = ""
    md = {}
    if MARKET_DATA_PATH.exists():
        with open(MARKET_DATA_PATH, 'r', encoding='utf-8') as f:
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

    articles_json = json.dumps(articles_slim, ensure_ascii=False)
    user_prompt = f"{market_context}ARTICOLI DA ANALIZZARE ({len(articles_slim)} totali):\n{articles_json}"

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
            max_tokens=4000,
            response_format={'type': 'json_object'},
        )
        raw_text = response.choices[0].message.content.strip()
        briefing = json.loads(raw_text)

        # CHIAMATA 2 — Audio Script IT
        logger.info('🎙️ Chiamata 2: Groq Llama 4 Audio Script IT (MIN 800 parole)...')
        today_str = datetime.now(timezone.utc).strftime('%d %B %Y')
        audio_user_it = f"""DATA DI OGGI: {today_str}
Scrivi lo script audio completo IN ITALIANO basandoti su questi dati:

SENTIMENT: {briefing.get('sentiment', {}).get('label', 'neutral').upper()} — score {briefing.get('sentiment', {}).get('score', 5)}/10
{briefing.get('sentiment', {}).get('reason_it', '')}

DATI MERCATO:
{market_context}

NOTIZIE DEL GIORNO:
{chr(10).join(f"- [{a['category'].upper()}] {a['title']} — {a['snippet'][:150]}" for a in articles_slim[:12])}

REQUISITO: minimo 800 parole in ITALIANO. Conta internamente prima di rispondere.
Restituisci JSON: {{"audio_script_it": "..."}}"""

        response_it = client.chat.completions.create(
            model='meta-llama/llama-4-scout-17b-16e-instruct',
            messages=[
                {'role': 'system', 'content': AUDIO_SYSTEM_PROMPT},
                {'role': 'user',   'content': audio_user_it},
            ],
            temperature=0.3,
            max_tokens=5000,
            response_format={'type': 'json_object'},
        )
        audio_it_data = json.loads(response_it.choices[0].message.content)
        briefing['audio_script_it'] = audio_it_data.get('audio_script_it', '')

        # CHIAMATA 3 — Audio Script EN
        logger.info('🎙️ Chiamata 3: Groq Llama 4 Audio Script EN (MIN 800 parole)...')
        audio_user_en = f"""TODAY'S DATE: {today_str}
Write the complete audio script IN ENGLISH based on these data:

SENTIMENT: {briefing.get('sentiment', {}).get('label', 'neutral').upper()} — score {briefing.get('sentiment', {}).get('score', 5)}/10
{briefing.get('sentiment', {}).get('reason_en', '')}

MARKET DATA:
{market_context}

NEWS OF THE DAY:
{chr(10).join(f"- [{a['category'].upper()}] {a['title']}" for a in articles_slim[:12])}

REQUIREMENT: minimum 800 words in ENGLISH. Count internally before answering.
Return JSON: {{"audio_script_en": "..."}}"""

        response_en = client.chat.completions.create(
            model='meta-llama/llama-4-scout-17b-16e-instruct',
            messages=[
                {'role': 'system', 'content': AUDIO_SYSTEM_PROMPT.replace("in italiano", "in English")},
                {'role': 'user',   'content': audio_user_en},
            ],
            temperature=0.3,
            max_tokens=5000,
            response_format={'type': 'json_object'},
        )
        audio_en_data = json.loads(response_en.choices[0].message.content)
        briefing['audio_script_en'] = audio_en_data.get('audio_script_en', '')

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
                    f'{"✅" if audio_words >= 800 else "⚠️ SOTTO 800"}')

        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
            json.dump(briefing, f, indent=2, ensure_ascii=False)

        return briefing

    except Exception as e:
        logger.error(f'❌ Errore durante summarizzazione: {e}')
        return None


if __name__ == '__main__':
    run()