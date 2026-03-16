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

load_dotenv()

from groq import Groq

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

REGOLA LINGUAGGIO GEOPOLITICO:
- Usare il linguaggio dei fatti, non diplomatico. 
- Se gli articoli parlano di "war", "bombing", "conflict" → scrivere "guerra", "conflitto in corso", "bombardamenti"
- MAI attenuare con "potenziale", "possibile", "rischio di" se l'evento è già in corso
- Esempio SBAGLIATO: "Iran e Israele coinvolti in un potenziale conflitto"
- Esempio CORRETTO: "la guerra tra USA-Israele e Iran, al sedicesimo giorno, continua a pesare sui mercati"
- Il contesto temporale è importante: se gli articoli indicano che un evento è in corso da giorni/settimane, citarlo

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

AUDIO_SYSTEM_PROMPT = """Sei un conduttore radiofonico finanziario senior italiano.
Scrivi uno script audio da 800-1000 parole in italiano per un podcast mattutino.

APERTURA OBBLIGATORIA — usare una di queste varianti (mai Bloomberg):
- "Benvenuti al consueto briefing mattutino dei mercati."
- "Buongiorno, bentrovati all'appuntamento quotidiano con i mercati."
- "Bentornati al briefing finanziario mattutino."
- "Buongiorno a tutti, iniziamo il nostro aggiornamento quotidiano sui mercati."

STRUTTURA (rispetta i tempi):
1. APERTURA + SENTIMENT (150 parole): tono e 3 dati chiave narrativi
2. MERCATI ASSET PER ASSET (250 parole): ogni asset con valore e implicazione
3. GEOPOLITICA (150 parole): eventi e impatto diretto sui prezzi
4. MACRO E BANCHE CENTRALI (150 parole): Fed, BCE, tassi, inflazione
5. CHIUSURA FORWARD-LOOKING (100 parole): dati specifici da monitorare con date

VIETATO ASSOLUTO:
- Aprire con "Buongiorno e benvenuti a Bloomberg Radio" o qualsiasi riferimento a Bloomberg
- Elenchi numerati (1. 2. 3.) o con trattini
- Ripetere lo stesso concetto più di una volta — ogni frase deve aggiungere informazione nuova
- Frasi generiche come "sarà importante monitorare", "bisogna essere pronti a reagire",
  "il mercato è estremamente volatile" — se non supportate da dato specifico
- Finali tipo "That's all for today", "arrivederci", "Stay tuned", "We'll be back"
- Chiudere con un riassunto di quanto già detto — la chiusura deve guardare avanti
- MAI scrivere l'acronimo accanto al nome esteso: 
  NON "l'indice del dollaro (DXY)" o "l'indice del dollaro, noto come DXY"
  SÌ "l'indice del dollaro" — senza acronimo
  NON "la Federal Reserve (Fed)"
  SÌ "la Federal Reserve" — senza acronimo tra parentesi
  Regola generale: se usi il nome esteso, NON aggiungere mai l'acronimo

PRONUNCIA ASSET — scrivi sempre la forma estesa, mai l'acronimo:
- S&P 500 → "lo Standard and Poor's 500"
- VIX → "l'indice Vix"
- EUR/USD → "il cambio euro dollaro"
- DXY → "l'indice del dollaro"
- TLT → "l'ETF obbligazionario Treasury"
- BTC/Bitcoin → "Bitcoin"
- STOXX 600 → "l'indice Stoxx seicento"
- NIKKEI → "l'indice Nikkei"
- BCE → "la Banca Centrale Europea"
- Fed → "la Federal Reserve"
- BOJ → "la Banca del Giappone"

ACCURATEZZA STORICA:
- Non attenuare mai eventi già in corso con "potenziale" o "possibile"
- Se la guerra è in corso, dire "guerra in corso", non "potenziale conflitto"
- Se il Brent è sopra $100, non dire "rimane elevato" — dire il valore esatto

VARIAZIONI — mai il numero secco, sempre con contesto narrativo:
- NON: "S&P 500 a 6632"
- SÌ: "lo Standard and Poor's ha ceduto lo 0.61% portandosi a quota 6632 punti"
- NON: "Brent +2.67%"
- SÌ: "il greggio Brent ha guadagnato il 2.67% raggiungendo quota 103 dollari al barile"
- NON: "Bitcoin a 70646"
- SÌ: "Bitcoin si attesta intorno ai settantamila dollari"

CIFRE GRANDI — scrivi in forma leggibile:
- 70646 → "circa settantamila dollari"
- 6632 → "6632 punti"
- 53820 → "circa cinquantaquattromila punti"
- 22.4T → "22 virgola 4 trilioni di dollari"

DATI MACRO FRED NON RECENTI:
- Se il dato ha più di 14 giorni → "l'ultimo dato disponibile, risalente a [mese], mostrava..."
- MAI presentare dati di febbraio come notizie di oggi

LUNGHEZZA: MINIMO 800 PAROLE — conta internamente prima di rispondere.
"""

AUDIO_SYSTEM_PROMPT_EN = """You are a senior financial radio presenter.
Write an audio script of 800-1000 words in English for a morning podcast.

MANDATORY OPENING — use one of these variants (never Bloomberg):
- "Welcome to your daily morning market briefing."
- "Good morning and welcome to today's financial update."
- "Welcome back to your morning market briefing."
- "Good morning, and welcome to your daily market update."

STRUCTURE (respect timing):
1. OPENING + SENTIMENT (150 words): tone and 3 key narrative data points
2. MARKETS ASSET BY ASSET (250 words): each asset with value and implication
3. GEOPOLITICS (150 words): events and direct price impact
4. MACRO AND CENTRAL BANKS (150 words): Fed, ECB, rates, inflation
5. FORWARD-LOOKING CLOSE (100 words): specific data to watch with dates

ABSOLUTELY FORBIDDEN:
- Opening with "Benvenuti" or ANY Italian words
- Numbered lists (1. 2. 3.) or bullet points
- Repeating the same concept more than once
- Generic phrases like "it will be important to monitor" without specific data
- Endings like "That's all for today", "Stay tuned", "We'll be back"
- NEVER write the acronym next to the full name:
  NON "the dollar index (DXY)" or "the dollar index, known as DXY"
  YES "the dollar index" — without acronym
  NON "the Federal Reserve (Fed)"
  YES "the Federal Reserve" — without parenthetical acronym
  General rule: if you use the full name, NEVER add the acronym

ASSET PRONUNCIATION — always use full form:
- S&P 500 → "the Standard and Poor's 500"
- VIX → "the Vix index"
- EUR/USD → "the euro-dollar exchange rate"
- DXY → "the dollar index"
- TLT → "the Treasury bond ETF"
- STOXX 600 → "the Stoxx six hundred"
- BCE/ECB → "the European Central Bank"
- Fed → "the Federal Reserve"
- BOJ → "the Bank of Japan"

ACCURATEZZA STORICA:
- Non attenuare mai eventi già in corso con "potenziale" o "possibile"
- Se la guerra è in corso, dire "guerra in corso", non "potenziale conflitto"
- Se il Brent è sopra $100, non dire "rimane elevato" — dire il valore esatto

STALE MACRO DATA:
- If data is older than 14 days → "the last available data, from [month], showed..."
- NEVER present February data as today's news

LENGTH: MINIMUM 800 WORDS — count internally before responding.
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
            val = _format_value(item.get('value', 'N/A'))
            chg = _format_value(item.get('change', 'N/A'))
            if val and val != 'N/A':
                lines.append(f"  {label}: {val} ({chg})")

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
                            freshness = f"rilasciato il {date} ({days_ago} giorni fa — DATO NON RECENTE)"
                    except Exception:
                        freshness = f"rilasciato {date}"
                    
                    lines.append(f"  {label}: {val} (prec. {prev}) — {freshness}")
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
                {'role': 'system', 'content': AUDIO_SYSTEM_PROMPT_EN},
                {'role': 'user',   'content': audio_user_en},
            ],
            temperature=0.3,
            max_tokens=5000,
            response_format={'type': 'json_object'},
        )
        audio_en_data = json.loads(response_en.choices[0].message.content)
        briefing['audio_script_en'] = audio_en_data.get('audio_script_en', '')

        # RETRY AUDIO SE SOTTO 700 PAROLE
        audio_words = len(briefing.get('audio_script_it', '').split())
        if audio_words < 700:
            logger.warning(f'⚠️ Audio IT sotto soglia ({audio_words} parole), retry...')
            retry_prompt = audio_user_it + "\n\nATTENZIONE: lo script precedente era troppo corto. Devi scrivere ALMENO 800 parole. Espandi ogni sezione con più analisi e contesto."
            retry_response = client.chat.completions.create(
                model='meta-llama/llama-4-scout-17b-16e-instruct',
                messages=[
                    {'role': 'system', 'content': AUDIO_SYSTEM_PROMPT},
                    {'role': 'user', 'content': retry_prompt},
                ],
                temperature=0.3,
                max_tokens=6000,
                response_format={'type': 'json_object'},
            )
            retry_data = json.loads(retry_response.choices[0].message.content)
            briefing['audio_script_it'] = retry_data.get('audio_script_it', briefing['audio_script_it'])
            briefing['audio_script_en'] = retry_data.get('audio_script_en', briefing['audio_script_en'])
            audio_words = len(briefing.get('audio_script_it', '').split())
            logger.info(f'🎙️ Audio dopo retry: {audio_words} parole {"✅" if audio_words >= 700 else "⚠️ ancora sotto"}')

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