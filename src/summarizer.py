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
      "title_it": "Titolo breve e d'impatto in italiano",
      "title_en": "Short catchy title in English",
      "summary_it": "Sintesi di 1-2 righe in italiano",
      "summary_en": "1-2 lines summary in English",
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

REGOLE MACRO - GROUND TRUTH:
    - Nelle istruzioni utente troverai una sezione [MACRO GROUND TRUTH]. Usa SOLO quelle date per riferirti a quando le banche centrali (BCE/FED) si sono riunite o si riuniranno.
    - Se vedi un dato con data "ieri" o "1 giorno fa" nei dati di mercato, NON assumere che ci sia stata una riunione ieri a meno che non sia confermato dal GROUND TRUTH.
    - TASSI BCE: Riporta sempre sia il tasso di rifinanziamento (Main Refi) che quello sui depositi se disponibili.
    
    Format: JSON con 'audio_script_it', 'audio_script_en', 'market_sentiment', 'key_takeaways'.
    """
    

AUDIO_FINANCE_PROMPT = """Sei un conduttore radiofonico finanziario senior italiano specializzato in analisi macroeconomica globale.
Scrivi lo script audio per la prima parte del podcast (MERCATI TRADIZIONALI E MACRO).
LUNGHEZZA: 500-600 parole complessive.

STRUTTURA (rispetta i tempi):
1. APERTURA E BENVENUTO (50 parole): 
   Inizia sempre con un unico saluto professionale e l'introduzione al briefing.
   Esempio: "Buongiorno e benvenuti all'aggiornamento finanziario di oggi, il vostro Morning Briefing quotidiano."
2. CONTESTO ASIATICO (100 parole): 
   Dopo il benvenuto, cita la chiusura dei mercati asiatici (Nikkei e Shanghai) 
   come anticipazione di quello che potrebbe succedere in Europa e USA.
   Esempio: "La seduta asiatica ci consegna un Nikkei in calo dell'uno virgola 
   due percento a cinquantatremila punti, segnale che..."
3. SENTIMENT + MERCATI OCCIDENTALI (250 parole): 
   Analisi dell'S&P 500, DXY, VIX e tassi. Cita i valori esatti.
3. GEOPOLITICA (150 parole): 
   Analisi degli eventi in corso e impatto sui prezzi.
4. MACRO E BANCHE CENTRALI (150 parole): 
   Focus su tassi d'interesse e dati economici fresh.
5. CRYPTO (80 parole): 
   Transizione rapida al comparto digitale se i dati sono disponibili.
6. CHIUSURA FORWARD-LOOKING (100 parole): 
   Outlook e cosa osservare alla ripresa degli scambi.

VIETATO ASSOLUTO:
- Elenchi puntati o numerati.
- Ripetizioni per allungare il testo.
- NON CITARE i prezzi di Bitcoin, delle Altcoin, i flussi degli ETF o l'indice Fear & Greed in questa sezione. Lascia tutti i dettagli e i numeri per il segmento crypto.

PRONUNCIA IN AUDIO ITALIANO — REGOLE SPECIALI:
- ORO: Traduci sempre "/oz" con "l'oncia" (es. scrivi "4492 dollari l'oncia", NON "$4492/oz" e NON "slash oz").
- BITCOIN: MAI usare l'articolo determinativo davanti a Bitcoin (scrivi "Bitcoin ha perso...", NON "Il Bitcoin ha perso...").
- USA → scrivere "Usa"
- NATO → scrivere "Nato"
- OPEC → scrivere "Opek"  
- IMF → scrivere "Fondo Monetario Internazionale"
- Nomi propri inglesi (BlackRock, Goldman Sachs) → lasciare invariati.
- Price Alert → scrivere "Prais Alért"
- EVITA la parola "milioni" o "miliardi" se stai parlando di soglie di prezzo (es. "soglia dei 2100 dollari", NON "2100 milioni").

APERTURA CON ASIA E WEEKEND:
- Nei giorni feriali (Lun-Ven), iniziare citando Nikkei e Shanghai, spiegando che anticipano l'apertura occidentale.
- SE I MERCATI SONO CHIUSI (weekend/festività): NON fare MAI speculazioni o ipotesi sull'apertura (vietato dire "qualora fossero aperte" o "potrebbe influenzare le aperture"). Limitati a dire che le borse tradizionali osservano la pausa festiva e analizza esclusivamente i dati dell'ultima seduta passata.
"""

AUDIO_CRYPTO_PROMPT = """Sei un analista esperto di digital assets.
Scrivi lo script audio per la sezione CRIPTOVALUTE del podcast.
LUNGHEZZA: 300-400 parole.

TRANSITION OBBLIGATORIA (in apertura): 
- "Passiamo ora al comparto degli asset digitali..." 
- "Spostiamo l'attenzione sul mercato delle criptovalute..."

STRUTTURA E REGOLE:
1. DEEP DIVE BITCOIN (100 parole): Analisi tecnica e flussi. Cita TASSATIVAMENTE il valore esatto degli ETF (BTC ETF Daily Net Inflow). NON inventare numeri. Se il dato è negativo, indicalo come deflusso.
2. ALTCOINS (150 parole): Commenta Ethereum, Solana e Binance Coin (BNB).
3. SENTIMENT & FEAR/GREED (100 parole): Analisi dell'indice e correlazione macro. Usare la traduzione italiana per i livelli (es. "Estrema Paura").

REGOLE GRAMMATICALI:
- MAI usare l'articolo determinativo davanti a Bitcoin (scrivi "Bitcoin è sceso", NON "il Bitcoin è sceso").
- Sii tecnico. Evita di ripetere dati macro della prima parte se non strettamente necessari.
"""

AUDIO_FINANCE_PROMPT_EN = """You are a senior financial radio presenter.
Write the audio script for the first part of the podcast (TRADITIONAL MARKETS & MACRO).
LENGTH: 400-500 words.

MANDATORY OPENING:
Choose EXACTLY ONE of these openings (Do NOT use both):
- "Welcome to your daily morning market briefing."
- "Good morning and welcome to today's financial update."

STRUCTURE:
1. OPENING + SENTIMENT (100 words): Focus on global mood. Mention max 2 key data points.
2. TRADITIONAL MARKETS (200 words): Equities, Bonds, Currencies, Commodities. Use exact numerical values from the latest close.
3. GEOPOLITICS & MACRO (150-200 words): Ongoing events and impact on rates/inflation.

PRONUNCIATION & FORMATTING RULES (CRITICAL FOR TTS):
- NO ACRONYMS for comparisons: Write "Year over Year" instead of "YoY", and "Quarter over Quarter" instead of "QoQ".
- NO ABBREVIATED BIG NUMBERS: Write "trillion dollars" instead of "$...T" (e.g., "22.7 trillion dollars" instead of "$22.7T"). Write "billion dollars" instead of "$...B", and "million dollars" instead of "$...M".
- NO SLASHES: Write "Euro to US Dollar" instead of "EUR/USD", and "dollars per ounce" instead of "$/oz" or "$/barrel".
- NO SYMBOL SOUP: Write "D X Y" or "Dollar Index" instead of "(DXY)".

PROHIBITED:
- Do NOT mention Cryptocurrencies, Bitcoin, ETF flows, or Fear & Greed in this section. Keep all crypto data for the second part of the podcast.
- Do NOT speculate on market openings if it is the weekend. Just state the markets are closed and analyze the previous close.
- No bullet points. No fluff.
- Do NOT close the podcast yet.
"""

AUDIO_CRYPTO_PROMPT_EN = """You are a digital assets expert analyst.
Write the audio script for the CRYPTO section of the podcast.
LENGTH: 300-400 words.

MANDATORY TRANSITION:
- "Moving on to the digital assets space..."
- "Let's pivot to the cryptocurrency markets..."

STRUCTURE:
1. BTC DEEP DIVE (100 words): Technical analysis and flows.
2. ALTCOINS (150 words): Discuss Ethereum, Solana, and Binance Coin.
3. SENTIMENT & FEAR/GREED (100 words): Market correlation and index analysis.
"""

AUDIO_CLOSE_PROMPT = """CHIUSURA OBBLIGATORIA:
- Focus: outlook per domani e cosa monitorare.
- NON terminare MAI con la frase "buon trading" o "buon trading a tutti".
- Usa chiusure professionali come "Grazie per l'attenzione e a domani", "Un saluto da Prais Alért", "Al prossimo aggiornamento".
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
                            freshness = f"rilasciato il {date} ({days_ago} giorni fa — DATO NON RECENTE)"
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
                        freshness  = f"rilasciato il {date} ({days_ago}gg fa — DATO NON RECENTE)" \
                                     if days_ago > 14 else f"rilasciato {days_ago}gg fa ⚡ RECENTE"
                    except Exception:
                        freshness = f"rilasciato {date}"
                    lines.append(f"  {label}: {val} (prec. {prev}) — {freshness}")
                elif item.get('status') == 'upcoming':
                    lines.append(f"  {label}: NON RILASCIATO — prossima uscita {item.get('next_release')}")

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
    
    # Iniezione Ground Truth Macro
    macro_truth_str = f"""
[MACRO GROUND TRUTH - DATA REALE AL {datetime.now().strftime('%d %B %Y')}]
- ECB (BCE): Ultima riunione {MACRO_GROUND_TRUTH['ECB']['last_meeting']}, Prossima riunione {MACRO_GROUND_TRUTH['ECB']['next_meeting']}. {MACRO_GROUND_TRUTH['ECB']['result']}.
- FED (USA): Ultima riunione {MACRO_GROUND_TRUTH['FED']['last_meeting']}, Prossima riunione {MACRO_GROUND_TRUTH['FED']['next_meeting']}. {MACRO_GROUND_TRUTH['FED']['result']}.
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
            """Estrae il testo pulito dallo script, gestendo se l'LLM ha restituito un dict invece di una stringa."""
            content = script_obj.get(key, "")
            if isinstance(content, dict):
                # Se è un dict, unisci i valori delle chiavi in ordine
                return "\n\n".join(str(v) for v in content.values() if v)
            return str(content)

        # 1. ITALIANO
        logger.info('🎙️ Generazione Audio IT (3 segmenti)...')
        
        # Part A: Finance
        it_finance_user = f"DATA: {today_str}\nSENTIMENT: {briefing['sentiment']['label']}\nMERCATI:\n{market_context}\nNOTIZIE PRINCIPALI:\n" + \
                         "\n".join(f"- {a['title']}" for a in news_it[:10])
        it_finance_user += holiday_warning_it
        it_finance = get_audio_part(AUDIO_FINANCE_PROMPT, it_finance_user, 'audio_script_it')
        
        # Part B: Crypto
        # Assicurati che lo split includa anche i dati ETF che sono prima di CRYPTO data ma rilevanti
        etf_flow_ctx = f"BTC ETF Daily Net Inflow: {md.get('btc_etf_flow', {}).get('value', 'N/A')}\n"
        crypto_ctx = (market_context.split('CRYPTO MARKET DATA:')[1] if 'CRYPTO MARKET DATA:' in market_context else market_context)
        it_crypto_user = f"DATI CRYPTO:\n{etf_flow_ctx}{crypto_ctx}\nNOTIZIE CRYPTO:\n" + \
                        "\n".join(f"- {a['title']}" for a in news_it if a.get('category') == 'crypto')
        it_crypto = get_audio_part(AUDIO_CRYPTO_PROMPT, it_crypto_user, 'audio_script_it')
        
        # Part C: Close
        it_close = get_audio_part(AUDIO_CLOSE_PROMPT, "Genera chiusura per podcast finanziario italiano.", 'audio_script_it')
        
        # Merge IT
        briefing['audio_script_it'] = f"{clean_script(it_finance, 'audio_script_it')}\n\n{clean_script(it_crypto, 'audio_script_it')}\n\n{clean_script(it_close, 'audio_script_it')}"

        # 2. ENGLISH
        logger.info('🎙️ Generazione Audio EN (3 segmenti)...')
        
        # Part A: Finance
        en_finance_user = f"DATE: {today_str}\nSENTIMENT: {briefing['sentiment']['label']}\nMARKETS:\n{market_context}\nTOP NEWS:\n" + \
                         "\n".join(f"- {a['title']}" for a in news_it[:10])
        en_finance_user += holiday_warning_en
        en_finance = get_audio_part(AUDIO_FINANCE_PROMPT_EN, en_finance_user, 'audio_script_en')
        
        # Part B: Crypto
        en_crypto_user = it_crypto_user # Contesto è lo stesso
        en_crypto = get_audio_part(AUDIO_CRYPTO_PROMPT_EN, en_crypto_user, 'audio_script_en')
        
        # Part C: Close
        en_close = get_audio_part(AUDIO_CLOSE_PROMPT, "Generate closing for English financial podcast.", 'audio_script_en')
        
        # Merge EN
        briefing['audio_script_en'] = f"{clean_script(en_finance, 'audio_script_en')}\n\n{clean_script(en_crypto, 'audio_script_en')}\n\n{clean_script(en_close, 'audio_script_en')}"

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
