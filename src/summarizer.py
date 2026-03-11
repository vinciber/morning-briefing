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

import google.generativeai as genai

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
INPUT_PATH = ROOT / 'data' / 'fetched_articles.json'
OUTPUT_PATH = ROOT / 'data' / 'briefing_today.json'

GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY', '')

SYSTEM_PROMPT = '''
Sei un analista finanziario e geopolitico senior. Ricevi una lista di
articoli in JSON. Produci un briefing strutturato in formato JSON.

REGOLE ASSOLUTE:
1. Mantieni SEMPRE il campo 'source_url' per ogni notizia citata — è obbligatorio
2. Ogni summary deve essere fattico, non speculativo — cita solo fatti verificabili
3. Produci testo in ENTRAMBE le lingue: 'summary_it' e 'summary_en', 'title_it' e 'title_en'
4. Aggiungi sentiment: risk_on / risk_off / neutral con motivazione in entrambe le lingue
5. Rispondi SOLO con JSON valido, nessun testo extra (no backticks, no markdown)
6. In 'market_data' riporta gli ultimi valori noti per: EUR/USD, VIX, BTP 10Y Yield, Gold, Brent, S&P 500 Futures
7. Ordina le notizie per importanza (5 = massima, 1 = minima)
8. Il campo 'importance' è un intero da 1 a 5
9. Le sezioni devono essere: mercati, geopolitica, macro_economia, energia
10. Per ogni item includi: title_it, title_en, summary_it, summary_en, source_name, source_url, importance
11. I summary devono essere di 2-3 frasi, concisi ma informativi
12. Se fonti contrastanti dicono cose diverse, segnalalo nel summary
13. MASSIMO 15 ARTICOLI TOTALI: Seleziona SOLO le notizie più importanti in assoluto. Scarta tutto il resto per assicurarti che il JSON generato sia completo e non venga troncato.

FORMATO OUTPUT ATTESO:
{
  "date": "YYYY-MM-DD",
  "sentiment": {
    "label": "risk_off",
    "reason_it": "motivazione in italiano",
    "reason_en": "reasoning in english"
  },
  "market_data": {
    "eur_usd": "1.0850",
    "vix": "18.5",
    "btp_10y": "3.65%",
    "gold": "2,150",
    "oil_brent": "82.30",
    "sp500_futures": "5,230"
  },
  "sections": [
    {
      "name": "mercati",
      "items": [
        {
          "title_it": "...",
          "title_en": "...",
          "summary_it": "...",
          "summary_en": "...",
          "source_name": "...",
          "source_url": "https://...",
          "importance": 5
        }
      ]
    }
  ]
}
'''


def run():
    """Pipeline principale: carica articoli → Gemini → salva briefing JSON."""
    if not GEMINI_API_KEY:
        logger.error('❌ GEMINI_API_KEY non configurata! Imposta la variabile d\'ambiente.')
        return None

    # Carica articoli pre-filtrati
    if not INPUT_PATH.exists():
        logger.error(f'❌ File non trovato: {INPUT_PATH}. Esegui prima fetcher.py')
        return None

    with open(INPUT_PATH, 'r', encoding='utf-8') as f:
        articles = json.load(f)

    if not articles:
        logger.warning('⚠️ Nessun articolo da processare')
        return None

    logger.info(f'📰 Caricati {len(articles)} articoli per il briefing')

    # Configura Gemini
    genai.configure(api_key=GEMINI_API_KEY)

    model = genai.GenerativeModel(
        model_name='gemini-2.5-flash',
        generation_config=genai.GenerationConfig(
            temperature=0.3,
            max_output_tokens=8000,
            response_mime_type='application/json',
        ),
    )

    # Prepara il prompt con gli articoli
    articles_json = json.dumps(articles, ensure_ascii=False, indent=1)
    user_prompt = f'''Ecco {len(articles)} articoli aggregati da fonti finanziarie e geopolitiche.
Data di oggi: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}

Produci il briefing strutturato seguendo ESATTAMENTE il formato JSON specificato.

ARTICOLI:
{articles_json}'''

    # Chiamata Gemini
    logger.info('🤖 Chiamata a Gemini 1.5 Flash...')
    try:
        response = model.generate_content(
            [SYSTEM_PROMPT, user_prompt],
        )

        raw_text = response.text.strip()

        # Pulizia: rimuovi eventiali backtick markdown
        if raw_text.startswith('```'):
            raw_text = raw_text.split('\n', 1)[1]
        if raw_text.endswith('```'):
            raw_text = raw_text.rsplit('```', 1)[0]

        briefing = json.loads(raw_text)

        # Validazione base
        if 'sections' not in briefing:
            logger.error('❌ Output Gemini mancante di "sections"')
            return None
        if 'sentiment' not in briefing:
            logger.warning('⚠️ Sentiment mancante, aggiungo default')
            briefing['sentiment'] = {
                'label': 'neutral',
                'reason_it': 'Dati insufficienti per determinare sentiment',
                'reason_en': 'Insufficient data to determine sentiment',
            }

        # Assicura che la data sia presente
        if 'date' not in briefing:
            briefing['date'] = datetime.now(timezone.utc).strftime('%Y-%m-%d')

        # Conta notizie
        total_items = sum(len(s.get('items', [])) for s in briefing.get('sections', []))
        logger.info(f'✅ Briefing generato: {total_items} notizie in {len(briefing["sections"])} sezioni')
        logger.info(f'   Sentiment: {briefing["sentiment"]["label"]}')

        # Salva output
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
            json.dump(briefing, f, ensure_ascii=False, indent=2)

        logger.info(f'💾 Salvato: {OUTPUT_PATH}')
        return briefing

    except json.JSONDecodeError as e:
        logger.error(f'❌ Gemini ha restituito JSON non valido: {e}')
        logger.error(f'   Raw response: {raw_text[:500]}...')
        return None
    except Exception as e:
        logger.error(f'❌ Errore Gemini: {e}')
        return None


if __name__ == '__main__':
    run()
