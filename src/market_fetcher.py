#!/usr/bin/env python3
"""
market_fetcher.py — Recupera dati di mercato reali via yfinance.
Salva in data/market_data.json.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
import yfinance as yf

# Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
OUTPUT_PATH = ROOT / 'data' / 'market_data.json'

# Mappa Simboli
SYMBOLS = {
    "eur_usd": "EURUSD=X",
    "vix": "^VIX",
    "us_10y": "^TNX",
    "sp500": "^GSPC",
    "stoxx_600": "^STOXX",
    "nikkei": "^N225",
    "shanghai": "000001.SS",
    "gold": "GC=F",
    "oil_brent": "BZ=F",
    "btp_10y": "BTP=F" # Usiamo il Future sul BTP come proxy affidabile su Yahoo
}

def get_formatted_value(ticker, symbol):
    try:
        data = ticker.history(period="1d")
        if data.empty:
            return "N/A", "0.0%"
        
        last_price = data['Close'].iloc[-1]
        prev_close = ticker.info.get('previousClose', last_price)
        
        change_pct = ((last_price - prev_close) / prev_close) * 100
        
        # Formattazione specifica
        if symbol == "EURUSD=X":
            return f"{last_price:.4f}", f"{change_pct:+.2f}%"
        elif symbol in ["^VIX", "GC=F", "BZ=F", "^GSPC", "^STOXX", "^N225", "000001.SS"]:
            return f"{last_price:,.2f}", f"{change_pct:+.2f}%"
        else:
            return f"{last_price:.2f}%" if "10y" in symbol or "btp" in symbol else f"{last_price:.2f}", f"{change_pct:+.2f}%"
            
    except Exception as e:
        logger.error(f"Errore nel fetch di {symbol}: {e}")
        return "N/A", "0.0%"

def run():
    logger.info("📈 Recupero dati di mercato...")
    results = {}
    
    for key, symbol in SYMBOLS.items():
        logger.info(f"Fetching {key} ({symbol})...")
        ticker = yf.Ticker(symbol)
        val, change = get_formatted_value(ticker, symbol)
        results[key] = {
            "value": val,
            "change": change
        }
    
    # Salva output
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    logger.info(f"✅ Dati di mercato salvati in {OUTPUT_PATH}")
    return results

if __name__ == "__main__":
    run()
