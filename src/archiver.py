#!/usr/bin/env python3
"""
archiver.py — Archiviazione dei briefing passati
Copia il briefing odierno in docs/archive/YYYY-MM-DD.json
E mantiene un indice dei briefing disponibili.
"""

import json
import logging
import shutil
from pathlib import Path
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
INPUT_PATH = ROOT / 'data' / 'briefing_today.json'
ARCHIVE_DIR = ROOT / 'docs' / 'archive'
API_DIR = ROOT / 'docs' / 'api'

def run():
    """Copia briefing in archivio e aggiorna indici."""
    if not INPUT_PATH.exists():
        logger.error(f"❌ File non trovato: {INPUT_PATH}")
        return False
        
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    API_DIR.mkdir(parents=True, exist_ok=True)
    
    with open(INPUT_PATH, 'r', encoding='utf-8') as f:
        data = json.load(f)
        
    date_str = data.get('date', datetime.now(timezone.utc).strftime('%Y-%m-%d'))
    archive_file = ARCHIVE_DIR / f"{date_str}.json"
    
    # Salva copia statica
    with open(archive_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logger.info(f"✅ Archiviato: {archive_file}")
    
    # Aggiorna index.json in api folder
    all_json_files = sorted(list(ARCHIVE_DIR.glob("*.json")), reverse=True)
    index_data = []
    
    for f in all_json_files[:30]: # Ultime 30 giornate
        with open(f, 'r', encoding='utf-8') as j:
            try:
                brief = json.load(j)
                index_data.append({
                    "date": brief.get("date"),
                    "sentiment": brief.get("sentiment", {}).get("label"),
                    "url": f"https://vinciber.github.io/morning-briefing/archive/{f.name}"
                })
            except:
                continue
                
    with open(API_DIR / "index.json", "w", encoding='utf-8') as f:
        json.dump(index_data, f, indent=2, ensure_ascii=False)
    logger.info(f"✅ Indice API aggiornato: {API_DIR / 'index.json'}")
    
    return True

if __name__ == "__main__":
    run()
