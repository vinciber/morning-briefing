#!/usr/bin/env python3
"""
archiver.py — Archiviazione dei briefing passati
Copia il briefing odierno in docs/archive/YYYY-MM-DD.json
E mantiene un indice dei briefing disponibili.
"""

import json
import logging
import re
import shutil
from pathlib import Path
from datetime import datetime, timezone, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
INPUT_PATH = ROOT / 'data' / 'briefing_today.json'
DOCS_DIR = ROOT / 'docs'
ARCHIVE_DIR = ROOT / 'docs' / 'archive'
API_DIR = ROOT / 'docs' / 'api'

RETENTION_DAYS = 30
DATE_RE = re.compile(r'^(\d{4})-(\d{2})-(\d{2})$')
NODASH_RE = re.compile(r'^(\d{4})(\d{2})(\d{2})$')


def _cleanup_old_files():
    """Elimina pagine HTML/JSON più vecchie di RETENTION_DAYS giorni."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)
    removed = 0

    def _file_date(stem):
        m = DATE_RE.match(stem)
        if m:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)), tzinfo=timezone.utc)
        m = NODASH_RE.match(stem)
        if m:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)), tzinfo=timezone.utc)
        return None

    for folder, pattern in [
        (DOCS_DIR, '20*.html'),
        (DOCS_DIR / 'en', '20*.html'),
        (ARCHIVE_DIR, '20*.json'),
        (DOCS_DIR / 'audio', 'briefing_*.mp3'),
    ]:
        if not folder.exists():
            continue
        for f in folder.glob(pattern):
            stem = f.stem.replace('briefing_', '').replace('_en', '')
            d = _file_date(stem)
            if d and d < cutoff:
                try:
                    f.unlink()
                    removed += 1
                except Exception as e:
                    logger.warning(f'⚠️ Impossibile rimuovere {f}: {e}')
    if removed:
        logger.info(f'🧹 Rimossi {removed} file più vecchi di {RETENTION_DAYS} giorni')

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
    
    # Cleanup file più vecchi di RETENTION_DAYS giorni (HTML, archive JSON, audio mp3)
    _cleanup_old_files()

    # Aggiorna index.json in api folder
    all_json_files = sorted(list(ARCHIVE_DIR.glob("*.json")), reverse=True)
    index_data = []

    for f in all_json_files[:RETENTION_DAYS]: # Retention 30 giornate
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
