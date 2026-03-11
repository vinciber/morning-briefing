#!/usr/bin/env python3
"""
site_generator.py — Generatore Sito Statico + RSS + API JSON
Genera: index.html, YYYY-MM-DD.html, feed.xml, api/today.json
Output in docs/ per GitHub Pages.
"""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from email.utils import format_datetime

import yaml
from jinja2 import Environment, FileSystemLoader

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
INPUT_PATH = ROOT / 'data' / 'briefing_today.json'
DOCS_DIR = ROOT / 'docs'
TEMPLATES_DIR = ROOT / 'templates'
CONFIG_PATH = ROOT / 'config.yml'


def load_config() -> dict:
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------
def generate_daily_page(briefing: dict, env: Environment, base_url: str):
    """Genera la pagina del briefing del giorno: docs/YYYY-MM-DD.html"""
    date = briefing.get('date', datetime.now(timezone.utc).strftime('%Y-%m-%d'))
    template = env.get_template('site_daily.html')

    sentiment = briefing.get('sentiment', {})
    sentiment_color = {
        'risk_on': '#22c55e',
        'risk_off': '#ef4444',
        'neutral': '#eab308',
    }.get(sentiment.get('label', 'neutral'), '#eab308')

    html = template.render(
        briefing=briefing,
        date=date,
        sentiment=sentiment,
        sentiment_color=sentiment_color,
        market_data=briefing.get('market_data', {}),
        base_url=base_url,
    )

    output_path = DOCS_DIR / f'{date}.html'
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    logger.info(f'✅ Pagina generata: {output_path}')


def generate_index(briefing: dict, env: Environment, base_url: str):
    """Genera la homepage: docs/index.html con le ultime notizie."""
    template = env.get_template('site_index.html')
    date = briefing.get('date', datetime.now(timezone.utc).strftime('%Y-%m-%d'))
    sentiment = briefing.get('sentiment', {})
    sentiment_color = {
        'risk_on': '#22c55e',
        'risk_off': '#ef4444',
        'neutral': '#eab308',
    }.get(sentiment.get('label', 'neutral'), '#eab308')

    # Raccolta di tutti gli item per il feed
    all_items = []
    for section in briefing.get('sections', []):
        for item in section.get('items', []):
            item['section'] = section.get('name', '')
            all_items.append(item)

    # Ordina per importanza decrescente
    all_items.sort(key=lambda x: x.get('importance', 0), reverse=True)

    # Carica archivio date precedenti
    archive_dates = sorted(
        [f.stem for f in DOCS_DIR.glob('20*.html')],
        reverse=True
    )[:30]

    html = template.render(
        briefing=briefing,
        date=date,
        sentiment=sentiment,
        sentiment_color=sentiment_color,
        market_data=briefing.get('market_data', {}),
        all_items=all_items,
        archive_dates=archive_dates,
        base_url=base_url,
    )

    output_path = DOCS_DIR / 'index.html'
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    logger.info(f'✅ Homepage generata: {output_path}')


def generate_rss(briefing: dict, base_url: str, max_items: int = 30):
    """Genera feed RSS: docs/feed.xml"""
    date = briefing.get('date', datetime.now(timezone.utc).strftime('%Y-%m-%d'))
    now_rfc822 = format_datetime(datetime.now(timezone.utc))

    items_xml = []
    for section in briefing.get('sections', []):
        for item in section.get('items', []):
            title = item.get('title_it', item.get('title_en', ''))
            summary = item.get('summary_it', item.get('summary_en', ''))
            source_url = item.get('source_url', f'{base_url}/{date}')

            items_xml.append(f'''    <item>
      <title>{_xml_escape(title)}</title>
      <link>{_xml_escape(source_url)}</link>
      <description>{_xml_escape(summary)}</description>
      <pubDate>{now_rfc822}</pubDate>
      <guid>{_xml_escape(source_url)}</guid>
      <category>{section.get("name", "")}</category>
    </item>''')

    rss = f'''<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom">
  <channel>
    <title>Morning Briefing — Finance &amp; Geopolitics</title>
    <link>{base_url}</link>
    <description>Daily AI-curated financial &amp; geopolitical briefing</description>
    <language>it-IT</language>
    <lastBuildDate>{now_rfc822}</lastBuildDate>
    <atom:link href="{base_url}/feed.xml" rel="self" type="application/rss+xml"/>
{chr(10).join(items_xml[:max_items])}
  </channel>
</rss>'''

    output_path = DOCS_DIR / 'feed.xml'
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(rss)
    logger.info(f'✅ RSS generato: {output_path}')


def generate_api_json(briefing: dict):
    """Genera docs/api/today.json — il file consumato da Price Alert."""
    api_dir = DOCS_DIR / 'api'
    api_dir.mkdir(parents=True, exist_ok=True)

    # today.json = copia completa del briefing
    output_path = api_dir / 'today.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(briefing, f, ensure_ascii=False, indent=2)
    logger.info(f'✅ API JSON generato: {output_path}')

    # index.json = lista briefing disponibili (ultimi 30 giorni)
    date = briefing.get('date', datetime.now(timezone.utc).strftime('%Y-%m-%d'))
    index_path = api_dir / 'index.json'

    index = []
    if index_path.exists():
        try:
            with open(index_path, 'r', encoding='utf-8') as f:
                index = json.load(f)
        except Exception:
            index = []

    # Aggiungi o aggiorna il briefing di oggi
    index = [b for b in index if b.get('date') != date]
    index.insert(0, {
        'date': date,
        'sentiment': briefing.get('sentiment', {}).get('label', 'neutral'),
        'sections_count': len(briefing.get('sections', [])),
        'items_count': sum(len(s.get('items', [])) for s in briefing.get('sections', [])),
    })
    index = index[:30]  # Keep last 30 days

    with open(index_path, 'w', encoding='utf-8') as f:
        json.dump(index, f, ensure_ascii=False, indent=2)
    logger.info(f'✅ API index generato: {index_path}')


def generate_archive(briefing: dict):
    """Archivia il briefing come JSON in docs/archive/YYYY-MM-DD.json."""
    archive_dir = DOCS_DIR / 'archive'
    archive_dir.mkdir(parents=True, exist_ok=True)

    date = briefing.get('date', datetime.now(timezone.utc).strftime('%Y-%m-%d'))
    output_path = archive_dir / f'{date}.json'

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(briefing, f, ensure_ascii=False, indent=2)
    logger.info(f'✅ Archivio JSON: {output_path}')


def _xml_escape(text: str) -> str:
    """Escape XML special characters."""
    return (text
            .replace('&', '&amp;')
            .replace('<', '&lt;')
            .replace('>', '&gt;')
            .replace('"', '&quot;')
            .replace("'", '&apos;'))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run():
    """Pipeline site: carica briefing → genera tutte le pagine + RSS + API."""
    if not INPUT_PATH.exists():
        logger.error(f'❌ File non trovato: {INPUT_PATH}')
        return False

    with open(INPUT_PATH, 'r', encoding='utf-8') as f:
        briefing = json.load(f)

    config = load_config()
    base_url = config.get('output', {}).get('site', {}).get('base_url', 'https://vinciber.github.io/morning-briefing')
    max_feed_items = config.get('output', {}).get('site', {}).get('max_items_feed', 30)

    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    env = Environment(loader=FileSystemLoader(str(TEMPLATES_DIR)))

    logger.info('🌐 Generazione sito...')
    generate_daily_page(briefing, env, base_url)
    generate_index(briefing, env, base_url)
    generate_rss(briefing, base_url, max_feed_items)
    generate_api_json(briefing)
    generate_archive(briefing)

    logger.info('✅ Sito completamente aggiornato')
    return True


if __name__ == '__main__':
    run()
