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
def generate_daily_page(briefing: dict, env: Environment, base_url: str, lang: str = 'it'):
    """Genera la pagina del briefing del giorno: docs/[en/]YYYY-MM-DD.html"""
    date = briefing.get('date', datetime.now(timezone.utc).strftime('%Y-%m-%d'))
    template = env.get_template('site_daily.html')

    sentiment = briefing.get('sentiment', {})
    sentiment_label = sentiment.get('label', 'neutral')
    sentiment_color = {
        'risk_on': '#22c55e',
        'risk_off': '#ef4444',
        'neutral': '#eab308',
    }.get(sentiment_label, '#eab308')
    
    # Text overrides for language
    lang_info = {
        'it': {'title': 'Briefing del Giorno', 'sentiment_text': sentiment.get('reason_it', '')},
        'en': {'title': 'Daily Briefing', 'sentiment_text': sentiment.get('reason_en', '')}
    }.get(lang, lang_info['it'])

    html = template.render(
        briefing=briefing,
        date=date,
        lang=lang,
        lang_info=lang_info,
        sentiment=sentiment,
        sentiment_color=sentiment_color,
        base_url=base_url,
        favicon_url='favicon.png' if lang == 'it' else '../favicon.png'
    )

    out_dir = DOCS_DIR if lang == 'it' else DOCS_DIR / 'en'
    out_dir.mkdir(parents=True, exist_ok=True)
    
    output_path = out_dir / f'{date}.html'
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    logger.info(f'✅ Pagina generata ({lang}): {output_path}')


def generate_index(briefing: dict, env: Environment, base_url: str, lang: str = 'it'):
    """Genera la homepage: docs/[en/]index.html con le ultime notizie."""
    template = env.get_template('site_index.html')
    date = briefing.get('date', datetime.now(timezone.utc).strftime('%Y-%m-%d'))
    sentiment = briefing.get('sentiment', {})
    sentiment_color = {
        'risk_on': '#22c55e',
        'risk_off': '#ef4444',
        'neutral': '#eab308',
    }.get(sentiment.get('label', 'neutral'), '#eab308')

    # Prep items for this language
    all_items = []
    for section in briefing.get('sections', []):
        for item in section.get('items', []):
            display_item = item.copy()
            display_item['section'] = section.get('name', '')
            if lang == 'en':
                display_item['title'] = item.get('title_en', item.get('title_it', ''))
                display_item['summary'] = item.get('summary_en', item.get('summary_it', ''))
            else:
                display_item['title'] = item.get('title_it', item.get('title_en', ''))
                display_item['summary'] = item.get('summary_it', item.get('summary_en', ''))
            all_items.append(display_item)

    all_items.sort(key=lambda x: x.get('importance', 0), reverse=True)

    # Archive links
    archive_dir = DOCS_DIR if lang == 'it' else DOCS_DIR / 'en'
    archive_dates = sorted(
        [f.stem for f in archive_dir.glob('20*.html')],
        reverse=True
    )[:30]

    lang_info = {
        'it': {'title': 'Morning Briefing', 'sentiment_text': sentiment.get('reason_it', '')},
        'en': {'title': 'Morning Briefing', 'sentiment_text': sentiment.get('reason_en', '')}
    }.get(lang, lang_info['it'])

    html = template.render(
        briefing=briefing,
        date=date,
        lang=lang,
        lang_info=lang_info,
        sentiment=sentiment,
        sentiment_color=sentiment_color,
        market_data=briefing.get('market_data', {}),
        all_items=all_items,
        archive_dates=archive_dates,
        base_url=base_url,
        favicon_url='favicon.png' if lang == 'it' else '../favicon.png'
    )

    out_dir = DOCS_DIR if lang == 'it' else DOCS_DIR / 'en'
    out_dir.mkdir(parents=True, exist_ok=True)
    
    output_path = out_dir / 'index.html'
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    logger.info(f'✅ Homepage generata ({lang}): {output_path}')


def generate_rss(briefing: dict, base_url: str, max_items: int = 30, lang: str = 'it'):
    """Genera feed RSS: docs/feed.xml e docs/feed_en.xml"""
    date = briefing.get('date', datetime.now(timezone.utc).strftime('%Y-%m-%d'))
    now_rfc822 = format_datetime(datetime.now(timezone.utc))

    items_xml = []
    for section in briefing.get('sections', []):
        for item in section.get('items', []):
            if lang == 'it':
                title = item.get('title_it', item.get('title_en', ''))
                summary = item.get('summary_it', item.get('summary_en', ''))
            else:
                title = item.get('title_en', item.get('title_it', ''))
                summary = item.get('summary_en', item.get('summary_it', ''))
            
            # Use specific language link if available
            link_suffix = f"{date}.html" if lang == 'it' else f"en/{date}.html"
            source_url = item.get('source_url', f'{base_url}/{link_suffix}')

            items_xml.append(f'''    <item>
      <title>{_xml_escape(title)}</title>
      <link>{_xml_escape(source_url)}</link>
      <description>{_xml_escape(summary)}</description>
      <pubDate>{now_rfc822}</pubDate>
      <guid>{_xml_escape(source_url)}</guid>
      <category>{section.get("name", "")}</category>
    </item>''')

    title_suffix = " (EN)" if lang == 'en' else ""
    desc_lang = "en-US" if lang == 'en' else "it-IT"
    filename = "feed_en.xml" if lang == 'en' else "feed.xml"

    rss = f'''<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom">
  <channel>
    <title>Morning Briefing — Finance &amp; Geopolitics{title_suffix}</title>
    <link>{base_url}</link>
    <description>Daily AI-curated financial &amp; geopolitical briefing</description>
    <language>{desc_lang}</language>
    <lastBuildDate>{now_rfc822}</lastBuildDate>
    <atom:link href="{base_url}/{filename}" rel="self" type="application/rss+xml"/>
{chr(10).join(items_xml[:max_items])}
  </channel>
</rss>'''

    output_path = DOCS_DIR / filename
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(rss)
    logger.info(f'✅ RSS generato: {output_path}')


def generate_api_json(briefing: dict):
    """Genera docs/api/today.json — il file consumato da Price Alert."""
    api_dir = DOCS_DIR / 'api'
    api_dir.mkdir(parents=True, exist_ok=True)

    output_path = api_dir / 'today.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(briefing, f, ensure_ascii=False, indent=2)
    logger.info(f'✅ API JSON generato: {output_path}')

    date = briefing.get('date', datetime.now(timezone.utc).strftime('%Y-%m-%d'))
    index_path = api_dir / 'index.json'

    index = []
    if index_path.exists():
        try:
            with open(index_path, 'r', encoding='utf-8') as f:
                index = json.load(f)
        except Exception:
            index = []

    index = [b for b in index if b.get('date') != date]
    index.insert(0, {
        'date': date,
        'sentiment': briefing.get('sentiment', {}).get('label', 'neutral'),
        'sections_count': len(briefing.get('sections', [])),
        'items_count': sum(len(s.get('items', [])) for s in briefing.get('sections', [])),
    })
    index = index[:60] # Esteso a 60 giorni

    with open(index_path, 'w', encoding='utf-8') as f:
        json.dump(index, f, ensure_ascii=False, indent=2)


def generate_archive(briefing: dict):
    """Archivia in docs/archive/YYYY-MM-DD.json."""
    archive_dir = DOCS_DIR / 'archive'
    archive_dir.mkdir(parents=True, exist_ok=True)
    date = briefing.get('date', datetime.now(timezone.utc).strftime('%Y-%m-%d'))
    output_path = archive_dir / f'{date}.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(briefing, f, ensure_ascii=False, indent=2)


def _xml_escape(text: str) -> str:
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
    # Generazione IT
    generate_daily_page(briefing, env, base_url, lang='it')
    generate_index(briefing, env, base_url, lang='it')
    
    # Generazione EN
    generate_daily_page(briefing, env, base_url, lang='en')
    generate_index(briefing, env, base_url, lang='en')
    
    generate_rss(briefing, base_url, max_feed_items, lang='it')
    generate_rss(briefing, base_url, max_feed_items, lang='en')
    generate_api_json(briefing)
    generate_archive(briefing)

    logger.info('✅ Sito completamente aggiornato')
    return True


if __name__ == '__main__':
    run()
