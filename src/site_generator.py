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
from collections import defaultdict

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
# Helpers
# ---------------------------------------------------------------------------
def build_market_strip(market_data):
    order = [
        ('eur_usd',   'EUR/USD'),
        ('dxy',       'DXY'),
        ('sp500',     'S&P 500'),
        ('stoxx_600', 'STOXX 600'),
        ('nikkei',    'NIKKEI'),
        ('shanghai',  'SHANGHAI'),
        ('vix',       'VIX'),
        ('tlt',       'TLT'),
        ('gold',      'GOLD'),
        ('btcusd',    'BTC'),
        ('oil_brent', 'BRENT'),
        ('btp_10y',   'BTP 10Y'),
        ('us_10y',    'US 10Y'),
        ('global_m2', 'M2 GLOBAL'),
    ]
    strip = []
    for key, label in order:
        item = market_data.get(key, {})
        if isinstance(item, str):
            val = item
            chg = 'N/A'
        else:
            val = item.get('value', 'N/A')
            chg = item.get('change', 'N/A')
        if val and val != 'N/A':
            positive = isinstance(chg, str) and '+' in chg
            negative = isinstance(chg, str) and '-' in chg
            strip.append({
                'label':    label,
                'value':    val,
                'change':   chg if chg and chg != 'N/A' else '',
                'positive': positive,
                'negative': negative,
            })
    return strip


# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------
def group_articles_into_sections(articles, lang):
    """Gruppa gli articoli per categoria e ordina le sezioni."""
    cat_map_en = {
        'mercati': 'markets',
        'geopolitica': 'geopolitics',
        'macro_economia': 'macro',
        'energia': 'energy',
        'tecnologia': 'technology',
        'cripto': 'crypto'
    }
    
    articles_by_cat = defaultdict(list)
    for art in articles:
        # Create a copy for template display to avoid modifying the original article
        # The display logic for title/summary/category is now handled directly in the Jinja2 templates
        # by checking for _en or _it suffixes, or falling back to generic fields.
        cat = art.get('category', 'mercati')
        articles_by_cat[cat].append(art)

    sections = []
    cat_order = ['mercati', 'geopolitica', 'macro_economia', 'energia']
    for cat in cat_order:
        if cat in articles_by_cat:
            sections.append({
                'name': cat,
                'display_name': cat_map_en.get(cat, cat) if lang == 'en' else cat,
                'items': articles_by_cat[cat]
            })
    for cat, items in articles_by_cat.items():
        if cat not in cat_order:
            sections.append({
                'name': cat,
                'display_name': cat_map_en.get(cat, cat) if lang == 'en' else cat,
                'items': items
            })
    return sections


def generate_daily_page(briefing: dict, env: Environment, base_url: str, lang: str = 'it'):
    """Genera la pagina del briefing del giorno: docs/[en/]YYYY-MM-DD.html"""
    date = briefing.get('date', datetime.now(timezone.utc).strftime('%Y-%m-%d'))
    template = env.get_template('site_daily.html')

    sentiment = briefing.get('sentiment', {})
    sentiment_label = sentiment.get('label', 'neutral')
    
    # Text overrides for language
    lang_info = {
        'title': 'The Morning Brief' if lang == 'en' else 'Morning Briefing',
        'sentiment_text': sentiment.get(f'reason_{lang}', '')
    }
    
    all_articles = briefing.get('articles', [])
    sections = group_articles_into_sections(all_articles, lang)

    template_vars = {
        'briefing': briefing,
        'date': date,
        'lang': lang,
        'lang_info': lang_info,
        'sentiment': sentiment,
        'market_strip': build_market_strip(briefing.get('market_data_raw', briefing.get('market_data', {}))),
        'sections': sections,
        'all_articles': all_articles,
        'audio_url': f'audio/briefing_{date.replace("-", "")}.mp3' if lang == 'it' else f'../audio/briefing_{date.replace("-", "")}_en.mp3',
        'rss_url': 'feed.xml' if lang == 'it' else '../feed_en.xml',
        'index_url': 'index.html' if lang == 'it' else 'index.html',
        'it_url': f'{date}.html' if lang == 'it' else f'../{date}.html',
        'en_url': f'en/{date}.html' if lang == 'it' else f'{date}.html',
        'favicon_url': 'https://vinciber.github.io/morning-briefing/favicon.ico'
    }

    html = template.render(**template_vars)

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

    # Prep articles for index
    all_articles = briefing.get('articles', [])
    cat_map_en = {
        'mercati': 'markets',
        'geopolitica': 'geopolitics',
        'macro_economia': 'macro',
        'energia': 'energy',
        'tecnologia': 'technology',
        'cripto': 'crypto'
    }
    # No modifications to articles here, Jinja2 template handles display labels
    # by checking for _en or _it suffixes, or falling back to generic fields.

    # Archive links
    archive_dir = DOCS_DIR if lang == 'it' else DOCS_DIR / 'en'
    archive_dates = sorted(
        [f.stem for f in archive_dir.glob('20*.html')],
        reverse=True
    )[:30]

    lang_info = {
        'title': 'The Morning Brief' if lang == 'en' else 'Morning Briefing',
        'sentiment_text': sentiment.get(f'reason_{lang}', '')
    }

    template_vars = {
        'briefing': briefing,
        'date': date,
        'lang': lang,
        'lang_info': lang_info,
        'sentiment': sentiment,
        'all_articles': all_articles,
        'market_strip': build_market_strip(briefing.get('market_data_raw', briefing.get('market_data', {}))),
        'audio_url': f'audio/briefing_{date.replace("-", "")}.mp3' if lang == 'it' else f'../audio/briefing_{date.replace("-", "")}_en.mp3',
        'rss_url': 'feed.xml' if lang == 'it' else '../feed_en.xml',
        'index_url': 'index.html' if lang == 'it' else 'index.html',
        'it_url': f'{date}.html' if lang == 'it' else f'../{date}.html',
        'en_url': f'en/{date}.html' if lang == 'it' else f'{date}.html',
        'archive_dates': archive_dates,
        'favicon_url': 'https://vinciber.github.io/morning-briefing/favicon.ico',
        'base_url': base_url
    }

    html = template.render(**template_vars)

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
    for art in briefing.get('articles', []):
        # Use localized fields with generic fallbacks to prevent empty cards
        if lang == 'it':
            title = art.get('title_it') or art.get('title_en') or art.get('title', '')
            summary = art.get('summary_it') or art.get('summary_en') or art.get('snippet', '')
        else:
            title = art.get('title_en') or art.get('title_it') or art.get('title', '')
            summary = art.get('summary_en') or art.get('summary_it') or art.get('snippet', '')
        
        # Link della news originale
        source_url = art.get('source_url', f'{base_url}/{date}.html')

        items_xml.append(f'''    <item>
      <title>{_xml_escape(title)}</title>
      <link>{_xml_escape(source_url)}</link>
      <description>{_xml_escape(summary)}</description>
      <pubDate>{now_rfc822}</pubDate>
      <guid>{_xml_escape(source_url)}</guid>
      <category>{art.get("category", "mercati")}</category>
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

    # Schema Canonico: Rimuovere ridondanze
    briefing_clone = json.loads(json.dumps(briefing)) # Deep copy
    
    # 1. Rimuovere market_data (usare solo market_data_raw)
    if 'market_data' in briefing_clone:
        del briefing_clone['market_data']
    
    # 2. Rimuovere sections
    if 'sections' in briefing_clone:
        del briefing_clone['sections']

    # 3. Pulizia articoli
    for art in briefing_clone.get('articles', []):
        # Rimuovere campi non previsti
        for field in ['display_title', 'display_summary', 'display_category', 'importance']:
            if field in art:
                del art[field]
        
        # Assicurarsi che url esista
        if 'source_url' in art and 'url' not in art:
            art['url'] = art['source_url']

    output_path = api_dir / 'today.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(briefing_clone, f, ensure_ascii=False, indent=2)
    logger.info(f'✅ API JSON generato (Schema Canonico): {output_path}')

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
        'items_count': len(briefing.get('articles', [])),
    })
 
    index = index[:60] # Esteso a 60 giorni

    with open(index_path, 'w', encoding='utf-8') as f:
        json.dump(index, f, ensure_ascii=False, indent=2)


def generate_archive(briefing: dict):
    """Archivia in docs/archive/YYYY-MM-DD.json con schema canonico."""
    archive_dir = DOCS_DIR / 'archive'
    archive_dir.mkdir(parents=True, exist_ok=True)
    date = briefing.get('date', datetime.now(timezone.utc).strftime('%Y-%m-%d'))
    
    # Use same cleanup logic as for today.json
    briefing_clone = json.loads(json.dumps(briefing))
    if 'market_data' in briefing_clone: del briefing_clone['market_data']
    if 'sections' in briefing_clone: del briefing_clone['sections']
    for art in briefing_clone.get('articles', []):
        for field in ['display_title', 'display_summary', 'display_category', 'importance']:
            if field in art: del art[field]
        if 'source_url' in art and 'url' not in art: art['url'] = art['source_url']

    output_path = archive_dir / f'{date}.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(briefing_clone, f, ensure_ascii=False, indent=2)


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
