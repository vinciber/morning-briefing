#!/usr/bin/env python3
"""
fetcher.py — RSS + Web Fetch Aggregator
Scarica feed RSS (Tier 1/2/4), web_fetch (Tier 3),
pre-filtra per rilevanza, de-duplica, normalizza in JSON.
Output: data/fetched_articles.json (max ~40 articoli)
"""

import os
import json
import re
import hashlib
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

import feedparser
import requests
import yaml
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT / 'config.yml'
OUTPUT_PATH = ROOT / 'data' / 'fetched_articles.json'

# Keywords per relevance scoring (weight 0‑1)
HIGH_KEYWORDS = [
    'fed', 'ecb', 'bce', 'rate', 'rates', 'inflation', 'gdp', 'cpi',
    'war', 'sanctions', 'crisis', 'recession', 'default', 'tariff',
    'opec', 'oil', 'brent', 'gold', 'treasury', 'yield', 'spread',
    'central bank', 'monetary policy', 'fiscal', 'debt', 'bonds',
    'geopolitical', 'nato', 'china', 'russia', 'ukraine', 'iran',
    'middle east', 'elections', 'trade war', 'supply chain',
    'vix', 'sp500', 's&p', 'nasdaq', 'bitcoin', 'crypto',
    'btp', 'eur', 'usd', 'dollar', 'euro',
]

# Caps per category
CATEGORY_CAPS = {
    'banche_centrali': 8,
    'geopolitica': 10,
    'macro_economia': 8,
    'finanza': 15,
    'energia': 7,
}

USER_AGENT = 'MorningBriefingAgent/1.0 (+https://github.com/vinciber/morning-briefing)'


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def load_config() -> dict:
    """Carica config.yml"""
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def clean_html(raw: str) -> str:
    """Rimuove tag HTML e pulisce il testo."""
    if not raw:
        return ''
    text = BeautifulSoup(raw, 'html.parser').get_text(separator=' ')
    # Rimuovi spazi multipli
    text = re.sub(r'\s+', ' ', text).strip()
    return text[:800]  # Max snippet 800 chars


def relevance_score(title: str, snippet: str) -> float:
    """Calcola un punteggio di rilevanza 0-1 basato su keyword matching."""
    combined = f'{title} {snippet}'.lower()
    matches = sum(1 for kw in HIGH_KEYWORDS if kw in combined)
    # Normalizza: 5+ keyword matches = score 1.0
    return min(matches / 5.0, 1.0)


def article_hash(title: str, url: str) -> str:
    """Genera hash univoco per deduplicazione."""
    key = f'{title.lower().strip()}|{url.strip()}'
    return hashlib.md5(key.encode()).hexdigest()


def normalize_title(title: str) -> str:
    """Normalizza titolo per confronto dedup (lowercase, no punteggiatura)."""
    return re.sub(r'[^a-z0-9\s]', '', title.lower()).strip()


def is_similar(title_a: str, title_b: str, threshold: float = 0.75) -> bool:
    """Confronto similarità semplice senza dipendenze pesanti.
    Usa set di parole (Jaccard similarity) come alternativa leggera a Levenshtein.
    """
    words_a = set(normalize_title(title_a).split())
    words_b = set(normalize_title(title_b).split())
    if not words_a or not words_b:
        return False
    intersection = words_a & words_b
    union = words_a | words_b
    return len(intersection) / len(union) >= threshold


def parse_date(entry) -> str:
    """Estrae data ISO8601 da un feed entry."""
    for field in ('published_parsed', 'updated_parsed'):
        tp = getattr(entry, field, None)
        if tp:
            try:
                dt = datetime(*tp[:6], tzinfo=timezone.utc)
                return dt.isoformat()
            except Exception:
                pass
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Fetchers
# ---------------------------------------------------------------------------
def fetch_rss_feed(source: dict, tier: int) -> list[dict]:
    """Fetcha un singolo feed RSS e restituisce articoli normalizzati."""
    url = source['url']
    name = source['name']
    category = source.get('category', 'finanza')

    try:
        feed = feedparser.parse(url, agent=USER_AGENT)
        if feed.bozo and not feed.entries:
            logger.warning(f'Feed RSS non valido o vuoto: {name} ({url})')
            return []

        cutoff = datetime.now(timezone.utc) - timedelta(hours=36)
        articles = []

        for entry in feed.entries[:30]:  # Max 30 entries per feed
            title = clean_html(getattr(entry, 'title', ''))
            if not title:
                continue

            # Snippet: usa summary o content
            snippet = ''
            if hasattr(entry, 'summary'):
                snippet = clean_html(entry.summary)
            elif hasattr(entry, 'content'):
                snippet = clean_html(entry.content[0].get('value', ''))

            link = getattr(entry, 'link', url)
            date_str = parse_date(entry)

            # Filtro per data (ultime 36 ore — margine generoso)
            try:
                article_dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                if article_dt < cutoff:
                    continue
            except Exception:
                pass  # Se non riusciamo a parsare la data, includiamo l'articolo

            score = relevance_score(title, snippet)

            articles.append({
                'title': title,
                'url': link,
                'source': name,
                'tier': tier,
                'category': category,
                'snippet': snippet,
                'date': date_str,
                'relevance_score': round(score, 3),
            })

        logger.info(f'✓ {name}: {len(articles)} articoli (tier {tier})')
        return articles

    except Exception as e:
        logger.error(f'✗ {name}: {e}')
        return []


def fetch_webfetch_source(source: dict) -> list[dict]:
    """Web-scrapes una pagina per fonti senza RSS nativo (Tier 3)."""
    url = source['url']
    name = source['name']
    category = source.get('category', 'finanza')

    # Controlla frequenza: weekly → solo lunedì
    frequency = source.get('frequency', 'daily')
    if frequency == 'weekly' and datetime.now(timezone.utc).weekday() != 0:
        logger.info(f'⏩ {name}: fonte settimanale, skip (non è lunedì)')
        return []

    try:
        headers = {'User-Agent': USER_AGENT}
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, 'html.parser')

        # Estrai titoli e link dai tag <a> con testo significativo
        articles = []
        seen = set()
        for link_tag in soup.find_all('a', href=True):
            text = link_tag.get_text(strip=True)
            href = link_tag['href']

            # Filtra: solo link con titoli lunghi (almeno 20 chars) e non duplicati
            if len(text) < 20 or text in seen:
                continue
            seen.add(text)

            # Normalizza URL relativo
            if href.startswith('/'):
                from urllib.parse import urljoin
                href = urljoin(url, href)

            if not href.startswith('http'):
                continue

            score = relevance_score(text, '')

            articles.append({
                'title': text,
                'url': href,
                'source': name,
                'tier': 3,
                'category': category,
                'snippet': '',
                'date': datetime.now(timezone.utc).isoformat(),
                'relevance_score': round(score, 3),
            })

            if len(articles) >= 10:
                break

        logger.info(f'✓ {name}: {len(articles)} articoli (web_fetch)')
        return articles

    except Exception as e:
        logger.error(f'✗ {name} (web_fetch): {e}')
        return []


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
def deduplicate(articles: list[dict]) -> list[dict]:
    """Rimuove articoli duplicati per hash e similarità titolo."""
    seen_hashes = set()
    seen_titles = []
    unique = []

    for art in articles:
        h = article_hash(art['title'], art['url'])
        if h in seen_hashes:
            continue

        # Controlla similarità con titoli già visti
        is_dup = False
        for seen_title in seen_titles:
            if is_similar(art['title'], seen_title):
                is_dup = True
                break

        if is_dup:
            continue

        seen_hashes.add(h)
        seen_titles.append(art['title'])
        unique.append(art)

    removed = len(articles) - len(unique)
    if removed > 0:
        logger.info(f'🔄 Dedup: rimossi {removed} duplicati, restano {len(unique)}')
    return unique


def apply_caps(articles: list[dict]) -> list[dict]:
    """Applica cap per categoria: rispetta i limiti configurati."""
    cat_counts: dict[str, int] = {}
    filtered = []

    for art in articles:
        cat = art['category']
        cap = CATEGORY_CAPS.get(cat, 10)
        current = cat_counts.get(cat, 0)

        if current < cap:
            filtered.append(art)
            cat_counts[cat] = current + 1

    removed = len(articles) - len(filtered)
    if removed > 0:
        logger.info(f'📊 Caps: rimossi {removed} articoli oltre il limite')
    return filtered


def run():
    """Pipeline principale: fetch → dedup → score → cap → output JSON."""
    config = load_config()
    all_articles: list[dict] = []

    # Fetch Tier 1
    for source in config.get('sources', {}).get('tier1', []):
        all_articles.extend(fetch_rss_feed(source, tier=1))

    # Fetch Tier 2
    for source in config.get('sources', {}).get('tier2', []):
        all_articles.extend(fetch_rss_feed(source, tier=2))

    # Fetch Tier 3 (web_fetch)
    for source in config.get('sources', {}).get('tier3_webfetch', []):
        stype = source.get('type', 'rss')
        if stype == 'rss':
            all_articles.extend(fetch_rss_feed(source, tier=3))
        else:
            all_articles.extend(fetch_webfetch_source(source))

    # Fetch Tier 4
    for source in config.get('sources', {}).get('tier4', []):
        all_articles.extend(fetch_rss_feed(source, tier=4))

    # Fetch Custom
    for source in config.get('sources', {}).get('custom', []):
        all_articles.extend(fetch_rss_feed(source, tier=4))

    logger.info(f'\n📰 Totale grezzo: {len(all_articles)} articoli')

    # Deduplicazione
    all_articles = deduplicate(all_articles)

    # Ordina: tier ASC (priorità fonti istituzionali), poi relevance DESC
    all_articles.sort(key=lambda a: (a['tier'], -a['relevance_score']))

    # Applica caps per categoria
    all_articles = apply_caps(all_articles)

    # Cap globale: max 40 articoli
    if len(all_articles) > 40:
        all_articles = all_articles[:40]
        logger.info('✂️ Cap globale: ridotti a 40 articoli')

    # Salva output
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(all_articles, f, ensure_ascii=False, indent=2)

    logger.info(f'✅ Output salvato: {OUTPUT_PATH} ({len(all_articles)} articoli)')
    return all_articles


if __name__ == '__main__':
    run()
