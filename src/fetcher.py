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
from difflib import SequenceMatcher
from dotenv import load_dotenv

load_dotenv()

import feedparser
import requests
import yaml
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
feedparser.USER_AGENT = USER_AGENT

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

TITLE_BLACKLIST = [
    # Real estate / personal finance
    'mortgage', 'heloc', 'real estate', 'housing market', 'rent',
    'credit card', 'personal loan', 'rating', 'downgrade', 'upgrade',
    'zillow', 'redfin', 'realtor', 'home prices', 'savings interest',
    'best rates', 'how to buy', 'first-time homebuyer', 'refinance',
    # Sports
    'calcio', 'serie a', 'serie b', 'champions league', 'europa league',
    'uefa', 'fifa', 'football', 'soccer', 'club tricolori', 'fuorigioco',
    'calciomercato', 'gol', 'partita', 'scudetto', 'coppa italia',
    'basket', 'nba', 'tennis', 'formula 1', 'f1', 'motogp', 'olimpiadi',
    'rugby', 'ciclismo', 'giro d\'italia', 'tour de france',
    # Entertainment / gossip
    'celebrity', 'gossip', 'entertainment', 'movie', 'film', 'tv show',
    'reality', 'red carpet', 'oscar', 'grammy', 'netflix', 'spotify',
    'streaming', 'box office', 'influencer',
    # Other noise
    'horoscope', 'oroscopo', 'meteo', 'weather forecast', 'ricetta',
    'recipe', 'travel tips', 'vacanze',
    # Politica nazionale di basso impatto sui mercati (scandali, gossip politico)
    'vip lounge', 'spese pazze', 'rimborsi', 'condannato', 'arrestato',
    'inchiesta', 'indagato', 'dimissioni', 'scandalo', 'bufera',
    'consigliere comunale', 'sindaco', 'assessore', 'parlamentare',
    'al via il processo', 'processo per', 'contestazione', 'manifestazione',
]

# Titoli rifiutati nei webfetch (link nav/footer/legal di asset manager pages)
WEBFETCH_NAV_BLACKLIST = [
    'privacy policy', 'cookie policy', 'cookie', 'terms of use', 'terms and conditions',
    'legal', 'sitemap', 'site map', 'careers', 'contact us', 'contact',
    'login', 'sign in', 'register', 'subscribe', 'newsletter signup',
    'investment stewardship', 'our company', 'our firm', 'about us',
    'local websites', 'regional sites', 'change region', 'global home',
    'rss feed', 'follow us', 'social media', 'help & support', 'help center',
    'accessibility', 'disclaimer', 'modern slavery', 'do not sell',
]

TIER_SCORE = {1: 1.0, 2: 0.75, 3: 0.5, 4: 0.3}
CATEGORY_CAPS = {
    'mercati':        8,
    'geopolitica':    4,
    'macro_economia': 7,
    'energia':        4,
    'crypto':         4,
}
GLOBAL_CAP = 25



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
def _fetch_pimco(source: dict, tier: int = 2) -> list[dict]:
    """Scraper ad hoc per PIMCO Insights."""
    url = source['url']
    name = source['name']
    
    try:
        headers = {'User-Agent': USER_AGENT}
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        articles = []
        
        # PIMCO structure: insights usually in cards
        for card in soup.select('.insight-card, .article-card, .list-item')[:3]:
            title_tag = card.select_one('h3, h4, .title')
            link_tag = card.select_one('a[href]')
            
            if title_tag and link_tag:
                title = title_tag.get_text(strip=True)
                href = link_tag['href']
                if href.startswith('/'):
                    from urllib.parse import urljoin
                    href = urljoin(url, href)
                
                score = relevance_score(title, '')
                articles.append({
                    'title': title,
                    'url': href,
                    'source': name,
                    'tier': tier,
                    'category': 'finanza',
                    'snippet': '',
                    'date': datetime.now(timezone.utc).isoformat(),
                    'relevance_score': round(score, 3),
                })
        
        # Fallback if specific selectors fail (try all <a> with significant text)
        if not articles:
            seen = set()
            for a in soup.find_all('a', href=True):
                text = a.get_text(strip=True)
                if len(text) > 40 and text not in seen:
                    href = a['href']
                    if href.startswith('/'):
                        from urllib.parse import urljoin
                        href = urljoin(url, href)
                    
                    score = relevance_score(text, '')
                    articles.append({
                        'title': text,
                        'url': href,
                        'source': name,
                        'tier': 3,
                        'category': 'finanza',
                        'snippet': '',
                        'date': datetime.now(timezone.utc).isoformat(),
                        'relevance_score': round(score, 3),
                    })
                    seen.add(text)
                    if len(articles) >= 3: break

        logger.info(f'✓ {name}: {len(articles)} articoli (scraper)')
        return articles
    except Exception as e:
        logger.error(f'✗ {name} (scraper): {e}')
        return []

def _calculate_cross_reference_score(articles: list[dict]):
    """Aumenta lo score se un tema compare in più fonti istituzionali."""
    themes = ['inflation', 'fed', 'ecb', 'bce', 'rates', 'china', 'energy', 'oil', 'growth', 'recession', 'debt']
    theme_counts = {t: 0 for t in themes}
    
    # Conta occorrenze nei titoli (fonti Tier 1 e 2)
    for art in articles:
        if art.get('tier', 4) <= 2:
            title = art.get('title', '').lower()
            for t in themes:
                if t in title:
                    theme_counts[t] += 1
                    
    # Applica bonus (max +0.2)
    for art in articles:
        title = art.get('title', '').lower()
        bonus = 0
        for t, count in theme_counts.items():
            if t in title and count > 1:
                bonus += 0.05 * min(count, 4)
        art['relevance_score'] = min(art.get('relevance_score', 0) + bonus, 1.0)

def fetch_rss_feed(source: dict, tier: int) -> list[dict]:
    """Fetcha un singolo feed RSS e restituisce articoli normalizzati."""
    url = source['url']
    name = source['name']
    category = source.get('category', 'finanza')

    # Header più robusti per bypass blocchi (IMF, PIIE, etc.)
    headers = {
        'User-Agent': USER_AGENT,
        'Accept': 'application/rss+xml, application/xml;q=0.9, */*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    try:
        # feedparser can take a request object or string. For custom headers we fetch with requests first
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)
        
        if feed.bozo and not feed.entries:
            logger.warning(f'Feed RSS non valido o vuoto: {name} ({url})')
            return []

        cutoff = datetime.now(timezone.utc) - timedelta(hours=36)
        articles = []

        for entry in feed.entries[:30]:  # Max 30 entries per feed
            title = clean_html(getattr(entry, 'title', ''))
            if not title:
                continue

            # Filtro blacklist (Problem 3)
            if any(term in title.lower() for term in TITLE_BLACKLIST):
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

            # Fallback snippet (Problem 3)
            if not snippet:
                snippet = title

            score = relevance_score(title, snippet)

            articles.append({
                'title': title,
                'url': link,
                'source': name,
                'tier': tier,
                'category': normalize_category(source.get('category', 'mercati')), # Normalize here (Problem 2)
                'snippet': snippet,
                'date': date_str,
                'relevance_score': round(score, 3),
            })

        logger.info(f'✓ {name}: {len(articles)} articoli (tier {tier})')
        return articles

    except Exception as e:
        logger.error(f'✗ {name}: {e}')
        return []


def fetch_webfetch_source(source: dict, tier: int = 3) -> list[dict]:
    """Web-scrapes una pagina per fonti senza RSS nativo.
    Filtra link di nav/footer/legal e titoli generici per evitare spazzatura
    (es. BlackRock raccoglieva 'Privacy Policy', 'Investment Stewardship', ...).
    """
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

            # Filtra: solo link con titoli lunghi (almeno 25 chars) e non duplicati
            if len(text) < 25 or text in seen:
                continue

            # Detect duplicato interno tipo "Our companyOur company"
            half = len(text) // 2
            if half >= 6 and text[:half].strip() == text[half:].strip():
                continue

            # Filtra titoli nav/footer/legal
            text_lower = text.lower()
            if any(term in text_lower for term in WEBFETCH_NAV_BLACKLIST):
                continue

            # Path filter: deve apparire un segmento "insight"/"research"/"article"/"publication"
            href_lower = href.lower()
            allowed_path_terms = ['insight', 'research', 'article', 'publication',
                                  'commentary', 'view', 'analysis', 'perspective',
                                  'report', 'outlook', 'paper', 'whitepaper',
                                  'wealth-management-insights', 'learning-center']
            if not any(term in href_lower for term in allowed_path_terms):
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
                'tier': tier,
                'category': normalize_category(source.get('category', 'mercati')), # Normalize here (Problem 2)
                'snippet': '',
                'date': datetime.now(timezone.utc).isoformat(),
                'relevance_score': round(score, 3),
            })

            if len(articles) >= 5:
                break

        logger.info(f'✓ {name}: {len(articles)} articoli (web_fetch)')
        return articles

    except Exception as e:
        logger.error(f'✗ {name} (web_fetch): {e}')
        return []


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
def title_similarity(a, b):
    return SequenceMatcher(None,
        a.lower().strip(),
        b.lower().strip()
    ).ratio()

CATEGORY_REMAP = {
    'banche_centrali': 'macro_economia',
    'finanza':         'mercati',
    'economia':        'macro_economia',
    'energia':        'energia',
    'politica':        'geopolitica',
    'crypto':          'crypto',
}

def normalize_category(cat):
    return CATEGORY_REMAP.get(cat, cat)

def smart_select(articles):
    
    is_monday = datetime.now(timezone.utc).weekday() == 0

    # PASSAGGIO 0 — Proteggi report settimanali il lunedì
    # Questi articoli bypassano i cap di categoria ma restano nel conteggio globale
    weekly_sources = ['BlackRock Investment Institute', 'Goldman Sachs Insights',
                      'PIMCO Insights', 'Apollo Academy', 'Vanguard Insights', 'Fidelity Insights']
    weekly_protected = []
    regular_articles = []
    
    if is_monday:
        for art in articles:
            if art.get('source') in weekly_sources:
                weekly_protected.append(art)
            else:
                regular_articles.append(art)
        logger.info(f'📅 Lunedì: {len(weekly_protected)} articoli settimanali protetti '
                    f'(BlackRock: {sum(1 for a in weekly_protected if "BlackRock" in a.get("source",""))}, '
                    f'Goldman: {sum(1 for a in weekly_protected if "Goldman" in a.get("source",""))})')
    else:
        regular_articles = articles

    # PASSAGGIO 1 — score composito (solo articoli regolari)
    for art in regular_articles:
        tier = art.get('tier', 4)
        relevance = art.get('relevance_score', 0)
        tier_w = TIER_SCORE.get(tier, 0.2)
        # relevance / 5 was in previous version, the user prompt suggests relevance (0-1 range assumed)
        # Keeping relevance * 0.4 as per user instruction
        art['_score'] = (tier_w * 0.6) + (relevance * 0.4)

    regular_articles = sorted(regular_articles, key=lambda x: x['_score'], reverse=True)

    # PASSAGGIO 2 — deduplicazione semantica
    deduplicated = []
    for candidate in regular_articles:
        title_c = candidate.get('title', '')
        is_duplicate = False
        for kept in deduplicated:
            title_k = kept.get('title', '')
            if title_similarity(title_c, title_k) > 0.70:
                is_duplicate = True
                break
        if not is_duplicate:
            deduplicated.append(candidate)

    # PASSAGGIO 3 — cap per categoria + cap per singola fonte
    SOURCE_CAP = 2  # Max 2 articoli per fonte (evita dominanza di una singola fonte)
    category_counts = {cat: 0 for cat in CATEGORY_CAPS}
    source_counts = {}
    selected = []

    # Riserva slot per i weekly (max 6 slot totali tra BlackRock e Goldman)
    weekly_slots = min(len(weekly_protected), 6) if is_monday else 0
    effective_cap = GLOBAL_CAP - weekly_slots

    for art in deduplicated:
        src = art.get('source', '')
        if source_counts.get(src, 0) >= SOURCE_CAP:
            continue
        cat = normalize_category(art.get('category', 'mercati'))
        if cat not in category_counts:
            category_counts[cat] = 0
        cap = CATEGORY_CAPS.get(cat, 4)
        if category_counts[cat] < cap:
            selected.append(art)
            category_counts[cat] += 1
            source_counts[src] = source_counts.get(src, 0) + 1
        if len(selected) >= effective_cap:
            break

    # Aggiungi weekly protetti in coda — max 2 per fonte (filtro di rilievo)
    if is_monday and weekly_protected:
        weekly_protected.sort(key=lambda x: x.get('relevance_score', 0), reverse=True)
        weekly_per_source = {}
        weekly_to_add = []
        for art in weekly_protected:
            src = art.get('source', '')
            if weekly_per_source.get(src, 0) >= 2:
                continue
            weekly_to_add.append(art)
            weekly_per_source[src] = weekly_per_source.get(src, 0) + 1
            if len(weekly_to_add) >= weekly_slots:
                break
        selected.extend(weekly_to_add)
        logger.info(f'✅ Aggiunti {len(weekly_to_add)} articoli settimanali al feed '
                    f'(max 2 per fonte tra {len(weekly_per_source)} fonti)')

    logger.info(f'🧠 Smart select: {len(articles)} → {len(deduplicated)} '
                f'(dedup) → {len(selected)} (final)')
    for cat, count in category_counts.items():
        if count > 0:
            logger.info(f'   {cat}: {count} articoli')

    for art in selected:
        art.pop('_score', None)

    return selected


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

    # Fetch Tier 2 webfetch / scraper (asset manager insights istituzionali)
    is_monday_for_weekly = datetime.now(timezone.utc).weekday() == 0
    for source in config.get('sources', {}).get('tier2_webfetch', []):
        # Skip fonti settimanali se non è lunedì
        if source.get('frequency') == 'weekly' and not is_monday_for_weekly:
            logger.info(f'⏩ {source.get("name")}: fonte settimanale, skip (non è lunedì)')
            continue
        stype = source.get('type', 'rss')
        if stype == 'rss':
            all_articles.extend(fetch_rss_feed(source, tier=2))
        elif stype == 'scraper':
            all_articles.extend(_fetch_pimco(source, tier=2))
        else:
            all_articles.extend(fetch_webfetch_source(source, tier=2))
            
    # Task 4: Cross-Reference Scoring
    _calculate_cross_reference_score(all_articles)

    # Fetch Tier 4
    for source in config.get('sources', {}).get('tier4', []):
        all_articles.extend(fetch_rss_feed(source, tier=4))

    # Fetch Custom
    for source in config.get('sources', {}).get('custom', []):
        all_articles.extend(fetch_rss_feed(source, tier=4))

    logger.info(f'\n📰 Totale grezzo: {len(all_articles)} articoli')

    # Filtra articoli con score troppo basso
    before = len(all_articles)
    PROTECTED_SOURCES = ['BlackRock Investment Institute', 'Goldman Sachs Insights',
                         'PIMCO Insights', 'Apollo Academy', 'Vanguard Insights', 'Fidelity Insights']
    # Generaliste: soglia più alta per scartare scandali/politica locale a basso impatto
    GENERAL_SOURCES = ['Repubblica Economia', 'Corriere Economia', 'Il Sole 24 Ore (Mondo)',
                       'Il Sole 24 Ore (Finanza)', 'Milano Finanza', 'Teleborsa']
    is_monday = datetime.now(timezone.utc).weekday() == 0
    logger.info(f'📅 Debug Lunedì: {is_monday} (UTC weekday: {datetime.now(timezone.utc).weekday()})')

    def _passes_filter(a):
        src = a.get('source', '')
        score = a.get('relevance_score', 0)
        if src in PROTECTED_SOURCES:
            return True
        if src in GENERAL_SOURCES:
            return score >= 0.5
        return score >= 0.3

    all_articles = [a for a in all_articles if _passes_filter(a)]
    logger.info(f'🗑️ Filtrati {before - len(all_articles)} articoli rumore '
                f'(soglia 0.3 / 0.5 generaliste, fonti protette preservate)')

    # Smart Selection (Scoring + Dedup + Caps)
    all_articles = smart_select(all_articles)

    # Salva output
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(all_articles, f, ensure_ascii=False, indent=2)

    logger.info(f'✅ Output salvato: {OUTPUT_PATH} ({len(all_articles)} articoli)')
    return all_articles


if __name__ == '__main__':
    run()
