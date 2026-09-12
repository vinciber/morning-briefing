#!/usr/bin/env python3
"""
telegram_bot.py — Invio Briefing via Telegram Bot
Invia il briefing come messaggio testo (Markdown) + audio MP3.
"""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
INPUT_PATH = ROOT / 'data' / 'briefing_today.json'
AUDIO_DIR = ROOT / 'docs' / 'audio'

TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')

BASE_URL = 'https://api.telegram.org/bot'


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def escape_html(text: str) -> str:
    """Escape HTML special characters."""
    if not text:
        return ""
    return (text.replace('&', '&amp;')
                .replace('<', '&lt;')
                .replace('>', '&gt;'))


def briefing_to_html(briefing: dict) -> str:
    """Converte il briefing JSON in un messaggio Telegram formattato HTML."""
    date = briefing.get('date', datetime.now(timezone.utc).strftime('%Y-%m-%d'))
    sentiment = briefing.get('sentiment', {})
    label = sentiment.get('label', 'neutral')

    emoji_map = {'risk_on': '🟢', 'risk_off': '🔴', 'neutral': '🟡'}
    label_map = {'risk_on': 'RISK ON', 'risk_off': 'RISK OFF', 'neutral': 'NEUTRAL'}
    emoji = emoji_map.get(label, '🟡')
    label_text = label_map.get(label, label.upper())

    lines = []
    lines.append(f'📰 <b>Morning Briefing — {date}</b>')
    lines.append(f'{emoji} Sentiment: <b>{label_text}</b>')

    reason = sentiment.get('reason_it', '')
    if reason:
        lines.append(f'<i>{escape_html(reason)}</i>')
    lines.append('')

    # Market data
    market = briefing.get('market_data', {})
    if market:
        lines.append('📊 <b>Dati di Mercato</b>')
        labels = {
            'eur_usd': '💶 EUR/USD',
            'vix': '📈 VIX',
            'btp_10y': '🇮🇹 BTP 10Y',
            'us_10y': '🇺🇸 US 10Y',
            'gold': '🥇 Gold',
            'oil_brent': '🛢 Brent',
            'sp500_futures': '🇺🇸 S&P 500',
            'stoxx_600': '🇪🇺 STOXX 600',
            'nikkei': '🇯🇵 Nikkei',
            'shanghai': '🇨🇳 Shanghai',
        }
        for key, label in labels.items():
            val = market.get(key, '')
            if val:
                # Append $/oz to gold if missing
                if key == 'gold':
                    str_val = str(val).strip()
                    if not str_val.endswith('$/oz') and not str_val.endswith('$') and not 'oz' in str_val.lower():
                        val = f"{str_val} $/oz"
                lines.append(f'  {label}: <code>{escape_html(str(val))}</code>')
        lines.append('')

    # Sezioni
    section_emojis = {
        'mercati': '💹',
        'geopolitica': '🌍',
        'macro_economia': '🏛',
        'energia': '⚡',
    }
    section_names = {
        'mercati': 'Mercati',
        'geopolitica': 'Geopolitica',
        'macro_economia': 'Macroeconomia',
        'energia': 'Energia',
    }

    for section in briefing.get('sections', []):
        sec_name = section.get('name', '')
        sec_emoji = section_emojis.get(sec_name, '📌')
        sec_label = section_names.get(sec_name, sec_name.title())
        lines.append(f'{sec_emoji} <b>{sec_label}</b>')

        for item in section.get('items', []):
            importance = item.get('importance', 3)
            stars = '⭐' * min(int(importance), 5)
            title = item.get('title_it', '')
            summary = item.get('summary_it', '')
            source = item.get('source_name', '')
            url = item.get('source_url', '')

            lines.append(f'  {stars} <b>{escape_html(title)}</b>')
            if summary:
                # Truncate to ~120 chars to keep the message short
                if len(summary) > 120:
                    cut = summary[:117]
                    summary = f"{cut.rsplit(' ', 1)[0]}..." if ' ' in cut else f"{cut}..."
                lines.append(f'  {escape_html(summary)}')
            if source and url:
                lines.append(f'  📎 <a href="{url}">{escape_html(source)}</a>')
            lines.append('')

    read_more_it = f'<a href="https://www.pricealertapp.app/morning-update/{date}.html">Leggi tutto →</a>'
    read_more_en = f'<a href="https://www.pricealertapp.app/morning-update/en/{date}.html">Read more →</a>'
    lines.append(f'🌐 {read_more_it}') # Assuming Italian is the primary language for this bot

    return '\n'.join(lines)


def send_text(text: str) -> bool:
    """Invia messaggio testo via Telegram Bot API."""
    url = f'{BASE_URL}{TELEGRAM_BOT_TOKEN}/sendMessage'
    
    # Pulizia chat_id
    chat_id = str(TELEGRAM_CHAT_ID).strip()

    # Telegram ha un limite di 4096 chars per messaggio
    if len(text) > 4000:
        # Split in più messaggi
        parts = []
        current = ''
        for line in text.split('\n'):
            if len(current) + len(line) + 1 > 3900:
                parts.append(current)
                current = ''
            current += line + '\n'
        if current:
            parts.append(current)

        for i, part in enumerate(parts):
            resp = requests.post(url, json={
                'chat_id': chat_id,
                'text': part,
                'parse_mode': 'HTML',
                'disable_web_page_preview': True,
            })
            if not resp.ok:
                logger.warning(f'⚠️ Messaggio parte {i+1}: {resp.status_code} {resp.text[:200]}')
        return True
    else:
        resp = requests.post(url, json={
            'chat_id': chat_id,
            'text': text,
            'parse_mode': 'HTML',
            'disable_web_page_preview': True,
        })

        if resp.ok:
            return True
        else:
            logger.error(f'❌ Errore Telegram: {resp.status_code} {resp.text[:200]}')
            return False


def send_audio(audio_path: Path) -> bool:
    """Invia file audio MP3 via Telegram Bot API."""
    url = f'{BASE_URL}{TELEGRAM_BOT_TOKEN}/sendAudio'
    chat_id = str(TELEGRAM_CHAT_ID).strip()

    with open(audio_path, 'rb') as f:
        resp = requests.post(url, data={
            'chat_id': chat_id,
            'title': f'Morning Briefing {audio_path.stem}',
            'performer': 'Morning Briefing Agent',
        }, files={'audio': f})

    if resp.ok:
        return True
    else:
        logger.error(f'❌ Errore invio audio: {resp.status_code} {resp.text[:200]}')
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run(*, send=False, published=False, session=requests):
    from delivery_state import (Ledger, fetch_published_audio, load_published_briefing,
                                ready, today, verify_publication)
    import hashlib
    date = today()
    if published:
        briefing = load_published_briefing(date, session)
    else:
        if not ready(ROOT, date):
            raise RuntimeError('Current briefing/audio unavailable')
        briefing = json.loads(INPUT_PATH.read_text())
    if not send:
        logger.info('Dry run: validated briefing; no delivery performed')
        return True
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        raise RuntimeError('Telegram configuration missing')
    verify_publication(briefing, session)
    ledger = Ledger(briefing['date'], session)
    if ledger.state.get('complete'):
        return True
    fingerprint = hashlib.sha256(json.dumps(
        briefing, sort_keys=True, ensure_ascii=False, separators=(',', ':')
    ).encode()).hexdigest()
    if ledger.state.get('fingerprint', fingerprint) != fingerprint:
        raise RuntimeError('Daily content changed after delivery began')
    ledger.state.update(published=True, fingerprint=fingerprint)
    ledger.save()
    text = briefing_to_html(briefing)
    parts, current = [], ''
    for line in text.splitlines(keepends=True):
        if len(line) > 3900:
            raise ValueError('Telegram line too long')
        if len(current) + len(line) > 3900:
            parts.append(current)
            current = ''
        current += line
    if current: parts.append(current)
    recipient = hashlib.sha256(TELEGRAM_CHAT_ID.encode()).hexdigest()[:16]
    for index, part in enumerate(parts):
        key = f'{recipient}:text:{index}'
        ledger.deliver(key, lambda part=part: session.post(
            f'{BASE_URL}{TELEGRAM_BOT_TOKEN}/sendMessage',
            json={'chat_id': TELEGRAM_CHAT_ID, 'text': part, 'parse_mode': 'HTML',
                  'disable_web_page_preview': True}, timeout=20))
    if published:
        audio_content = fetch_published_audio(briefing, 'it', session)
        audio_file = (f"briefing_{briefing['date'].replace('-', '')}.mp3", audio_content, 'audio/mpeg')
        ledger.deliver(f'{recipient}:audio:it', lambda: session.post(
            f'{BASE_URL}{TELEGRAM_BOT_TOKEN}/sendAudio',
            data={'chat_id': TELEGRAM_CHAT_ID, 'title': f"Morning Briefing {briefing['date']}"},
            files={'audio': audio_file}, timeout=60))
    else:
        audio = AUDIO_DIR / f"briefing_{briefing['date'].replace('-', '')}.mp3"
        with audio.open('rb') as handle:
            ledger.deliver(f'{recipient}:audio:it', lambda: session.post(
                f'{BASE_URL}{TELEGRAM_BOT_TOKEN}/sendAudio',
                data={'chat_id': TELEGRAM_CHAT_ID, 'title': f"Morning Briefing {briefing['date']}"},
                files={'audio': handle}, timeout=60))
    ledger.state['complete'] = True
    ledger.save()
    return True


def delivery_error_detail(error: Exception) -> str:
    """Return a useful diagnostic without ever placing a Bot API URL in logs."""
    if isinstance(error, requests.RequestException):
        return 'network_or_http_error_url_redacted'
    if isinstance(error, (RuntimeError, ValueError)):
        return str(error)
    return type(error).__name__


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--send', action='store_true')
    parser.add_argument('--published', action='store_true',
                        help='load the deployed daily archive instead of runner-local temporary data')
    args = parser.parse_args()
    try:
        run(send=args.send, published=args.published)
    except Exception as error:
        logger.error('Delivery failed: %s; inspect the delivery ledger', delivery_error_detail(error))
        raise SystemExit(1)
