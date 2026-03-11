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
            'gold': '🥇 Gold',
            'oil_brent': '🛢 Brent',
            'sp500_futures': '🇺🇸 S&P 500',
        }
        for key, label in labels.items():
            val = market.get(key, '')
            if val:
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
                lines.append(f'  {escape_html(summary)}')
            if source and url:
                lines.append(f'  📎 <a href="{url}">{escape_html(source)}</a>')
            lines.append('')

    lines.append('🌐 <a href="https://vinciber.github.io/morning-briefing">Web App</a>')
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
def run():
    """Pipeline Telegram: carica briefing → markdown → invia testo + audio."""
    if not TELEGRAM_BOT_TOKEN:
        logger.error('❌ TELEGRAM_BOT_TOKEN non configurato!')
        return False

    if not TELEGRAM_CHAT_ID:
        logger.error('❌ TELEGRAM_CHAT_ID non configurato!')
        return False

    if not INPUT_PATH.exists():
        logger.error(f'❌ File non trovato: {INPUT_PATH}')
        return False

    with open(INPUT_PATH, 'r', encoding='utf-8') as f:
        briefing = json.load(f)

    # Invia testo
    text = briefing_to_html(briefing)
    logger.info(f'📱 Invio messaggio Telegram ({len(text)} chars)...')

    if send_text(text):
        logger.info('✅ Messaggio testo inviato')
    else:
        logger.warning('⚠️ Errore invio messaggio testo')

    # Invia audio se disponibile
    date_str = briefing.get('date', datetime.now(timezone.utc).strftime('%Y-%m-%d'))
    audio_file = AUDIO_DIR / f'briefing_{date_str.replace("-", "")}.mp3'

    if audio_file.exists():
        logger.info(f'🎙️ Invio audio: {audio_file.name}...')
        if send_audio(audio_file):
            logger.info('✅ Audio inviato')
        else:
            logger.warning('⚠️ Errore invio audio')
    else:
        logger.info('ℹ️ Nessun file audio trovato, solo messaggio testo inviato')

    return True


if __name__ == '__main__':
    run()
