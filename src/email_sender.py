#!/usr/bin/env python3
"""
email_sender.py — Invio Email HTML via Resend
Compone email HTML dal briefing con template Jinja2 e la invia via Resend API.
"""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import resend
import yaml
from jinja2 import Environment, FileSystemLoader
from dotenv import load_dotenv

# Carica variabili d'ambiente da .env se presente
load_dotenv()

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
INPUT_PATH = ROOT / 'data' / 'briefing_today.json'
TEMPLATES_DIR = ROOT / 'templates'
CONFIG_PATH = ROOT / 'config.yml'

RESEND_API_KEY = os.environ.get('RESEND_API_KEY', '')
RECIPIENT_EMAIL = os.environ.get('RECIPIENT_EMAIL', '')


def run():
    """Pipeline email: carica briefing → render HTML → invia via Resend."""
    # Controllo se disabilitato in config.yml
    if CONFIG_PATH.exists():
        try:
            with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                email_config = config.get('output', {}).get('email', {})
                if not email_config.get('enabled', True):
                    logger.info('ℹ️ Invio email disabilitato in config.yml. Salto.')
                    return True
        except Exception as e:
            logger.warning(f'⚠️ Errore lettura config.yml: {e}')

    if not RESEND_API_KEY:
        logger.error('❌ RESEND_API_KEY non configurata!')
        return False

    if not RECIPIENT_EMAIL:
        logger.error('❌ RECIPIENT_EMAIL non configurata!')
        return False

    if not INPUT_PATH.exists():
        logger.error(f'❌ File non trovato: {INPUT_PATH}. Esegui prima summarizer.py')
        return False

    with open(INPUT_PATH, 'r', encoding='utf-8') as f:
        briefing = json.load(f)

    date = briefing.get('date', datetime.now(timezone.utc).strftime('%Y-%m-%d'))
    sentiment = briefing.get('sentiment', {})
    sentiment_label = sentiment.get('label', 'neutral')

    emoji_map = {'risk_on': '🟢', 'risk_off': '🔴', 'neutral': '🟡'}
    emoji = emoji_map.get(sentiment_label, '🟡')

    # Render template HTML
    env = Environment(loader=FileSystemLoader(str(TEMPLATES_DIR)))
    template = env.get_template('email.html')

    sentiment_colors = {'risk_on': '#10b981', 'risk_off': '#ef4444', 'neutral': '#64748b'}
    color = sentiment_colors.get(sentiment_label, '#64748b')

    html_content = template.render(
        briefing=briefing,
        date=date,
        sentiment=sentiment,
        sentiment_emoji=emoji,
        sentiment_color=color,
        market_data=briefing.get('market_data', {}),
    )

    # Invio via Resend
    resend.api_key = RESEND_API_KEY

    subject = f'{emoji} Morning Briefing — {date}'

    try:
        result = resend.Emails.send({
            'from': 'Morning Briefing <briefing@resend.dev>',
            'to': [RECIPIENT_EMAIL],
            'subject': subject,
            'html': html_content,
        })

        logger.info(f'✅ Email inviata a {RECIPIENT_EMAIL}')
        logger.info(f'   ID: {result.get("id", "N/A")}')
        return True

    except Exception as e:
        logger.error(f'❌ Errore invio email: {e}')
        return False


if __name__ == '__main__':
    run()
