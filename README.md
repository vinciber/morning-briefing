# Morning Briefing Agent

Agente autonomo che ogni mattina aggrega notizie finanziarie e geopolitiche da 23+ fonti istituzionali, le riassume con Gemini Flash, e le distribuisce via email, audio Telegram e sito web.

## 🏗 Stack (100% gratuito)

| Componente | Tecnologia |
|---|---|
| Scheduler | GitHub Actions (2.000 min/mese) |
| AI | Google Gemini 2.0 Flash |
| Audio TTS | Edge-TTS (Microsoft) |
| Email | Resend.com (3.000/mese) |
| Telegram | python-telegram-bot |
| Sito + RSS | GitHub Pages |
| Archivio | JSON nel repo |

## 📂 Struttura

```
src/
├── fetcher.py          # Aggregatore RSS + web_fetch
├── summarizer.py       # Gemini Flash → JSON bilingue
├── tts.py              # Audio MP3 (Edge-TTS)
├── email_sender.py     # Email HTML (Resend)
├── telegram_bot.py     # Testo + audio Telegram
└── site_generator.py   # HTML + RSS + API JSON
```

## 🚀 Uso

```bash
# Test locale step-by-step
pip install -r requirements.txt
python src/fetcher.py        # RSS fetch
python src/summarizer.py     # AI briefing (richiede GEMINI_API_KEY)
python src/tts.py            # Audio
python src/site_generator.py # Sito
```

## ⚙️ GitHub Secrets

| Nome | Descrizione |
|---|---|
| GEMINI_API_KEY | Google AI Studio |
| RESEND_API_KEY | Resend.com |
| RECIPIENT_EMAIL | Email destinatario |
| TELEGRAM_BOT_TOKEN | @BotFather |
| TELEGRAM_CHAT_ID | @userinfobot |

## 🔗 Integrazione Price Alert

Il file `docs/api/today.json` viene consumato dall'app Price Alert per alimentare il tab Custom ⭐ premium.
