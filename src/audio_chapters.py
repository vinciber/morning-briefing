"""Validated narrative chapters, separate from the measured audio manifest."""
import json
import re

TITLES = {
    'intro': ('Apertura', 'Opening'), 'finance': ('Mercati', 'Markets'),
    'asia': ('Mercati Asiatici', 'Asian Markets'),
    'occidente': ('Mercati Occidentali', 'Western Markets'),
    'geopolitica': ('Geopolitica', 'Geopolitics'),
    'macro': ('Macro ed Economia', 'Macroeconomics'),
    'chiusure': ('Chiusure', 'Market Closes'),
    'notizie': ('Notizie e Outlook', 'News and Outlook'),
    'wall_street': ('Wall Street', 'Wall Street'), 'europa': ('Europa', 'Europe'),
    'commodities': ('Materie Prime', 'Commodities'),
    'crypto': ('Criptovalute', 'Cryptocurrencies'), 'close': ('Chiusura', 'Closing'),
}

MONTHS_IT = (
    'gennaio', 'febbraio', 'marzo', 'aprile', 'maggio', 'giugno',
    'luglio', 'agosto', 'settembre', 'ottobre', 'novembre', 'dicembre',
)
MONTHS_EN = (
    'January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December',
)


def sanitize_audio_text(text, language='it'):
    """Remove recurring LLM speech errors before text is stored or synthesized."""
    if not isinstance(text, str):
        return text

    # The model occasionally translates the financial instrument "oro" as a
    # non-existent "orologio di oro" / "gold clock".  This is deterministic
    # cleanup, not a rewrite of the surrounding financial claim.
    if language == 'en':
        def replace_gold_clock_en(match):
            return 'The gold' if match.group(0)[0].isupper() else 'the gold'

        text = re.sub(
            r'\bthe gold clock\b',
            replace_gold_clock_en,
            text,
            flags=re.IGNORECASE,
        )
    else:
        def replace_gold_clock(match):
            return "L'oro" if match.group(0)[0].isupper() else "l'oro"

        text = re.sub(
            r"\b(?:l['’]orologio|il\s+orologio)\s+(?:di\s+|dell['’])oro\b",
            replace_gold_clock,
            text,
            flags=re.IGNORECASE,
        )

    # Dates are data for the calendar, but slash notation is spoken literally
    # by Piper.  Convert only valid day/month pairs, leaving values such as
    # /oz and ordinary prose untouched.
    date_pattern = re.compile(
        r'(?<![\d/])(?P<day>0?[1-9]|[12]\d|3[01])/'
        r'(?P<month>0?[1-9]|1[0-2])(?![\d/])'
    )

    def replace_date(match):
        day = int(match.group('day'))
        month = int(match.group('month')) - 1
        if language == 'en':
            return f'{MONTHS_EN[month]} {day}'
        return f'{day} {MONTHS_IT[month]}'

    return date_pattern.sub(replace_date, text)


def narrative_text(value):
    """Recover text only; never speak metadata such as ids or localized titles."""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        return '\n\n'.join(filter(None, (narrative_text(v) for v in value)))
    if isinstance(value, dict):
        for key in ('text', 'audio_script_it', 'audio_script_en'):
            if key in value:
                return narrative_text(value[key])
        return '\n\n'.join(narrative_text(v) for k, v in value.items()
                            if k not in ('id', 'title') and narrative_text(v))
    return ''


def make_it_chapters(finance, crypto, close, *, saturday=False, sunday=False):
    finance = finance.get('chapters_it', finance) if isinstance(finance, dict) else finance
    keys = ['apertura']
    keys += (['chiusure', 'notizie'] if saturday else
             ['wall_street', 'europa', 'asia', 'commodities'] if sunday else
             ['asia', 'occidente', 'geopolitica'])
    keys += ['macro']
    chapters = []
    def add(cid, text):
        if text:
            chapters.append({
                'id': cid,
                'title': TITLES[cid][0],
                'text': sanitize_audio_text(text, 'it'),
            })
    if isinstance(finance, dict) and any(k in finance for k in keys):
        for key in keys:
            add('intro' if key == 'apertura' else key, narrative_text(finance.get(key)))
    else:
        add('finance', narrative_text(finance))
    for cid, value in [('crypto', crypto), ('close', close)]:
        add(cid, narrative_text(value.get(cid, value) if isinstance(value, dict) else value))
    if not chapters or not any(c['id'] not in ('crypto', 'close') for c in chapters):
        raise ValueError('Financial audio content missing')
    return chapters


def validate_narrative(chapters, language='it', expected_ids=None):
    if not isinstance(chapters, list) or not chapters:
        return []
    result, seen = [], set()
    for chapter in chapters:
        if not isinstance(chapter, dict):
            return []
        cid, text = chapter.get('id'), chapter.get('text')
        if cid not in TITLES or cid in seen or not isinstance(text, str) or not text.strip():
            return []
        seen.add(cid)
        result.append({
            'id': cid,
            'title': TITLES[cid][language == 'en'],
            'text': sanitize_audio_text(text.strip(), 'en' if language == 'en' else 'it'),
        })
    if expected_ids is not None and set(expected_ids) != seen:
        return []
    if expected_ids is not None:
        indexed = {c['id']: c for c in result}
        result = [indexed[cid] for cid in expected_ids]
    return result


def translated_chapters(response, source):
    value = response.get('audio_chapters_en') if isinstance(response, dict) else None
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            value = None
    chapters = validate_narrative(value, 'en', [c['id'] for c in source])
    # An invalid translation keeps usable narration, but has no fabricated chapter map.
    fallback = narrative_text(value) if value is not None else narrative_text(response)
    fallback = sanitize_audio_text(fallback, 'en')
    return chapters, '\n\n'.join(c['text'] for c in chapters) if chapters else fallback
