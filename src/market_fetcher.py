#!/usr/bin/env python3
import json, logging, os, requests
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
OUTPUT_PATH = ROOT / 'data' / 'market_data.json'
FRED_API_KEY = os.environ.get('FRED_API_KEY', '')

def get_yahoo_finance(symbol):
    """
    Yahoo Finance API v8 — works from GitHub Actions without keys.
    Symbols: ^GSPC, ^VIX, GC=F (gold), BZ=F (brent), ^TNX (US10Y),
             TLT, DX-Y.NYB (DXY), ^STOXX600, ^N225, 000001.SS, BTC-USD
    """
    try:
        url = (f'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}'
               f'?interval=1d&range=5d')
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            'Accept': 'application/json',
        }
        r = requests.get(url, headers=headers, timeout=15)
        data = r.json()
        result = data.get('chart', {}).get('result', [])
        if not result:
            return 'N/A', 'N/A'
        
        indicators = result[0].get('indicators', {})
        quote = indicators.get('quote', [{}])[0]
        closes = quote.get('close', [])
        closes = [c for c in closes if c is not None]
        
        # Troviamo gli ultimi due valori distinti per evitare 0% a mercati chiusi
        distinct_closes = []
        for c in closes:
            if not distinct_closes or c != distinct_closes[-1]:
                distinct_closes.append(c)
        
        if len(distinct_closes) < 2:
            return 'N/A', 'N/A'
            
        close = distinct_closes[-1]
        prev  = distinct_closes[-2]
        change_pct = ((close - prev) / prev) * 100
        
        if close > 10000:
            val = f'{close:,.0f}'
        elif close > 100:
            val = f'{close:.2f}'
        elif close > 1:
            val = f'{close:.4f}'
        else:
            val = f'{close:.6f}'
        return val, f'{change_pct:+.2f}%'
    except Exception as e:
        logger.error(f'Yahoo {symbol}: {e}')
        return 'N/A', 'N/A'

def get_stooq(symbol):
    try:
        url = f'https://stooq.com/q/d/l/?s={symbol}&i=d'
        r = requests.get(url, timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
        lines = r.text.strip().split('\n')
        if len(lines) < 3:
            return 'N/A', 'N/A'
        last = lines[-1].split(',')
        prev = lines[-2].split(',')
        close = float(last[4])
        prev_close = float(prev[4])
        change_pct = ((close - prev_close) / prev_close) * 100
        if close > 1000:
            val = f'{close:,.0f}'
        elif close > 10:
            val = f'{close:.2f}'
        else:
            val = f'{close:.4f}'
        return val, f'{change_pct:+.2f}%'
    except Exception as e:
        logger.error(f'Stooq {symbol}: {e}')
        return 'N/A', 'N/A'

def get_fred_series(series_id):
    if not FRED_API_KEY:
        return 'N/A', 'N/A'
    try:
        url = (f'https://api.stlouisfed.org/fred/series/observations'
               f'?series_id={series_id}&api_key={FRED_API_KEY}'
               f'&file_type=json&sort_order=desc&limit=14')
        r = requests.get(url, timeout=15)
        obs = [o for o in r.json()['observations'] if o['value'] != '.']
        if len(obs) < 2:
            return 'N/A', 'N/A'
        latest = float(obs[0]['value'])
        yoy_obs = obs[12] if len(obs) >= 13 else obs[-1]
        yoy_base = float(yoy_obs['value'])
        yoy_pct = ((latest - yoy_base) / yoy_base) * 100
        val_str = f'${latest/1000:.1f}T' if latest > 1000 else f'${latest:.0f}B'
        return val_str, f'{yoy_pct:+.1f}% YoY'
    except Exception as e:
        logger.error(f'FRED {series_id}: {e}')
        return 'N/A', 'N/A'

def get_global_m2_proxy():
    if not FRED_API_KEY:
        return 'N/A', 'N/A'
    try:
        us_val, us_chg = get_fred_series('M2SL')
        _, eu_chg = get_fred_series('MABMM301EZM189S')
        _, jp_chg = get_fred_series('MYAGM2JPM189S')
        _, cn_chg = get_fred_series('MYAGM2CNM189N')
        changes = [us_chg, eu_chg, jp_chg, cn_chg]
        expanding = sum(
            1 for c in changes
            if c != 'N/A' and float(
                c.replace('%','').replace(' YoY','').replace('+','')
            ) > 0
        )
        trend = ('expanding 📈' if expanding >= 3
                 else 'contracting 📉' if expanding <= 1
                 else 'flat ➡️')
        return us_val, f'{trend} | US YoY: {us_chg}'
    except Exception as e:
        logger.error(f'Global M2 proxy: {e}')
        return 'N/A', 'N/A'

def get_macro_calendar() -> dict:
    """
    Scarica dati macro USA via FRED API.
    Se il dato è recente (ultimi 45 giorni) → status released.
    Altrimenti → status upcoming con next_release dal calendario statico.
    """
    if not FRED_API_KEY:
        logger.warning('⚠️ FRED_API_KEY non configurata — macro calendar skip')
        return {}

    from datetime import timedelta

    # Calendario release date 2026 (aggiornare a inizio anno)
    # Fonte: https://www.bls.gov/schedule/ e https://www.federalreserve.gov/
    NEXT_RELEASE = {
        'cpi':        '2026-04-10',
        'core_cpi':   '2026-04-10',
        'nfp':        '2026-04-03',
        'unemployment':'2026-04-03',
        'pce_core':   '2026-03-28',
        'fed_funds':  '2026-05-07',  # prossimo FOMC
        'gdp':        '2026-04-29',
    }

    # Serie FRED → (label, unità, decimali)
    SERIES = {
        'cpi':         ('CPIAUCSL',       'CPI YoY USA',          1, '%'),
        'core_cpi':    ('CPILFESL',       'Core CPI YoY USA',     1, '%'),
        'nfp':         ('PAYEMS',         'Non-Farm Payrolls',     0, 'K'),
        'unemployment':('UNRATE',         'Unemployment Rate USA', 1, '%'),
        'pce_core':    ('PCEPILFE',       'PCE Core YoY',         1, '%'),
        'fed_funds':   ('FEDFUNDS',       'Fed Funds Rate',        2, '%'),
        'gdp':         ('A191RL1Q225SBEA','GDP QoQ USA',           1, '%'),
    }

    BASE = 'https://api.stlouisfed.org/fred/series/observations'
    cutoff = datetime.now(timezone.utc) - timedelta(days=45)
    result = {}

    for key, (series_id, label, decimals, unit) in SERIES.items():
        try:
            params = {
                'series_id':       series_id,
                'api_key':         FRED_API_KEY,
                'file_type':       'json',
                'sort_order':      'desc',
                'limit':           2,
                'observation_start': '2024-01-01',
            }
            r = requests.get(BASE, params=params, timeout=15)
            r.raise_for_status()
            obs = r.json().get('observations', [])

            if not obs:
                raise ValueError('Nessuna osservazione')

            latest = obs[0]
            previous = obs[1] if len(obs) > 1 else None

            val_raw = latest.get('value', '.')
            date_str = latest.get('date', '')

            # Controlla se il dato è recente
            try:
                obs_dt = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                is_recent = obs_dt >= cutoff
            except Exception:
                is_recent = False

            if val_raw == '.' or not is_recent:
                # Dato non ancora uscito
                result[key] = {
                    'label':        label,
                    'value':        None,
                    'unit':         unit,
                    'status':       'upcoming',
                    'next_release': NEXT_RELEASE.get(key, 'N/A'),
                    'consensus':    None,
                }
                logger.info(f'📅 {label}: upcoming ({NEXT_RELEASE.get(key)})')
                continue

            # Formatta valore
            val = float(val_raw)
            
            # NFP: converti da migliaia, calcola variazione MoM
            if key == 'nfp':
                val_fmt = f'{val/1000:+.0f}K' if previous else f'{val/1000:.0f}K'
                prev_val = float(previous['value']) / 1000 if previous and previous['value'] != '.' else None
            else:
                val_fmt = f'{val:.{decimals}f}{unit}'
                prev_val = float(previous['value']) if previous and previous['value'] != '.' else None

            # Calcola YoY per CPI/PCE (FRED dà livello, non variazione)
            if key in ('cpi', 'core_cpi', 'pce_core') and previous:
                # FRED CPIAUCSL è già indice — calcola YoY manualmente
                # Per semplicità usiamo la variazione rispetto al dato precedente come proxy
                change = val - prev_val if prev_val else 0
                beat = None  # senza consensus non possiamo determinarlo
            else:
                change = (val - prev_val) if prev_val else 0
                beat = None

            result[key] = {
                'label':        label,
                'value':        val_fmt,
                'raw':          val,
                'previous':     f'{prev_val:.{decimals}f}{unit}' if prev_val else 'N/A',
                'change':       f'{change:+.{decimals}f}',
                'unit':         unit,
                'release_date': date_str,
                'status':       'released',
                'beat':         beat,
                'next_release': NEXT_RELEASE.get(key, 'N/A'),
            }
            logger.info(f'✅ {label}: {val_fmt} ({date_str})')

        except Exception as e:
            logger.error(f'✗ FRED {key} ({series_id}): {e}')
            result[key] = {
                'label':        label,
                'value':        None,
                'unit':         unit,
                'status':       'error',
                'next_release': NEXT_RELEASE.get(key, 'N/A'),
            }

    return result

def run():
    logger.info('📈 Recupero dati di mercato...')
    results = {}

    val, chg = get_yahoo_finance('DX-Y.NYB')
    results['dxy'] = {'value': val, 'change': chg}
    logger.info(f'DXY: {val}')

    val, chg = get_yahoo_finance('^GSPC')
    results['sp500'] = {'value': val, 'change': chg}
    logger.info(f'S&P 500: {val}')

    val, chg = get_yahoo_finance('^VIX')
    results['vix'] = {'value': val, 'change': chg}
    logger.info(f'VIX: {val}')

    val, chg = get_yahoo_finance('TLT')
    results['tlt'] = {'value': val, 'change': chg}
    logger.info(f'TLT: {val}')

    val, chg = get_yahoo_finance('^TNX')
    results['us_10y'] = {'value': f'{val}%' if val != 'N/A' else 'N/A', 'change': chg}
    logger.info(f'US 10Y: {val}')

    val, chg = get_yahoo_finance('GC=F')
    results['gold'] = {'value': f'${val}/oz' if val != 'N/A' else 'N/A', 'change': chg}
    logger.info(f'Gold: {val}')

    val, chg = get_yahoo_finance('BTC-USD')
    results['btcusd'] = {'value': f'${val}' if val != 'N/A' else 'N/A', 'change': chg}
    logger.info(f'BTC: {val}')

    val, chg = get_yahoo_finance('BZ=F')
    results['oil_brent'] = {'value': f'${val}' if val != 'N/A' else 'N/A', 'change': chg}
    logger.info(f'Brent: {val}')

    val, chg = get_yahoo_finance('^STOXX600')
    results['stoxx_600'] = {'value': val, 'change': chg}
    logger.info(f'STOXX 600: {val}')

    val, chg = get_yahoo_finance('^N225')
    results['nikkei'] = {'value': val, 'change': chg}
    logger.info(f'Nikkei: {val}')

    val, chg = get_yahoo_finance('000001.SS')
    results['shanghai'] = {'value': val, 'change': chg}
    logger.info(f'Shanghai: {val}')

    val, chg = get_stooq('10ity.b')
    results['btp_10y'] = {'value': f'{val}%' if val != 'N/A' else 'N/A', 'change': chg}
    logger.info(f'BTP 10Y: {val}')

    val, chg = get_global_m2_proxy()
    results['global_m2'] = {'value': val, 'change': chg}
    logger.info(f'Global M2: {val} ({chg})')

    # Macro calendar
    logger.info('📅 Fetching macro calendar...')
    results['macro_calendar'] = get_macro_calendar()

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    logger.info(f'✅ Dati mercato salvati: {OUTPUT_PATH}')
    return results

if __name__ == '__main__':
    run()
