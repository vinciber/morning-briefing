#!/usr/bin/env python3
import json, logging, os, requests
from datetime import datetime, timezone, timedelta
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
OUTPUT_PATH = ROOT / 'data' / 'market_data.json'
ETF_STATUS_PATH = ROOT.parent / 'public' / 'data' / 'etf_status.json'
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

def get_crypto_fear_greed():
    """Recupera l'indice Crypto Fear & Greed da alternative.me."""
    try:
        url = 'https://api.alternative.me/fng/'
        r = requests.get(url, timeout=10)
        data = r.json()
        val = data['data'][0]['value']
        cls = data['data'][0]['value_classification']
        return val, cls
    except Exception as e:
        logger.error(f'Fear & Greed: {e}')
        return 'N/A', 'N/A'

def get_coingecko_prices():
    """Recupera prezzi BTC, ETH, SOL da CoinGecko (API pubblica)."""
    try:
        url = 'https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,solana,binancecoin&vs_currencies=usd&include_24hr_change=true'
        r = requests.get(url, timeout=15)
        data = r.json()
        
        res = {}
        mapping = {
            'bitcoin': 'BTC',
            'ethereum': 'ETH',
            'solana': 'SOL',
            'binancecoin': 'BNB'
        }
        for cg_id, ticker in mapping.items():
            if cg_id in data:
                price = data[cg_id]['usd']
                change = data[cg_id]['usd_24h_change']
                res[ticker] = {
                    'value': f'${price:,.0f}' if price > 1000 else f'${price:.2f}',
                    'change': f'{change:+.2f}%'
                }
        return res
    except Exception as e:
        logger.error(f'CoinGecko: {e}')
        return {}

def get_crypto_data():
    """Wrapper per aggregare dati crypto."""
    fng_val, fng_cls = get_crypto_fear_greed()
    prices = get_coingecko_prices()
    return {
        'fear_greed': {'value': fng_val, 'class': fng_cls},
        'prices': prices
    }

def get_etf_flow():
    """Carica gli inflow degli ETF BTC dal file generato dallo scraper."""
    try:
        if not ETF_STATUS_PATH.exists():
            return 'N/A', 'N/A'
        with open(ETF_STATUS_PATH, 'r') as f:
            data = json.load(f)
        val = data.get('net_flow_usd_m', 0)
        return f'${val:+.1f}M', data.get('trend_indicator', '➡️')
    except Exception as e:
        logger.error(f'ETF Flow: {e}')
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
    cutoff = datetime.now(timezone.utc) - timedelta(days=120)
    result = {}

    for key, (series_id, label, decimals, unit) in SERIES.items():
        try:
            params = {
                'series_id':       series_id,
                'api_key':         FRED_API_KEY,
                'file_type':       'json',
                'sort_order':      'desc',
                'limit':           25, # Aumentato per trovare YoY (12 mesi fa)
                'observation_start': '2024-01-01',
            }
            r = requests.get(BASE, params=params, timeout=15)
            r.raise_for_status()
            obs = r.json().get('observations', [])

            if not obs:
                raise ValueError('Nessuna osservazione')

            latest = obs[0]
            val_raw = latest.get('value', '.')
            date_str = latest.get('date', '')

            # Controlla se il dato è recente (released) o vecchio (upcoming)
            try:
                obs_dt = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                is_recent = obs_dt >= cutoff
            except Exception:
                is_recent = False

            if val_raw == '.' or not is_recent:
                # Fallback: prendi l'ultimo valore disponibile anche se non recente
                latest_obs = [o for o in obs if o['value'] != '.']
                if latest_obs:
                   val_fallback = float(latest_obs[0]['value'])
                   prev_fallback = float(latest_obs[1]['value']) if len(latest_obs) > 1 else None
                   
                   # Formattazione base (senza YoY complesso per brevità nel fallback)
                   val_fmt = f'{val_fallback:.{decimals}f}{unit}'
                   prev_fmt = f'{prev_fallback:.{decimals}f}{unit}' if prev_fallback else 'N/A'
                   
                   result[key] = {
                       'label':        label,
                       'value':        val_fmt,
                       'previous':     prev_fmt,
                       'status':       'upcoming',
                       'next_release': NEXT_RELEASE.get(key, 'N/A'),
                   }
                else:
                    result[key] = {
                        'label':        label,
                        'value':        None,
                        'unit':         unit,
                        'status':       'upcoming',
                        'next_release': NEXT_RELEASE.get(key, 'N/A'),
                    }
                logger.info(f'📅 {label}: upcoming ({NEXT_RELEASE.get(key)}) - showing last: {result[key].get("value")}')
                continue

            val = float(val_raw)
            previous = obs[1] if len(obs) > 1 else None
            prev_val = float(previous['value']) if previous and previous['value'] != '.' else None

            # Calcolo Valore Visualizzato (val_fmt) e Previous
            if key in ('cpi', 'core_cpi', 'pce_core'):
                # Filtra valori nulli per calcolo YoY corretto
                clean_obs = [o for o in obs if o['value'] != '.']
                
                # Calcola YoY reale (vs 12 mesi fa)
                val = float(latest['value'])
                yoy_obs = clean_obs[12] if len(clean_obs) >= 13 else None
                if yoy_obs:
                    base_val = float(yoy_obs['value'])
                    yoy_val = ((val - base_val) / base_val) * 100
                    val_fmt = f'{yoy_val:.{decimals}f}{unit}'
                else:
                    val_fmt = f'{val:.{decimals}f}' # Fallback
                
                # Previous YoY (dato del mese scorso vs 13 mesi fa)
                prev_latest_obs = clean_obs[1] if len(clean_obs) > 1 else None
                prev_base_obs = clean_obs[13] if len(clean_obs) >= 14 else None
                
                if prev_latest_obs and prev_base_obs:
                    p_val = float(prev_latest_obs['value'])
                    p_base = float(prev_base_obs['value'])
                    p_yoy = ((p_val - p_base) / p_base) * 100
                    prev_fmt = f'{p_yoy:.{decimals}f}{unit}'
                else:
                    prev_fmt = 'N/A'

            elif key == 'nfp':
                # NFP: Valore è la variazione (+200k), Previous è la variazione del mese scorso
                # PAYEMS è in migliaia, quindi diff 150 = 150K.
                change = val - prev_val if prev_val else 0
                val_fmt = f'{change:+.0f}K'
                
                # Previous NFP (variazione del mese precedente: obs[1] - obs[2])
                if len(obs) >= 3 and obs[1]['value'] != '.' and obs[2]['value'] != '.':
                    p_val = float(obs[1]['value'])
                    p_prev = float(obs[2]['value'])
                    p_change = p_val - p_prev
                    prev_fmt = f'{p_change:+.0f}K'
                else:
                    prev_fmt = 'N/A'
            else:
                # Unemployment, Fed Funds, GDP (già in %)
                val_fmt = f'{val:.{decimals}f}{unit}'
                prev_fmt = f'{prev_val:.{decimals}f}{unit}' if prev_val else 'N/A'

            result[key] = {
                'label':        label,
                'value':        val_fmt,
                'previous':     prev_fmt,
                'release_date': date_str,
                'status':       'released',
                'next_release': NEXT_RELEASE.get(key, 'N/A'),
            }
            logger.info(f'✅ {label}: {val_fmt} (prev: {prev_fmt})')

        except Exception as e:
            logger.error(f'✗ FRED {key} ({series_id}): {e}')

    return result

def _format_market_value(val):
    """Tronca decimali a 2 cifre per il risparmio nel JSON."""
    if val == 'N/A' or not val: return val
    val_str = str(val).replace(',', '')
    if '.' in val_str:
        try:
            import re
            return re.sub(r'(\d+)\.(\d{2})\d+', r'\1.\2', val_str)
        except: return val
    return val

def fetch_eurostat_indicator(indicator_url: str):
    """
    Helper per recuperare dati da Eurostat API.
    Restituisce (valore, precedente, data_str)
    """
    try:
        r = requests.get(indicator_url, timeout=15)
        r.raise_for_status()
        data = r.json()
        values = data.get('value', {})
        indices = sorted([int(k) for k in values.keys()])
        
        if not indices:
            return None, None, None
            
        latest_idx = indices[-1]
        prev_idx = indices[-2] if len(indices) > 1 else None
        
        val = float(values[str(latest_idx)])
        prev = float(values[str(prev_idx)]) if prev_idx is not None else None
        
        time_cat = data['dimension']['time']['category']
        time_labels = {idx: label for label, idx in time_cat['index'].items()}
        date_str = time_labels.get(latest_idx, "N/A")
        
        return val, prev, date_str
    except Exception as e:
        logger.error(f'✗ Eurostat Error ({indicator_url.split("/")[-1].split("?")[0]}): {e}')
        return None, None, None

def get_macro_calendar_eu() -> dict:
    """
    Scarica dati macro Eurozona via Eurostat API + FRED.
    """
    FRED_API_KEY = os.environ.get('FRED_API_KEY', '')
    
    NEXT_RELEASE_EU = {
        'ecb_rate':        '2026-04-17',  # Prossima riunione BCE
        'cpi_eu':          '2026-04-02',  # Flash CPI Eurozona
        'gdp_eu':          '2026-04-30',  # PIL Eurozona 1° stima
        'unemployment_eu': '2026-04-01',  # Disoccupazione Eurozona
    }

    result = {}
    cutoff_short = datetime.now(timezone.utc) - timedelta(days=60)  # Per CPI/Unemp
    cutoff_long = datetime.now(timezone.utc) - timedelta(days=150) # Per GDP (trimestrale)

    # 1. Indicatore PIL (Eurostat)
    gdp_url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/namq_10_gdp?geo=EA&unit=CLV_PCH_PRE&na_item=B1GQ&s_adj=SCA&lastTimePeriod=2"
    val, prev, date_str = fetch_eurostat_indicator(gdp_url)
    if val is not None:
        is_recent = False
        try:
            if '-Q' in date_str:
                year, q = date_str.split('-Q')
                month = int(q) * 3
                dt = datetime(int(year), month, 28, tzinfo=timezone.utc)
                is_recent = dt >= cutoff_long
        except: pass

        if is_recent:
            result['gdp_eu'] = {
                'label':        'PIL QoQ Eurozona',
                'value':        f'{val:.1f}%',
                'previous':     f'{prev:.1f}%' if prev is not None else 'N/A',
                'release_date': date_str,
                'status':       'released',
                'next_release': NEXT_RELEASE_EU.get('gdp_eu', 'N/A'),
                'region':       'EU',
            }
            logger.info(f'✅ EU PIL QoQ: {val:.1f}% ({date_str})')
        else:
            logger.warning(f'⚠️ EU PIL QoQ: Dato datato ({date_str})')

    # 2. Indicatore Inflazione CPI (Eurostat)
    cpi_url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/prc_hicp_manr?geo=EA20&coicop=CP00&lastTimePeriod=2"
    val, prev, date_str = fetch_eurostat_indicator(cpi_url)
    if val is not None:
        is_recent = False
        try:
            dt = datetime.strptime(date_str, '%Y-%m').replace(tzinfo=timezone.utc)
            is_recent = dt >= cutoff_short
        except: pass

        if is_recent:
            result['cpi_eu'] = {
                'label':        'CPI YoY Eurozona',
                'value':        f'{val:.1f}%',
                'previous':     f'{prev:.1f}%' if prev is not None else 'N/A',
                'release_date': date_str,
                'status':       'released',
                'next_release': NEXT_RELEASE_EU.get('cpi_eu', 'N/A'),
                'region':       'EU',
            }
            logger.info(f'✅ EU CPI YoY: {val:.1f}% ({date_str})')

    # 3. Indicatore Disoccupazione (Eurostat)
    unemp_url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/une_rt_m?geo=EA20&unit=PC_ACT&s_adj=SA&age=TOTAL&sex=T&lastTimePeriod=2"
    val, prev, date_str = fetch_eurostat_indicator(unemp_url)
    if val is not None:
        is_recent = False
        try:
            dt = datetime.strptime(date_str, '%Y-%m').replace(tzinfo=timezone.utc)
            is_recent = dt >= cutoff_short
        except: pass

        if is_recent:
            result['unemployment_eu'] = {
                'label':        'Disoccupazione EU',
                'value':        f'{val:.1f}%',
                'previous':     f'{prev:.1f}%' if prev is not None else 'N/A',
                'release_date': date_str,
                'status':       'released',
                'next_release': NEXT_RELEASE_EU.get('unemployment_eu', 'N/A'),
                'region':       'EU',
            }
            logger.info(f'✅ EU Disoccupazione: {val:.1f}% ({date_str})')

    # 4. Tasso BCE (FRED)
    if FRED_API_KEY:
        try:
            params = {'series_id': 'ECBDFR', 'api_key': FRED_API_KEY, 'file_type': 'json', 'sort_order': 'desc', 'limit': 2}
            r = requests.get('https://api.stlouisfed.org/fred/series/observations', params=params, timeout=15)
            r.raise_for_status()
            obs = r.json().get('observations', [])
            if obs and obs[0]['value'] != '.':
                val_bce = float(obs[0]['value'])
                date_bce = obs[0]['date']
                result['ecb_rate'] = {
                    'label': 'Tasso BCE',
                    'value': f'{val_bce:.2f}%',
                    'release_date': date_bce,
                    'status': 'released',
                    'next_release': NEXT_RELEASE_EU.get('ecb_rate', 'N/A'),
                    'region': 'EU',
                }
                logger.info(f'✅ EU Tasso BCE: {val_bce:.2f}% ({date_bce})')
        except Exception as e:
            logger.error(f'✗ FRED ECB Rate: {e}')

    # Gestione fallback per indicatori mancanti (status upcoming)
    for key in ['gdp_eu', 'cpi_eu', 'unemployment_eu', 'ecb_rate']:
        if key not in result:
            label_map = {
                'gdp_eu': 'PIL QoQ Eurozona',
                'cpi_eu': 'CPI YoY Eurozona',
                'unemployment_eu': 'Disoccupazione EU',
                'ecb_rate': 'Tasso BCE'
            }
            result[key] = {
                'label': label_map.get(key, key),
                'value': None,
                'status': 'upcoming',
                'next_release': NEXT_RELEASE_EU.get(key, 'N/A'),
                'region': 'EU',
            }

    return result

def run():
    logger.info('📈 Recupero dati di mercato...')
    results = {}

    val, chg = get_yahoo_finance('DX-Y.NYB')
    results['dxy'] = {'value': _format_market_value(val), 'change': chg}
    logger.info(f'DXY: {val}')

    val, chg = get_yahoo_finance('EURUSD=X')
    results['eur_usd'] = {'value': _format_market_value(val), 'change': chg}
    logger.info(f'EUR/USD: {val}')

    val, chg = get_yahoo_finance('^GSPC')
    results['sp500'] = {'value': _format_market_value(val), 'change': chg}
    logger.info(f'S&P 500: {val}')

    val, chg = get_yahoo_finance('^VIX')
    results['vix'] = {'value': _format_market_value(val), 'change': chg}
    logger.info(f'VIX: {val}')

    val, chg = get_yahoo_finance('TLT')
    results['tlt'] = {'value': _format_market_value(val), 'change': chg}
    logger.info(f'TLT: {val}')

    val, chg = get_yahoo_finance('^TNX')
    results['us_10y'] = {'value': f'{_format_market_value(val)}%' if val != 'N/A' else 'N/A', 'change': chg}
    logger.info(f'US 10Y: {val}')

    val, chg = get_yahoo_finance('GC=F')
    results['gold'] = {'value': f'${_format_market_value(val)}/oz' if val != 'N/A' else 'N/A', 'change': chg}
    logger.info(f'Gold: {val}')

    val, chg = get_yahoo_finance('BTC-USD')
    results['btcusd'] = {'value': f'${_format_market_value(val)}' if val != 'N/A' else 'N/A', 'change': chg}
    logger.info(f'BTC: {val}')

    val, chg = get_yahoo_finance('BZ=F')
    results['oil_brent'] = {'value': f'${_format_market_value(val)}' if val != 'N/A' else 'N/A', 'change': chg}
    logger.info(f'Brent: {val}')

    val, chg = get_yahoo_finance('EXSA.DE')
    results['stoxx_600'] = {'value': _format_market_value(val), 'change': chg}
    logger.info(f'STOXX 600: {val}')

    val, chg = get_yahoo_finance('^N225')
    results['nikkei'] = {'value': _format_market_value(val), 'change': chg}
    logger.info(f'Nikkei: {val}')

    val, chg = get_yahoo_finance('000001.SS')
    results['shanghai'] = {'value': _format_market_value(val), 'change': chg}
    logger.info(f'Shanghai: {val}')

    val, chg = get_yahoo_finance('^HSI')
    results['hang_seng'] = {'value': _format_market_value(val), 'change': chg}
    logger.info(f'Hang Seng: {val}')

    val, chg = get_stooq('10YITY.B')
    results['btp_10y'] = {'value': f'{_format_market_value(val)}%' if val != 'N/A' else 'N/A', 'change': chg}
    logger.info(f'BTP 10Y: {val}')

    val, chg = get_global_m2_proxy()
    results['global_m2'] = {'value': _format_market_value(val), 'change': chg}
    logger.info(f'Global M2: {val} ({chg})')

    val, chg = get_etf_flow()
    results['btc_etf_flow'] = {'value': val, 'change': chg}
    logger.info(f'BTC ETF Flow: {val}')

    # Crypto data
    logger.info('₿ Fetching crypto data...')
    results['crypto'] = get_crypto_data()
    logger.info(f'Crypto: BTC {results["crypto"]["prices"].get("BTC", {}).get("value", "N/A")}, '
                f'F&G: {results["crypto"]["fear_greed"]["value"]}')

    # Macro calendar
    logger.info('📅 Fetching macro calendar...')
    results['macro_calendar'] = get_macro_calendar()
    results['macro_calendar_eu'] = get_macro_calendar_eu()
    logger.info(f'🇪🇺 Macro EU: {len(results["macro_calendar_eu"])} indicatori')

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    logger.info(f'✅ Dati mercato salvati: {OUTPUT_PATH}')
    return results

if __name__ == '__main__':
    run()
