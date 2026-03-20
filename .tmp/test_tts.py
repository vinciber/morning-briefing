
import re

def _number_to_italian(n: int) -> str:
    words = {
        1: 'un', 2: 'due', 3: 'tre', 4: 'quattro', 5: 'cinque',
        6: 'sei', 7: 'sette', 8: 'otto', 9: 'nove', 10: 'dieci',
        11: 'undici', 12: 'dodici', 13: 'tredici', 14: 'quattordici',
        15: 'quindici', 16: 'sedici', 17: 'diciassette', 18: 'diciotto',
        19: 'diciannove', 20: 'venti', 30: 'trenta', 40: 'quaranta',
        50: 'cinquanta', 60: 'sessanta', 70: 'settanta', 80: 'ottanta',
        90: 'novanta',
    }
    if n in words:
        return words[n]
    if n < 100:
        tens = (n // 10) * 10
        ones = n % 10
        return words[tens] + words[ones]
    return str(n)

def replace_usd(m):
    num_str = m.group(1).replace(',', '')
    suffix = m.group(2)
    try:
        val = float(num_str)
        
        # Gestione suffissi espliciti (es. $45.1M, $1.2B)
        if suffix:
            s = suffix.lower()
            if s in ('m', 'million', 'milioni'):
                val_str = f"{val:g}".replace('.', ' virgola ')
                return f'{val_str} milioni di dollari'
            if s in ('b', 'billion', 'miliardi'):
                val_str = f"{val:g}".replace('.', ' virgola ')
                return f'{val_str} miliardi di dollari'

        # Gestione numeri estesi (es. $1,000,000)
        if val >= 1_000_000_000:
            miliardi = val / 1_000_000_000
            return f'{miliardi:g} miliardi di dollari'.replace('.', ' virgola ')
        elif val >= 1_000_000:
            milioni = val / 1_000_000
            return f'{milioni:g} milioni di dollari'.replace('.', ' virgola ')
        elif val >= 1_000:
            thousands = int(val // 1000)
            remainder = int(val % 1000)
            thousands_word = _number_to_italian(thousands) + 'mila'
            if remainder:
                return f'{thousands_word} {remainder} dollari'
            else:
                return f'{thousands_word} dollari'
        else:
            int_part = int(val)
            dec_part = round((val - int_part) * 100)
            if dec_part:
                return f'{int_part} dollari e {dec_part} centesimi'
            else:
                return f'{int_part} dollari'
    except Exception as e:
        return str(e)

test_regex = r'\$([0-9,]+(?:\.[0-9]+)?)\s*(million|billion|milioni|miliardi|M|B)?'

tests = [
    "$45.1 million",
    "$45.1M",
    "$1,000,000",
    "$1.5 billion",
    "$100",
    "$45.10",
]

for t in tests:
    res = re.sub(test_regex, replace_usd, t, flags=re.IGNORECASE)
    print(f"{t} -> {res}")
