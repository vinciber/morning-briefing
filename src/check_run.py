import json, os
from pathlib import Path
from delivery_state import ready, today


def check(root):
    date = today()
    content_ready = ready(root, date)
    try:
        state = json.loads((Path(root) / f'data/delivery-state/{date}.json').read_text())
    except (OSError, ValueError):
        state = {}
    return {'already_run': str(content_ready and state.get('complete') is True).lower(),
            'content_ready': str(content_ready).lower()}


if __name__ == '__main__':
    values = check(Path(__file__).resolve().parent.parent)
    output = os.environ.get('GITHUB_OUTPUT')
    text = ''.join(f'{key}={value}\n' for key, value in values.items())
    if output:
        with open(output, 'a') as handle: handle.write(text)
    else: print(text)
