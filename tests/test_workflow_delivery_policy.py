from pathlib import Path

import yaml


WORKFLOW = Path(__file__).resolve().parents[1] / '.github/workflows/briefing.yml'


def test_telegram_delivery_does_not_gate_a_published_briefing():
    workflow = yaml.safe_load(WORKFLOW.read_text())
    steps = workflow['jobs']['notify']['steps']
    delivery = next(step for step in steps if step.get('id') == 'telegram_delivery')
    report = next(
        step for step in steps
        if step.get('name') == 'Report pending Telegram retry'
    )

    assert delivery['continue-on-error'] is True
    assert delivery['run'] == 'python src/telegram_bot.py --send --published'
    assert report['if'] == "steps.telegram_delivery.outcome == 'failure'"
    assert workflow['jobs']['briefing']['if'] == "needs.check.outputs.already_run != 'true'"
    assert workflow['jobs']['deploy']['if'].startswith("needs.check.outputs.content_ready != 'true'")
    assert 'always()' in workflow['jobs']['notify']['if']
