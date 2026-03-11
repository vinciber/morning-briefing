import requests
import os
from dotenv import load_dotenv

load_dotenv()
token = os.environ.get('TELEGRAM_BOT_TOKEN')
url = f'https://api.telegram.org/bot{token}/getMe'
resp = requests.get(url)
print("Bot Info:", resp.json())

url_updates = f'https://api.telegram.org/bot{token}/getUpdates'
resp_updates = requests.get(url_updates)
print("Updates:", resp_updates.json())
