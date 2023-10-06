from config.main import Config
from modules.scraping import scraping
from importlib import import_module
import os

web = os.getenv('WEB')
if web:
  # Config.init(env, tenant)
  scraping_web = import_module(f'modules.scraping_web.{web}.main')
  print(f'Starting scraping for {web}')
  scraping(scraping_web.run)()
else:
  print('No web arguments, not running')
