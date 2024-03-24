from google.cloud import logging

client = logging.Client()
logger = client.logger('ETL-Desc')