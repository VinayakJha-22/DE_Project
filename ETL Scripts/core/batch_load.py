from google.cloud import bigquery 
import pandas_gbq
import pandas as pd
import json
import configparser
from datetime import datetime, timedelta

class Read_avro_data:
    def __init__(self):
        try:
            config = configparser.ConfigParser()
            config.read('./cred/config.ini')
            self.json_key_path = config.get('DEFAULT', 'JSON_KEY')
            self.bq_client = bigquery.Client.from_service_account_json(self.json_key_path)
        except Exception as e:
            print(f"Not able to create client object {e}")
    
    def read_data_from_external_table(self):

        def parse_json(byte_str):
            json_str = byte_str.decode('utf-8')  # Decode byte string to regular string
            return json.loads(json_str) 

        Query = 'select * from `practice.external_stream_table`'
        avro_df = self.bq_client.query(Query).to_dataframe()
        avro_df['data'] = avro_df['data'].apply(parse_json) 
        batch_data_df = pd.json_normalize(avro_df['data'])
        return batch_data_df
    
    def filter_batch_data(self, dataframe):
        current_date = datetime.now()
        batch_format_date = current_date - timedelta(days=1)
        batch_date = batch_format_date.strftime('%Y-%m-%d')
        dataframe['capture_date'] = pd.to_datetime(dataframe['capture_date'])
        batch_data = dataframe[(dataframe['capture_date'] == batch_date)]
        return batch_data
    
    def upload_to_bigquery(self, dataframe, dataset_id, table_id):
        table_ref = self.bq_client.dataset(dataset_id).table(table_id)
        schema = [
            bigquery.SchemaField('page', 'STRING'),
            bigquery.SchemaField('visit_id', 'STRING'),
            bigquery.SchemaField('event_auth', 'BOOLEAN'),
            bigquery.SchemaField('event_buy_now', 'BOOLEAN'),
            bigquery.SchemaField('event_buy', 'BOOLEAN'),
            bigquery.SchemaField('event_buy_three', 'BOOLEAN'),
            bigquery.SchemaField('event_buy_six', 'BOOLEAN'),
            bigquery.SchemaField('event_buy_nine', 'BOOLEAN'),
            bigquery.SchemaField('event_location', 'STRING'),
            bigquery.SchemaField('capture_date', 'DATE')
        ]
        rows_to_insert = dataframe.astype(str).to_dict(orient='records')
        errors = self.bq_client.insert_rows_json(table_ref, rows_to_insert)
        if errors:
            print(f'Encountered errors while inserting rows: {errors}')
            return f"Error Occured {errors}"
        else:
            print('All rows successfully inserted into BigQuery table.')
            return "Batch Data Loaded Successully!"
        
    
