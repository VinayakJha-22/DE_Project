from google.cloud import bigquery 
import pandas_gbq
import pandas as pd
import json

try:
    bq_client = bigquery.Client.from_service_account_json('cred/cred.json')
except:
    print("Not able to create client object")

class Read_avro_data:
    def read_data_from_external_table():

        def parse_json(byte_str):
            json_str = byte_str.decode('utf-8')  # Decode byte string to regular string
            return json.loads(json_str) 

        Query = 'select * from `practice.external_stream_table`'
        avro_df = bq_client.query(Query).to_dataframe()
        avro_df['data'] = avro_df['data'].apply(parse_json) 
        batch_data_df = pd.json_normalize(avro_df['data'])
        return batch_data_df
    
    

data = Read_avro_data.read_data_from_external_table()
print(data)