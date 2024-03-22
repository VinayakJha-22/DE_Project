from core.batch_load import Read_avro_data
from core.gcs_archive import Archive_storage
import configparser

if __name__ == "__main__":
    # load config data 
    try:
        config = configparser.ConfigParser()
        config.read('cred/config.ini')
        table_id = config.get('GCP', 'BATCH_TABLE')
        dataset_id = config.get('GCP', 'DATASET_ID')
        stream_bucket =  config.get('GCP', 'STREAM_BUCKET')
        archive_bucket =  config.get('GCP', 'ARCHIVE_BUCKET')
    except Exception as e:
        print(f"Info : Not able to load GCP config data : {e}")

    # batch load from avro external table
    batch_Obj = Read_avro_data()
    extracted_avro_data = batch_Obj.read_data_from_external_table()
    print("Info : Avro Data : ", extracted_avro_data)
    batch_data = batch_Obj.filter_batch_data(extracted_avro_data)
    print("Info : Batch Data :", batch_data)
    status = batch_Obj.upload_to_bigquery(batch_data, dataset_id, table_id)
    print(status)

    # archival process of files from bucket 
    gcs_Obj = Archive_storage()
    gcs_Obj.move_to_archive(stream_bucket, archive_bucket)
    print("Info : Data Archival is done")