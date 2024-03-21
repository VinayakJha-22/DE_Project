from core.batch_load import Read_avro_data

if __name__ == "__main__":
    batch_Obj = Read_avro_data()
    extracted_avro_data = batch_Obj.read_data_from_external_table()
    print("Avro Data : ", extracted_avro_data)
    batch_data = batch_Obj.filter_batch_data(extracted_avro_data)
    print("Batch Data :", batch_data)
    table_id = 'batch_data'
    dataset_id = 'practice'
    status = batch_Obj.upload_to_bigquery(batch_data, dataset_id, table_id)
    print(status)