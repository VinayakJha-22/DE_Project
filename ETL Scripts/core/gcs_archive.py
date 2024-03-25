from google.cloud import storage
from datetime import datetime as dt, timedelta as td
import pytz
from . import logger

class Archive_storage:
    def __init__(self):
        try:
            self.gcs_client = storage.Client()
        except Exception as e:
            print(f"Info : Not able to create Client Object : {e}")
            logger.log_text(f"Info : Not able to create Client Object : {e}")

    def move_to_archive(self, standard_bucket_name, archive_bucket_name):
        source_bucket = self.gcs_client.get_bucket(standard_bucket_name)
        destination_bucket = self.gcs_client.get_bucket(archive_bucket_name)
        ist_timezone = pytz.timezone('Asia/Kolkata')
        current_date = dt.now(ist_timezone)
        batch_datetime = current_date - td(days=1)
        batch_date = batch_datetime.date()
        blobs = source_bucket.list_blobs()
        print(f"Info : Looking for File date : {batch_date}")
        logger.log_text(f"Info : Looking for File date : {batch_date}")

        for blob in blobs:
            blob_created_time = blob.time_created
            creation_time_ist = blob_created_time.astimezone(ist_timezone)
            blob_created_date = creation_time_ist.date()
            print(f"Info : {blob} : {blob_created_date}")
            logger.log_text(f"Info : {blob} : {blob_created_date}")
            if blob_created_date == batch_date:
                new_blob = source_bucket.copy_blob(blob, destination_bucket)
                blob.delete()
        print(f"Info : Moved objects to archive")
        logger.log_text(f"Info : Moved objects to archive")
        