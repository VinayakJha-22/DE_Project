import tarfile
import os

def create_tar_gz(source_dir, output_filename):
    with tarfile.open(output_filename, "w:gz") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))

# Example usage
source_directory = "D:/de_project/DE_Project/ETL Scripts"
output_filename = "etl_batch.tar.gz"
create_tar_gz(source_directory, output_filename)
