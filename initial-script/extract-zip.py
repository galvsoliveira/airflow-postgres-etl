import os
import zipfile

ZIP_FILE_PATH = "./../data/data.zip"
DESTINATION_PATH = "./../data/extracted"

def extract_zip(zip_file_path, destination_path):
    try:
        with zipfile.ZipFile(zip_file_path) as _rf:
            os.makedirs(destination_path, exist_ok=True)

            _rf.extractall(destination_path)
        print("Extraction completed successfully.")
    except Exception as exception:
        print("An error occurred:", str(exception))

if __name__ == '__main__':
    extract_zip(ZIP_FILE_PATH, DESTINATION_PATH)