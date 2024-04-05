import argparse
import magic
import os
from auth import init_client

def get_mime_based_folder(file_path):

    mime_type = magic.Magic(mime=True).from_file(file_path)

    folder_name = mime_type.split('/')[0]
    return folder_name.lower()

def upload_file(file_path, bucket_name):

    s3_client = init_client()
    folder_name = get_mime_based_folder(file_path)
    file_name = os.path.basename(file_path)
    s3_key = f"{folder_name}/{file_name}"

    try:
        s3_client.upload_file(file_path, bucket_name, s3_key)
        print("file uploaded successfully")
    except Exception as e:
        print(f"error to upload file ")

def main():
    parser = argparse.ArgumentParser
    parser.add_argument("file_path")
    parser.add_argument("bucket_name")
    args = parser.parse_args()

    upload_file(args.file_path, args.bucket_name)

if __name__ == "__main__":
    main()
