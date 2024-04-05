import argparse
from datetime import datetime, timedelta
from auth import init_client

def delete_old_versions(bucket_name, file_keys):

    s3 = init_client()
    six_months_ago = datetime.now() - timedelta(days=6*30)  

    for file_key in file_keys:

        versions = s3.list_object_versions(Bucket=bucket_name, Prefix=file_key)
        for version in versions.get('Versions', []):  
            version_id = version['VersionId']
            last_modified = version['LastModified']

            if last_modified < six_months_ago:
                s3.delete_object(Bucket=bucket_name, Key=file_key, VersionId=version_id)
                print(f"Deleted version {version_id} ")

def main():
    parser = argparse.ArgumentParser
    parser.add_argument("bucket_name")
    parser.add_argument("file_keys")
    args = parser.parse_args()

    delete_old_versions(args.bucket_name, args.file_keys)

if __name__ == "__main__":
    main()
