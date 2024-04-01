import argparse
from botocore.exceptions import ClientError
from auth import init_client
from bucket.crud import bucket_exists, delete_bucket

def main():
    parser = argparse.ArgumentParser(description="Check if an S3 bucket exists. If it exists, delete it.")
    parser.add_argument("bucket_name", type=str, help="Name of the S3 bucket to check or delete.")
    args = parser.parse_args()

    s3_client = init_client()

    try:
        if bucket_exists(s3_client, args.bucket_name):
            if delete_bucket(s3_client, args.bucket_name):
                print(f"The bucket '{args.bucket_name}' has been deleted.")
            else:
                print(f"Failed to delete the bucket '{args.bucket_name}'.")
        else:
            print(f"The bucket '{args.bucket_name}' does not exist.")
    except ClientError as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
