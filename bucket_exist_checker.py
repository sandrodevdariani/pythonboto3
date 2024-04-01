import argparse
from botocore.exceptions import ClientError
from auth import init_client
from bucket.crud import bucket_exists, create_bucket

def main():
    parser = argparse.ArgumentParser(description="Check if an S3 bucket exists. If not, create it.")
    parser.add_argument("bucket_name", type=str, help="Name of the S3 bucket to check or create.")
    args = parser.parse_args()

    s3_client = init_client()

    try:
        if bucket_exists(s3_client, args.bucket_name):
            print(f"The bucket '{args.bucket_name}' already exists.")
        else:
            if create_bucket(s3_client, args.bucket_name):
                print(f"Bucket '{args.bucket_name}' successfully created.")
            else:
                print(f"Failed to create bucket '{args.bucket_name}'.")
    except ClientError as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
