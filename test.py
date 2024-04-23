import boto3
import argparse
import os
from botocore.exceptions import NoCredentialsError, ClientError
from collections import Counter
import requests
import json

from os import getenv

def init_client():
    aws_access_key_id = ""
    aws_secret_access_key = ""
    aws_session_token = ""
    aws_region_name = "us-west-2"

    if not (aws_access_key_id and aws_secret_access_key and aws_region_name):
        raise ValueError("AWS credentials and region name are required.")

    try:
        client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region_name=aws_region_name
        )
        # Check if credentials are correct by listing buckets
        client.list_buckets()
        return client
    except Exception as e:
        raise RuntimeError("Failed to initialize AWS S3 client:", e)

# Initialize S3 client
s3 = init_client()


# Function to create a new bucket
def create_bucket(bucket_name, region):
    location = {'LocationConstraint': region}
    response = s3.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration=location
    )
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code == 200:
        return True
    return False


# Function to check if a bucket exists
def check_bucket_existence(bucket_name):
    try:
        s3.head_bucket(Bucket=bucket_name)
        return True
    except ClientError:
        return False


# Function to delete a bucket
def delete_bucket(bucket_name):
    try:
        s3.delete_bucket(Bucket=bucket_name)
        print(f'Bucket {bucket_name} deleted successfully.')
    except ClientError as e:
        print(f'Error deleting bucket {bucket_name}: {e}')


# Function to delete an object from a bucket
def delete_object(bucket_name, object_key):
    try:
        s3.delete_object(Bucket=bucket_name, Key=object_key)
        print(f'Object {object_key} deleted successfully.')
    except ClientError as e:
        print(f'Error deleting object {object_key}: {e}')


# Function to enable versioning on a bucket
def enable_versioning(bucket_name):
    s3.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration={'Status': 'Enabled'}
    )
    print(f'Versioning enabled for bucket {bucket_name}.')


# Function to disable versioning on a bucket
def disable_versioning(bucket_name):
    s3.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration={'Status': 'Suspended'}
    )
    print(f'Versioning disabled for bucket {bucket_name}.')


# Function to promote a specific version by ID
def promote_version(bucket_name, object_key, version_id):
    s3.copy_object(
        Bucket=bucket_name,
        CopySource={
            'Bucket': bucket_name,
            'Key': object_key,
            'VersionId': version_id
        },
        Key=object_key,
        MetadataDirective='COPY'
    )
    print(f'Version {version_id} promoted for object {object_key}.')


def list_object_versions(bucket_name):
    try:
        response = s3.list_object_versions(Bucket=bucket_name)
        for version in response.get('Versions', []):
            print(f"Key: {version['Key']} | Version ID: {version['VersionId']} | Is Latest: {version['IsLatest']}")
    except ClientError as e:
        print(f'Error listing object versions: {e}')


def list_objects(bucket_name):
    try:
        response = s3.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            for obj in response['Contents']:
                print(f"Key: {obj['Key']} | Size: {obj['Size']} bytes")
        else:
            print(f'No objects found in bucket {bucket_name}.')
    except ClientError as e:
        print(f'Error listing objects in bucket: {e}')


def public_read_policy(bucket_name):
    policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "PublicReadGetObject",
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": f"arn:aws:s3:::{bucket_name}/*",
        }],
    }
    return json.dumps(policy)


def multiple_policy(bucket_name):
    policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Action": [
                "s3:PutObject", "s3:PutObjectAcl", "s3:GetObject", "s3:GetObjectAcl",
                "s3:DeleteObject"
            ],
            "Resource": [
                f"arn:aws:s3:::{bucket_name}",
                f"arn:aws:s3:::{bucket_name}/*"
            ],
            "Effect": "Allow",
            "Principal": "*"
        }]
    }
    return json.dumps(policy)


def assign_policy(policy_function, bucket_name):
    policy = None
    if policy_function == "public_read_policy":
        policy = public_read_policy(bucket_name)
    elif policy_function == "multiple_policy":
        policy = multiple_policy(bucket_name)

    if not policy:
        print("Please provide a valid policy function.")
        return

    s3.put_bucket_policy(Bucket=bucket_name, Policy=policy)
    print(f"Bucket policy assigned successfully to {bucket_name}.")


def read_bucket_policy(bucket_name):
    try:
        policy = s3.get_bucket_policy(Bucket=bucket_name)
        return policy["Policy"]
    except ClientError:
        print(f"Error reading bucket policy: {e}")
        return None


def upload_file(bucket_name, file_path):
    file_key = os.path.basename(file_path)
    try:
        with open(file_path, 'rb') as file_obj:
            s3.upload_fileobj(file_obj, bucket_name, file_key)
        print(f'File {file_key} uploaded to bucket {bucket_name}.')
    except NoCredentialsError:
        print('promblem with uploading')


def put_object(bucket_name, object_key, content):
    try:
        # Using put_object to upload raw content (like strings or bytes)
        s3.put_object(Bucket=bucket_name, Key=object_key, Body=content)
        print(f'Object {object_key} created in bucket {bucket_name}.')
    except ClientError as e:
        print('problem with uploading')


def download_link_and_upload(bucket_name, download_url, object_key):
    response = requests.get(download_url)
    if response.status_code == 200:
        s3.put_object(Bucket=bucket_name, Key=object_key, Body=response.content)
        print(f'Content downloaded from {download_url} and uploaded as {object_key}.')
    else:
        print(f'Error downloading content from {download_url}.')

def count_extensions_usage(bucket_name):
    try:
        response = s3.list_objects_v2(Bucket=bucket_name)
        extensions_count = Counter()
        total_size = 0
        
        if 'Contents' in response:
            for obj in response['Contents']:
                extension = os.path.splitext(obj['Key'])[1].lower()
                extensions_count[extension] += 1
                total_size += obj['Size']
        
        for extension, count in extensions_count.items():
            print(f'{extension}: {count}, usage: {total_size/1024/1024:.2f} MB')
    except ClientError as e:
        print(f'Error counting extensions usage in bucket {bucket_name}: {e}')


# Main script execution
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket', required=True, help='Name of the S3 bucket')
    parser.add_argument('--region', help='AWS region for bucket operations')
    parser.add_argument('--create-bucket', action='store_true', help='Create a new bucket')
    parser.add_argument('--check-bucket', action='store_true', help='Check if the bucket exists')
    parser.add_argument('--delete-bucket', action='store_true', help='Delete the bucket')
    parser.add_argument('--delete-object', help='Delete an object by key')
    parser.add_argument('--enable-versioning', action='store_true', help='Enable versioning for the bucket')
    parser.add_argument('--disable-versioning', action='store_true', help='Disable versioning for the bucket')
    parser.add_argument('--promote-version', help='Promote a specific version by ID')
    parser.add_argument('--list-versions', action='store_true', help='List all object versions in the bucket')
    parser.add_argument('--list-objects', action='store_true', help='List all objects in the bucket')
    parser.add_argument('--upload-file', help='Upload a file to the bucket')
    parser.add_argument('--assign-policy', help='Policy to assign (public_read_policy or multiple_policy)')
    parser.add_argument('--read-policy', action='store_true', help='Read the bucket policy')
    parser.add_argument('--put-object', nargs=2, metavar=('key', 'content'), help='Create an object with specific content')
    parser.add_argument('--download-upload', nargs=2, metavar=('download_url', 'object_key'), help='Download content and upload to bucket')
    parser.add_argument('--count-extensions', action='store_true', help='Count extensions usage in the bucket')

    # Parse command-line arguments
    args = parser.parse_args()

    # Region and bucket name setup
    region = args.region
    bucket_name = args.bucket

    if args.create_bucket:
        if create_bucket(bucket_name, region):
            print(f'Bucket {bucket_name} created.')
        else:
            print(f'Bucket {bucket_name} might already exist or there was an error.')

    if args.check_bucket:
        if check_bucket_existence(bucket_name):
            print(f'Bucket {bucket_name} exists.')
        else:
            print(f'Bucket {bucket_name} does not exist.')

    if args.delete_bucket:
        delete_bucket(bucket_name)

    if args.delete_object:
        delete_object(bucket_name, args.delete-object)

    if args.enable_versioning:
        enable_versioning(bucket_name)

    if args.disable_versioning:
        disable_versioning(bucket_name)

    if args.promote_version:
        promote_version(bucket_name, args.promote_version[0], args.promote_version[1])

    if args.list_versions:
        list_object_versions(bucket_name)

    if args.list_objects:
        list_objects(bucket_name)

    if args.assign_policy:
        assign_policy(s3, args.assign-policy, bucket_name)

    if args.read_policy:
        policy = read_bucket_policy(s3, bucket_name)
        if policy:
            print(f"Bucket policy for {bucket_name}: {policy}")

    if args.put_object:
        object_key, content = args.put_object
        bucket_name = args.bucket
        put_object(bucket_name, object_key, content)

    if args.put_object:
        put_object(bucket_name, args.put_object[0], args.put_object[1])

    if args.download_upload:
        download_link_and_upload(bucket_name, args.download_upload[0], args.download_upload[1])

    if args.count_extensions:
        count_extensions_usage(bucket_name)        

if __name__ == '__main__':
    main()