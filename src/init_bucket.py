from utils.utils import get_s3_client, get_bucket_name, upload_file_into_s3_bucket
import time
import requests
import botocore

TAXI_TYPES = ['yellow', 'green', 'fhv', 'fhvhv']
YEARS = range(2025, 2026) # from 2025 to 2026
MONTHS = [f"{m:02d}" for m in range(1, 13)]
BASE_URL = f'https://d37ci6vzurychx.cloudfront.net/trip-data/'
TIMEOUT_SECONDS = 300

FOLDERS = ['bronze', 'silver', 'gold']

def create_s3_folder(s3_client, bucket_name: str, folder_name: str) -> None:
    
    """ Create a logical folder in an S3 bucket. """
    
    s3_client.put_object(
        Bucket=bucket_name, 
        Key=f'{folder_name}/', 
        Body=''
    )

def build_filename(taxi_type: str, year: int, month: str) -> str:
    """
    """
    return f"{taxi_type}_tripdata_{year}-{month}.parquet"

def generate_urls() -> list[str]:
    return [
        f"{BASE_URL}{build_filename(taxi_type, year, month)}"
        for year in YEARS
        for month in MONTHS
        for taxi_type in TAXI_TYPES
    ]


def search_s3_filename(folder: str, filename: str, bucket_name=get_bucket_name()) -> bool:
    """
    Checks if a specific file exists in an S3 bucket.
    """
    s3 = get_s3_client()
    key = f'{folder}/{filename}'
    
    try:
        s3.head_object(Bucket=bucket_name, Key=key)
        print(f"{key} is already available in the S3 bucket.")
        return True
    
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"{key} is NOT available in the S3 bucket.")
            return False
        else:
            raise


def ingest_raw_data(list_urls: list[str], timeout_seconds: int, s3_client, bucket_name) -> None:
    """
    Download files via HTTP and upload them to the S3 bronze layer.
    """

    for url in list_urls:
        filename = url.replace(BASE_URL, '')
        if search_s3_filename(folder='bronze',filename=filename) == False:

            with requests.get(url, stream=True, timeout=timeout_seconds) as response:
                if response.status_code == 404:
                        print(f"Skipping {filename}: data not available yet.")
                        continue

                response.raise_for_status()
                response.raw.decode_content = True

                upload_file_into_s3_bucket(s3_client, bucket_name, response.raw, 'bronze', filename)
                time.sleep(45)


def main():
    s3_client = get_s3_client()
    bucket_name = get_bucket_name()

    for folder in FOLDERS:
        create_s3_folder(s3_client, bucket_name, folder)

    list_urls = generate_urls()
    s3_client = get_s3_client()
    bucket_name = get_bucket_name()
    timeout = TIMEOUT_SECONDS
     
    ingest_raw_data(
        list_urls=list_urls,
        s3_client=s3_client,
        bucket_name=bucket_name,
        timeout_seconds=timeout
    )


if __name__ == "__main__":
    main()