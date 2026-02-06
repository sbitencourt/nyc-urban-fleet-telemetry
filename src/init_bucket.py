from utils import get_s3_client, get_bucket_name

FOLDERS = ['bronze', 'silver', 'gold']

def create_s3_folder(s3_client, bucket_name: str, folder_name: str) -> None:
    
    """ Create a logical folder in an S3 bucket. """
    
    s3_client.put_object(
        Bucket=bucket_name, 
        Key=f'{folder_name}/', 
        Body=''
    )


def main():
    s3_client = get_s3_client()
    bucket_name = get_bucket_name()

    for folder in FOLDERS:
        create_s3_folder(s3_client, bucket_name, folder)


if __name__ == "__main__":
    main()