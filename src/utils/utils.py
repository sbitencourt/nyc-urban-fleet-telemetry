import boto3

def get_bucket_name():
    """
    This function returns the bucket name.
    """
    return "nyc-tlc-trips-data-lake--use2-az1--x-s3"

def get_s3_client():
    """
    This function returns the S3 Client.
    """    
    region_code = "us-east-2"
    return boto3.client('s3', region_name=region_code)

    
def upload_file_into_s3_bucket(s3_client, bucket_name: str, file_obj, folder_name: str, filename: str) -> None:
    """
    Upload a file-like object into an S3 bucket.
    """
    s3_client.upload_fileobj(
        file_obj,
        bucket_name,
        f"{folder_name}/{filename}"
    )