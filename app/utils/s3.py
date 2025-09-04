import boto3
from botocore.client import Config
from ..config.settings import settings
def s3_client():
    if not settings.S3_ENDPOINT: return None
    return boto3.client("s3", endpoint_url=settings.S3_ENDPOINT,
        aws_access_key_id=settings.S3_ACCESS_KEY, aws_secret_access_key=settings.S3_SECRET_KEY,
        region_name=settings.S3_REGION, config=Config(signature_version="s3v4"))
def presign_put(key:str, content_type:str="application/octet-stream", expires:int=3600):
    cli=s3_client()
    if not cli or not settings.S3_BUCKET: return None
    return cli.generate_presigned_url("put_object",
        Params={"Bucket":settings.S3_BUCKET,"Key":key,"ContentType":content_type}, ExpiresIn=expires)
def object_uri(key:str)->str: return f"s3://{settings.S3_BUCKET}/{key}"
