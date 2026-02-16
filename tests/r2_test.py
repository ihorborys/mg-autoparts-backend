import os, boto3
from dotenv import load_dotenv
from botocore.client import Config
from botocore.exceptions import ClientError

load_dotenv()

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("R2_ENDPOINT"),
    aws_access_key_id=os.getenv("R2_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY"),
    config=Config(signature_version="s3v4"),
    region_name="auto",
)

bucket = os.getenv("R2_BUCKET")

# Перевіряємо доступ до КОНКРЕТНОГО бакета
try:
    s3.head_bucket(Bucket=bucket)  # не потребує прав на "всі бакети"
    print("Bucket OK:", bucket)
except ClientError as e:
    print("head_bucket error:", e)

# Тестовий аплоад
with open("hello.txt", "w", encoding="utf-8") as f:
    f.write("hello R2!\n")

key = "test/hello.txt"
s3.upload_file("hello.txt", bucket, key, ExtraArgs={"ContentType": "text/plain"})
print("Uploaded:", key)

# Тимчасовий URL на 1 годину
url = s3.generate_presigned_url(
    "get_object",
    Params={"Bucket": bucket, "Key": key},
    ExpiresIn=3600
)
print("Presigned URL:", url)
