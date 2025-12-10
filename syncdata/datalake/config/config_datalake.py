# file_uploader.py MinIO Python SDK example
from minio import Minio
from minio.error import S3Error
import os
from dotenv import load_dotenv

load_dotenv()
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

def create_conection():
    # Criar um cliente MinIO com o endpoint, access key e secret key
    client = Minio(
        endpoint="localhost:8000",
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        secure=False
    )
    return client

if __name__ == "__main__":
    try:
        create_conection()
    except S3Error as exc:
        print("Erro: ", exc)