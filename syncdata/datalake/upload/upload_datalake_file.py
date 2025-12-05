from minio import Minio
from minio.error import S3Error
from config.config_datalake import create_conection
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

PROJECT_PATH = Path('__file__').absolute().parent
logger.info(PROJECT_PATH)

def upload_file(client, file):
    # Arquivo a ser enviado
    source_file = f"{PROJECT_PATH}/syncdata/data/{file}"
    # Destino de envio no bucket
    bucket_name = "syncdata"
    destination_file = f"{file}"
    
    # Criar o bucket caso não exista
    found = client.bucket_exists(bucket_name=bucket_name)
    if not found:
        client.make_bucket(bucket_name=bucket_name)
        logger.info(f"Bucket criado '{bucket_name}'")
    else:
        logger.info(f"Bucket '{bucket_name}' já existe")

    # Enviar o arquivo, renomeando-o no processo
    client.fput_object(
        bucket_name=bucket_name,
        object_name=destination_file,
        file_path=source_file,
    )
    logger.info(f"O dataset {source_file} enviado com sucesso como objeto para o bucket {bucket_name}")
    

if __name__ == "__main__":
    try:
        minio_client = create_conection()
        upload_file(minio_client, "cms_inpatient.csv")
    except S3Error as exc:
        logger.error(f"Erro: {exc}")