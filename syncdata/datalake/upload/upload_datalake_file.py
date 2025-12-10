
import sys
from minio import Minio
from minio.error import S3Error
from pathlib import Path
import logging

current_file = Path(__file__).resolve()
PROJECT_PATH = current_file.parent.parent.parent
sys.path.append(str(PROJECT_PATH))

from datalake.config.config_datalake import create_conection

logger = logging.getLogger()
logger.info(PROJECT_PATH)
logging.basicConfig(level=logging.INFO)

def upload_file(client, file):
    # Arquivo a ser enviado
    source_file = f"{PROJECT_PATH}/data/{file}"
    # Destino de envio no bucket
    bucket_name = "syncdata"
    destination_file = f"{file}"
    
    logger.info("Tentando criar o bucket caso não exista")
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
        logger.info("Estabelecendo conexão...")
        minio_client = create_conection()
        upload_file(minio_client, "ACGRBR2000-2025.parquet")
    except S3Error as exc:
        logger.error(f"Erro: {exc}")