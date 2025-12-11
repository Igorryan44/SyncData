
import os
import sys
import pandas as pd
from pysus import SINAN
from pathlib import Path
from minio import Minio
from minio.error import S3Error
import logging
from io import BytesIO
from dotenv import load_dotenv

load_dotenv()
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

def create_conection():
    # Criar um cliente MinIO com o endpoint, access key e secret key
    client = Minio(
        "localhost:9000",
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        secure=False
    )
    return client

current_file = Path(__file__).resolve()
PROJECT_PATH = current_file.parent.parent.parent

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("extrator")


logger.info("Iniciando extração dos dados do SINAN - Acidente de Trabalho (ACGR)")

# Configuração e extração dos dados do SINAN - Acidente de Trabalho (ACGR)
sinan = SINAN().load()
years = list(range(2000, 2025))

# Extração dos dados de Acidente de Trabalho de 2000 a 2025
datasets = sinan.get_files(dis_code="ACGR", year=years)

logger.info(f"Total de arquivos a serem convertidos: {len(datasets)}")

# Concatenação dos dataframes
try:

    dfs = []

    for file in datasets:
        logger.info(f"Processando dataset: {file.name}")
        df = pd.read_parquet(sinan.download(file).path)
        dfs.append(df)

    df_final = pd.concat(dfs)
    df_buffer = BytesIO()
    df_buffer.seek(0)


    logger.info("Extração e concatenação concluídas com sucesso.")

except Exception as e:
    logger.warning(f"Processamento falhou com o erro: {e}")
# logger.info(f"Arquivo final salvo em: {data_path.parent / 'data' / 'ACGRBR.parquet'}")

def upload_file(client, df_parquet):
    # Destino de envio no bucket
    logger.info("Iniciando processamento de upload dos dados")

    bucket_name = "syncdata"
    
    logger.info("Tentando criar o bucket caso não exista")
    found = client.bucket_exists(bucket_name=bucket_name)
    if not found:
        client.make_bucket(bucket_name=bucket_name)
        logger.info(f"Bucket criado '{bucket_name}'")
    else:
        logger.info(f"Bucket '{bucket_name}' já existe")

    client.put_object(
    bucket_name="syncdata",
    object_name="ACGRBR.parquet",
    data=df_parquet,
    length=df_parquet.getbuffer().nbytes,
    content_type="application/octet-stream"
    
)
    

if __name__ == "__main__":
    try:
        logger.info("Estabelecendo conexão...")
        upload_file(create_conection(), df_buffer)
        logger.info("Processamento concluído")
    except S3Error as exc:
        logger.error(f"Erro: {exc}")