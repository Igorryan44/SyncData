import polars as pl
from pysus import SINAN
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("extrator")

data_path = Path(__name__).parent.absolute().parent / "datasets"

logger.info("Iniciando extração dos dados do SINAN - Acidente de Trabalho (ACGR)")

# Configuração e extração dos dados do SINAN - Acidente de Trabalho (ACGR)
sinan = SINAN().load()
years = [i for i in range(2025) if i >1999]

# Extração dos dados de Acidente de Trabalho de 2000 a 2025
files = sinan.get_files(dis_code="ACGR", year=years)

logger.info(f"Total de arquivos a serem baixados: {len(files)}")
logger.info(f"Iniciando download dos arquivos...")
# Download dos arquivos
sinan.download(files, local_dir="data/")

logger.info("Download concluído. Iniciando leitura e concatenação dos arquivos...")
# Concatenação dos dataframes
dfs = []

for file in data_path.rglob("*.parquet"):
    if file.is_file():
        df = pl.read_parquet(file)
        dfs.append(df)

df_final = pl.concat(dfs, rechunk=True)
df_final.write_parquet(f"{data_path}ACGRBR.parquet")

logger.info("Extração e concatenação concluídas com sucesso.")
logger.info(f"Arquivo final salvo em: {data_path / 'ACGRBR.parquet'}")