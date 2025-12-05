import requests
import logging
import polars as pl
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

FILE_PATH = Path('__file__').parent.absolute().parent

# Configurações de log
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Processor")

# Recursos API
url = 'https://data.cms.gov/data.json'
headers = {'accept': 'application/json'}

# Função principal
def main(api_url, api_headers):
    logger.info("Iniciando extração dos dados da API CMS")
    data = requests.get(url=api_url, headers=api_headers).json()

    logger.info("Criando DataFrame inicial")
    root_df = pl.DataFrame(data["dataset"], strict=False, nan_to_null=True)
    logger.info("DataFrame inicial criado com sucesso")
    
    logger.info("Transformando dados")
    root_df = root_df.explode("distribution"
                                ).explode("theme"
                                ).explode("references"
                                ).explode("programCode"
                                ).explode("language"
                                ).explode("keyword"
                                ).explode("bureauCode"
                                ).unnest(columns=("contactPoint","distribution", "publisher"), separator="_"
                                )

    logger.info("Dados transformados com sucesso")
    logger.info("Explorando URLs de distribuição para 'Inpatient'")

    lf_group = root_df.group_by("keyword").agg(pl.col("distribution_accessURL"))


    lf_count_values = lf_group.filter(pl.col("keyword") == "Inpatient")
    lf_count_values = lf_count_values.explode("distribution_accessURL").select("distribution_accessURL")

    logger.info("Preparando requisições para as APIs encontradas")
    lista_de_apis = []

    for api in lf_count_values["distribution_accessURL"]:
        if not api == None:
            lista_de_apis.append(api)
    total_apis = len(lista_de_apis)
    dfs = []
    cont = 1

    logger.info(f"{total_apis} APIs encontradas para requisição")
    logger.info(f"Iniciando requisições...")
    for api in lista_de_apis:
        data = requests.get(url=api, headers=headers)
        logger.info(f"Requisições realizadas: {cont}/{total_apis}")
        df = pl.DataFrame(data.json(), strict=False, nan_to_null=True)
        dfs.append(df)
        cont += 1

    logger.info("Todas as requisições foram realizadas com sucesso")    

    logger.info("Concatenando DataFrames obtidos")
    final_df = pl.concat(dfs, how="diagonal", rechunk=True)
    logger.info("Salvando arquivo cms_inpatient.parquet")
    final_df.write_parquet(f"{FILE_PATH}/SyncData/syncdata/data/cms_inpatient.parquet")

if __name__ == "__main__":
    main(api_url=url, api_headers=headers)