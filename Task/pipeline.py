from scraping import Scraping
from data_download import Download
from pathlib import Path
import pandas as pd
from datetime import timedelta
import prefect
from prefect import task, Flow, Parameter
import toml

@task
def scrape_urls(conf):
    logger = prefect.context.get("logger")
    logger.info(f"Scraping links for downloading data...")
    sp = Scraping(cfg=conf)
    download_links_json = sp.get_download_links()
    return download_links_json

@task(max_retries=3, retry_delay=timedelta(seconds=10))
def download_data(conf, links_json):
    logger = prefect.context.get("logger")
    logger.info(f"Downloading data!!!")
    
    data_files_list = []
    dwld = Download(file_loc=Path.cwd().joinpath('data'))
    for taxi_type in conf["DATA"]["TAXI_TYPES"]:
        for url in links_json[taxi_type]:
            logger.info(f"Downloading data from {url}")
            dwld.download_files(url=url, file_name=url.split('/')[-1])
    data_files_list.append(url.split('/')[-1])
    return data_files_list

@task
def convert_csv_to_parquet(file_list):
    for file in file_list:
        df = pd.read_csv(Path.cwd().joinpath(f'data/{file}'))
        df.to_parquet(Path.cwd().joinpath(f'data/{file.replace(".csv", ".parquet")}'))


config = toml.load(Path.cwd().joinpath("config/config.toml"))

with Flow("NYCTaxi-Trips-ETL") as flow:
  conf = Parameter(name="conf", default=config)
  links_json = scrape_urls(conf)
  data_list = download_data(conf, links_json)
  convert_csv_to_parquet(data_list)
  
flow.run()
#run()