from scraping import Scraping
from data_download import Download
from pathlib import Path
import pandas as pd
from datetime import timedelta
import prefect
from prefect import task, Flow, Parameter, unmapped
from prefect.executors.dask import LocalDaskExecutor
from utils import pg_utils
import toml

FLOW_NAME = "NYCTaxi-Trips-Local-EL"

@task
def scrape_urls(conf: dict):
    logger = prefect.context.get("logger")
    logger.info(f"Scraping links for downloading data...")
    sp = Scraping(cfg=conf)
    download_links_json = sp.get_download_links()
    return download_links_json

@task(max_retries=3, retry_delay=timedelta(seconds=10))
def download_data(conf: dict, links_json: dict):
    logger = prefect.context.get("logger")
    logger.info(f"Downloading data!!!")
    
    data_files_list = []
    dwld = Download(file_loc=Path.cwd().joinpath('data'))
    for taxi_type in conf["DATA"]["TAXI_TYPES"]:
        for url in links_json[taxi_type]:
            logger.info(f"Downloading data from {url}")
            if not Path.cwd().joinpath(f"nyc_raw_data/{url.split('/')[-1]}").exists():
                dwld.download_files(url=url, file_name=url.split('/')[-1])
            data_files_list.append(url.split('/')[-1])
    return data_files_list

@task
def convert_csv_to_parquet(file_list: list):
    for file in file_list:
        df = pd.read_csv(Path.cwd().joinpath(f'nyc_raw_data/{file}'))
        df.to_parquet(Path.cwd().joinpath(f'nyc_raw_data/{file.replace(".csv", ".parquet")}'))

@task
def ingest_raw_data(conf: dict, file_name: str):
    logger = prefect.context.get("logger")
    local_pg = pg_utils.PostgresDataLoader(port=conf["DB"]["PG_PORT"],
                                           db=conf["DB"]["DB_NAME"])
    logger.info(f"Ingesting data for {file_name}")
    df = pd.read_parquet(Path.cwd().joinpath(f'nyc_raw_data/{file_name.replace(".csv", ".parquet")}'))
    df.drop_duplicates
    date_cols = [col for col in df.columns if 'datetime' in col]
    
    for col in date_cols:
        df[col] = pd.to_datetime(df[col])

    if 'yellow' in file_name:
        local_pg.load_df_to_db(df=df, table_name=conf["DB"]["YELLOW_TAXI_TABLE"])
    else:
        local_pg.load_df_to_db(df=df, table_name=conf["DB"]["GREEN_TAXI_TABLE"])


config = toml.load(Path.cwd().joinpath("config/config.toml"))

with Flow(FLOW_NAME, executor=LocalDaskExecutor()) as flow:
    conf = Parameter(name="conf", default=config)
    links_json = scrape_urls(conf)
    data_list = download_data(conf, links_json)
    ingest_raw_data.set_upstream(convert_csv_to_parquet(file_list=data_list))
    ingest_raw_data.map(file_name=data_list, conf=unmapped(conf))

flow.visualize()
flow.run()