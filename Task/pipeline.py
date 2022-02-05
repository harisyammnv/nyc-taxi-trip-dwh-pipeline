from scraping import Scraping
from data_download import Download
from pathlib import Path
import prefect
from prefect import task, Flow, Parameter
import toml

@task
def scrape_urls(conf):
    logger = prefect.context.get("logger")
    logger.info(f"Configuration: {conf}")
    logger.info(f"Type: {type(conf)}")
    sp = Scraping(cfg=conf)
    sp.get_download_links()


config = toml.load(Path.cwd().joinpath("config/config.toml"))

with Flow("NYCTaxi-Trips-ETL") as flow:
  conf = Parameter(name="conf", default=config)
  scrape_urls(conf)
flow.run()
#run()