from scraping import Scraping
from data_download import Download
from pathlib import Path
import hydra
from omegaconf import DictConfig

@hydra.main(config_path=Path.cwd().joinpath("config"), config_name="config.yaml")
def run(cfg: DictConfig):
    sp = Scraping(cfg=cfg)
    sp.get_download_links()

run()