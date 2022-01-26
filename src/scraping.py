import httpx
from httpx._models import Response
from bs4 import BeautifulSoup, SoupStrainer
import hydra
from hydra.utils import get_original_cwd
from omegaconf import DictConfig
import json
from pathlib import Path


class ScrapingException:
    pass


class Scraping:
    
    def __init__(self, cfg: DictConfig) -> None:
        self.cfg = cfg
        self.target_url = self.cfg.DATA['NYC_DATA_URL']
            
    def _get_content(self) -> Response:
        response = httpx.get(self.target_url)
        return response
    
    def _extract_content(self, response: Response):
        data_links = dict()
        for taxi in self.cfg.DATA['TAXI_TYPES']:
            url_links = []
            for link in BeautifulSoup(response, parse_only=SoupStrainer('a')):
                if link.has_attr('href') and link['href'].startswith("https"):
                    if taxi in link['href']:
                        url_links.append(link['href'])
            data_links[taxi]=url_links
        return data_links
    
    def _write_file(self, data_download_links):
        with open(Path(get_original_cwd()).joinpath('data/download_links.json'), 'w') as fp:
            json.dump(data_download_links, fp)
    
    def get_download_links(self):
        response = self._get_content()
        
        if response.status_code == 200:
            data_download_links = self._extract_content(response=response)
            self._write_file(data_download_links=data_download_links)
            
        else:
            raise ScrapingException(f"Could not scrape because of: {response.status_code}")


@hydra.main(config_path=Path.cwd().joinpath("config"), config_name="config.yaml")
def run(cfg: DictConfig):
    sp = Scraping(cfg=cfg)
    sp.get_download_links()

run()