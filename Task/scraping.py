import httpx
from httpx._models import Response
from bs4 import BeautifulSoup, SoupStrainer
from omegaconf import DictConfig
import json
from pathlib import Path
import pandas as pd
from datetime import datetime
import re
class ScrapingException(Exception):
    pass


class Scraping:
    
    def __init__(self, cfg) -> None:
        self.cfg = cfg
        self.target_url = self.cfg['DATA']['NYC_DATA_URL']
            
    def _get_content(self) -> Response:
        response = httpx.get(self.target_url)
        return response
    
    def _extract_content(self, response: Response, date_limits: list) -> dict:
        data_links = dict()
        for taxi in self.cfg["DATA"]['TAXI_TYPES']:
            url_links = []
            for link in BeautifulSoup(response, parse_only=SoupStrainer('a')):
                if link.has_attr('href') and link['href'].startswith("https"):
                    if taxi in link['href']:
                        trip_str=re.findall(r"_(.*).csv", link['href'])[-1].strip(".csv").strip("_tripdata_")
                        if trip_str in date_limits:
                            url_links.append(link['href'])
            data_links[taxi]=url_links
        return data_links
    
    def _write_file(self, data_download_links):
        with open(Path.cwd().joinpath(self.cfg['DATA']['URL_JSON']), 'w') as fp:
            json.dump(data_download_links, fp)
    
    def _formulate_link_limits(self):
        try:
            from_date = datetime.fromisoformat(self.cfg["DATA"]['FROM_DATE'])
            to_date = datetime.fromisoformat(self.cfg["DATA"]['TO_DATE'])
            date_limits = pd.date_range(str(from_date),str(to_date), freq='MS').strftime("%Y-%m").tolist()
            
        except ScrapingException as ex:
            print("Date should be in ISO Format: YYYY-MM-DD is only accepted")
        return date_limits

    def get_download_links(self):
        date_limits = self._formulate_link_limits()
        response = self._get_content()
        
        if response.status_code == 200:
            data_download_links = self._extract_content(response=response, date_limits=date_limits)
            self._write_file(data_download_links=data_download_links)
            return data_download_links
        else:
            raise ScrapingException(f"Could not scrape because of: {response.status_code}")