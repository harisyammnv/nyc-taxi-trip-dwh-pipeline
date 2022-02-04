from pathlib import Path
from tqdm import tqdm
import httpx
class DownloadException(Exception):
    pass


class Download:
    
    def __init__(self, file_loc: Path) -> None:
        self.file_loc = file_loc
    
    def download_files(self, url: str, file_name: str):
        try:
            with httpx.stream("GET", url) as res:
                with open(Path(self.file_loc).joinpath(file_name), "wb") as handle:
                    for data in tqdm(res.iter_bytes()):
                        handle.write(data)
        except Exception as e:
            raise DownloadException(f"Could not download file from url: {url} because: {e}")

# if __name__ == "__main__":
#     dwld = Download(file_loc=Path.cwd().joinpath('data'))
#     dwld.download_files(url="https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2017-01.csv", file_name="temp.csv")