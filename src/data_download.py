from pathlib import Path
from tqdm import tqdm
import httpx
class DownloadException:
    pass


class Download:
    
    def __init__(self, file_loc: Path) -> None:
        self.file_loc = file_loc
    
    def download_files(self, url: str, file_name: str):
        try:
            response = httpx.get(url, stream=True)

            with open(Path(self.file_loc).joinpath(file_name), "wb") as handle:
                for data in tqdm(response.iter_content()):
                    handle.write(data)
        except Exception as e:
            raise DownloadException(f"Could not download file from url: {url} because: {e}")