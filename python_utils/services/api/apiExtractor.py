import json
import os
import time
from urllib.parse import urlparse

import requests

DIRECTORY_KEY = "LANDING_ZONE"

class ApiExtractor:
    def __init__(self):
        self.url = None
        self.data = {}
        self.is_wrote = False

    def get_json_from_api(self, url):
        self.url = url
        self.data = requests \
            .get(url) \
            .json()
        return self.data

    def write_json_file(self, json_dict):

        filename = time.strftime("%Y%m%d-%H%M%S")
        parsed_url = urlparse(self.url)

        if os.environ.get(DIRECTORY_KEY) is not None:
            path = f"{os.environ[DIRECTORY_KEY]}/{parsed_url.netloc}{parsed_url.path}"
        else:
            path = f"data/{parsed_url.netloc}{parsed_url.path}"
        if not os.path.exists(path):
            os.makedirs(path)

        with open(f"{path}/{filename}", 'w') as outfile:
            json.dump(json_dict, outfile)
        self.is_wrote = True

        return f"{path}{filename}"