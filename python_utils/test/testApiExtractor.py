import json
import os
import shutil
from unittest import TestCase

from python.services.api.apiExtractor import ApiExtractor, DIRECTORY_KEY


class TestApiExtractor(TestCase):

    def setUp(self):
        nasaMarsWeatherApiUrl = "https://api.nasa.gov/insight_weather/?api_key=DEMO_KEY&feedtype=json&ver=1.0"
        os.environ[DIRECTORY_KEY] = "test_data"
        self.apiExtraxtor = ApiExtractor()
        self.data = self.apiExtraxtor.get_json_from_api(nasaMarsWeatherApiUrl)
        self.file_route = self.apiExtraxtor.write_json_file(self.data)

    def test_data_wrote_correctly(self):
        with open(f"{self.file_route}") as f:
            data_loaded = json.load(f)
        self.assertEqual(self.data, data_loaded)
        # After that remove all data generated
        shutil.rmtree(self.file_route.split("/")[0])
