import uuid
import time
from elasticsearch import Elasticsearch, RequestsHttpConnection, exceptions
import logging
from datetime import datetime


DEFAULT_CONFIG = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "members": {
                "dynamic": "strict",
                "properties": {
                    "title": {
                        "type": "text"
                    },
                    "submitter": {
                        "type": "text"
                    },
                    "description": {
                        "type": "text"
                    },
                    "calories": {
                        "type": "integer"
                    }
                }
            }
        }
    }


class ElasticPython(object):
    def __init__(self, host, port, user, password, index):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.index_name = index
        self.doc = None
        self.params_index_path = DEFAULT_CONFIG
        self.client = Elasticsearch(hosts=self.host, connection_class=RequestsHttpConnection,
                                    http_auth=(self.user, self.password))

    def wait_for_rest_layer(self, max_attempts=20):
        for attempt in range(max_attempts):
            try:
                self.client.info()
                return True
            except exceptions.ConnectionError as e:
                if "SSL: UNKNOWN_PROTOCOL" in str(e):
                    raise Exception("Could not connect to cluster via https. Is this an https endpoint?. %s" % str(e))
                else:
                    time.sleep(1)
            except exceptions.TransportError as e:
                if e.status_code == 503:
                    time.sleep(1)
                elif e.status_code == 401:
                    time.sleep(1)
                else:
                    raise e
        return False

    def create_index(self):
        created = False
        try:
            if self.wait_for_rest_layer():
                if not self.client.indices.exists(self.index_name):
                    # Ignore 400 means to ignore "Index Already Exist" error.
                    self.client.indices.create(index=self.index_name, ignore=400, body=self.params_index_path)
                    logging.info('Created Index')
                created = True
            else:
                raise ConnectionError("Problems to connect to the cluster")
        except Exception as ex:
            logging.info("Error something happen Reason %s" % str(ex))
        finally:
            return created

    def prepare_data(self, message, log_level, info_resp=None, error_resp=None):
        item = {
            "time": datetime.utcnow(),
            "type": log_level,
            "comment": message,
            "error": error_resp,
            "info": info_resp,
        }
        return item

    def load_json_data(self, doc_type, message):
        try:
            if self.client.indices.exists(self.index_name):
                # use a `yield` generator so that the data
                # isn't loaded into memory
                self.client.index(index=self.index_name, doc_type=doc_type, id=uuid.uuid4(), body=message)
                logging.info("Data inserted in elastichsearch")
            else:
                if self.create_index():
                    self.client.index(index=self.index_name, doc_type=doc_type, id=uuid.uuid4(), body=message)
                    logging.info("Data inserted in elastichsearch")
        except Exception as e:
            logging.info("Cannot insert data, reason %s" % str(e))