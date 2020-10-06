import yaml
import logging
from pathlib import Path
import os


def get_project_root():
    """Returns project root folder."""
    return Path(__file__).parent.parent


def open_yaml(path):
    with open(str(path)) as stream:
        try:
            yaml_dict = yaml.safe_load(stream)
        except yaml.YAMLError:
            logging.error('Error opening YAML file.')

    return yaml_dict


def define_path_file(file_name, path_to_find=None):
    if path_to_find is not None:
        root_path = path_to_find
    else:
        root_path = get_project_root()
    for root, dirs, files in os.walk(root_path):
        for file in files:
            if file == file_name:
                return Path(root) / str(file)


def load_config(env='dev', path_to_file=None, yaml_fie='dbconfig.yml'):
    if path_to_file is not None:
        return open_yaml(define_path_file(yaml_fie, path_to_find=path_to_file))[env]
    else:
        return open_yaml(define_path_file(yaml_fie))[env]
