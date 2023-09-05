import json


def config_loader(filepath) -> dict:
    with open(filepath) as file:
        config = json.load(file)
        return config
