import orjson
import pathlib
import requests


def data_get(url: str, parameters: dict = {}, headers: dict = {}):
    print(url, parameters)
    response = requests.get(url, headers=headers, params=parameters)
    print(url, parameters, response.status_code, response.headers)
    return orjson.loads(response.text)


def json_load(file_path):
    return orjson.loads(pathlib.Path(file_path).read_bytes())


def json_save(data, file_path, option=None):
    pathlib.Path(file_path).write_bytes(orjson.dumps(data, option=option))
