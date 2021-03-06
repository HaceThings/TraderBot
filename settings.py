import json
import os


class Settings:
    _config_location = 'config.json'

    def __init__(self):
        if os.path.exists(self._config_location):
            self._dict_ = json.load(open(self._config_location))
        else:
            self.__dict__ = {
                "filename": "demo.db",
                "table_names": [],
                "symbol_list": [],
                "api_key": "demo"
            }
    def __enter__(self):
        return self

    def set(self, key, value):
        self.__dict__[key] = value
        json.dump(self.__dict__, open(self._config_location, 'w'))

    def __exit__(self, exc_type, exc_value, traceback):
        json.dump(self.__dict__, open(self._config_location, 'w'))
