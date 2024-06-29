import yaml


class Config:
    def __init__(self, source):
        self._config = self._load_config(source)

    def _load_config(self, file_path: str):
        with open(file_path, "r") as file:
            return yaml.safe_load(file)

    def get(self, key, default=None):
        keys = key.split(".")
        value = self._config
        try:
            for k in keys:
                value = value[k]
        except KeyError:
            return default
        return value

    def set(self, key, value):
        keys = key.split(".")
        d = self._config
        for k in keys[:-1]:
            d = d.setdefault(k, {})
        d[keys[-1]] = value
