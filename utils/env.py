from os import environ
from pathlib import Path

from dotenv import load_dotenv


class Env:
    def __init__(self):
        load_dotenv()
        self.BASE = Path(__file__).parent.parent.resolve()

    def __getattr__(self, item):
        attr = environ.get(item.upper())
        setattr(self, item, attr) if attr else ...
        return attr


env = Env()
