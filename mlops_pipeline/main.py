import pytz

from mlops_pipeline.steps.inicializa_pipeline import inicializa_pipeline
from mlops_pipeline.storage import Storage


class Pipeline:
    def __init__(self):
        self.storage = Storage()
        self.saopaulo_timezone = pytz.timezone("America/Sao_Paulo")

    def inicializa_pipeline(self, params: dict) -> str:
        return inicializa_pipeline(self.storage, params)
