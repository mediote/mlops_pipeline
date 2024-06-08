import pytz

from mlops_pipeline.steps.inicializa_pipeline import inicializa_pipeline
from mlops_pipeline.storage import Storage


class Pipeline:
    def __init__(self, control_table_path='/mnt/gold/MLOPS'):
        self.storage = Storage(base_path=control_table_path)
        self.saopaulo_timezone = pytz.timezone("America/Sao_Paulo")

    def inicializa_pipeline(self, params: dict) -> str:
        return inicializa_pipeline(self.storage, params)
