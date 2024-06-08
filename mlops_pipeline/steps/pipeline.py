import pytz

from mlops_pipeline.steps.inicializa_pipeline import inicializa_pipeline
from mlops_pipeline.steps.monitora_e_prediz import \
    monitora_drift_faz_predicoes
from mlops_pipeline.steps.treina_avalia_modelos import treina_avalia_modelos
from mlops_pipeline.storage.storage_base import Storage


class Pipeline:
    def __init__(self, storage_backend='lakehouse', connection_string=None):
        self.storage = Storage(backend=storage_backend, connection_string=connection_string)
        self.saopaulo_timezone = pytz.timezone("America/Sao_Paulo")

    def inicializa_pipeline(self, params: dict) -> str:
        return inicializa_pipeline(self.storage, self.saopaulo_timezone, params)

    def treina_avalia_modelos(self, params: dict) -> str:
        return treina_avalia_modelos(self.storage, self.saopaulo_timezone, params)

    def monitora_drift_faz_predicoes(self, params: dict) -> str:
        return monitora_drift_faz_predicoes(self.storage, self.saopaulo_timezone, params)
