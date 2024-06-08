import pytz

from mlops_pipeline.steps.inicializa_pipeline import inicializa_pipeline
from mlops_pipeline.steps.monitora_e_prediz import monitora_drift_faz_predicoes
from mlops_pipeline.steps.treina_avalia_modelos import treina_avalia_modelos
from mlops_pipeline.storage import Storage


class Pipeline:
    def __init__(self):
        self.storage = Storage()
        self.saopaulo_timezone = pytz.timezone("America/Sao_Paulo")

    def inicializa_pipeline(self, params: dict) -> str:
        return inicializa_pipeline(self.storage, params)

    def treina_avalia_modelos(self, params: dict) -> str:
        return treina_avalia_modelos(self.storage, params)

    def monitora_drift_faz_predicoes(self, params: dict) -> str:
        return monitora_drift_faz_predicoes(self.storage, params)
