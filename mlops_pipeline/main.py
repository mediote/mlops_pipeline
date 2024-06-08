from mlops_pipeline.steps.inicializa_pipeline import inicializa_pipeline
from mlops_pipeline.storage import Storage


class Pipeline:
    def __init__(self, delta_path):
        self.storage = Storage(delta_path)

    def inicializa_pipeline(self, params: dict) -> str:
        return inicializa_pipeline(self.storage, params)
