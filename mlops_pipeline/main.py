from mlops_pipeline.steps.inicializa_pipeline import inicializa_pipeline
from mlops_pipeline.storage import Storage


class Pipeline:
    def __init__(self, delta_path: str, params: dict):
        self.params = params
        self.storage = Storage(delta_path)

    def inicializa_pipeline(self) -> str:
        return inicializa_pipeline(self.storage, self.params)
