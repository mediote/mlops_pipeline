from mlops_pipeline.steps.init_pipeline import init_pipeline
from mlops_pipeline.storage import Storage


class Pipeline:
    def __init__(self, delta_path: str, params: dict):
        self.params = params
        self.storage = Storage(delta_path)

    def init_pipeline(self) -> str:
        return init_pipeline(self.storage, self.params)
