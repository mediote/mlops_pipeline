from pydantic import BaseModel, ValidationError

from mlops_pipeline.storage.storage_base import (grava_estado_execucao_atual_pipeline,
                                    obtem_estado_execucao_atual_pipeline)


class MonitoraDriftFazPredicoesParams(BaseModel):
    nome_modal: str
    nome_projeto: str
    nome_modelo: str
    tipo_modelo: str
    tipo_esteira: int
    data_drift_current: float
    data_drift_max_threshold: float


def monitora_drift_faz_predicoes(storage, saopaulo_timezone, params: dict) -> str:
    try:
        validated_params = MonitoraDriftFazPredicoesParams(**params)
    except ValidationError as e:
        raise ValueError(f"Erro na validação dos parâmetros: {e}")

    nome_modal = validated_params.nome_modal
    nome_projeto = validated_params.nome_projeto
    nome_modelo = validated_params.nome_modelo
    tipo_modelo = validated_params.tipo_modelo
    tipo_esteira = validated_params.tipo_esteira
    data_drift_current = validated_params.data_drift_current
    data_drift_max_threshold = validated_params.data_drift_max_threshold

    execucao_atual = obtem_estado_execucao_atual_pipeline(storage, nome_modal, nome_projeto, nome_modelo, tipo_esteira)

    if not execucao_atual.empty:
        execucao_atual["id_execucao_pipeline"] = execucao_atual["id_execucao_pipeline"].iloc[0] + 1
        execucao_atual["valor_medido_drift"] = data_drift_current

        if data_drift_current <= data_drift_max_threshold:
            execucao_atual["status_execucao_pipeline"] = "green"
            execucao_atual['etapa_retreino_modelo'] = 0
        else:
            execucao_atual["status_execucao_pipeline"] = "yellow"
            execucao_atual['etapa_retreino_modelo'] = execucao_atual['etapa_retreino_modelo'].iloc[0] + 1
            execucao_atual["resumo_execucao"] = "Retreino por Drift"
            execucao_atual['qtd_medida_retreino'] = execucao_atual['qtd_medida_retreino'].iloc[0] + 1

        grava_estado_execucao_atual_pipeline(storage, execucao_atual, tipo_esteira)
        return execucao_atual["status_execucao_pipeline"].iloc[0]
    else:
        return 'red'
