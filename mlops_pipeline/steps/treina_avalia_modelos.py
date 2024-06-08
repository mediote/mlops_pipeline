from datetime import datetime

import pandas as pd
import pytz
from pydantic import BaseModel, ValidationError

from mlops_pipeline.storage.storage_base import (grava_estado_execucao_atual_pipeline,
                                    obtem_estado_execucao_atual_pipeline)


class TreinaAvaliaModelosParams(BaseModel):
    nome_modal: str
    nome_projeto: str
    nome_modelo: str
    tipo_modelo: str
    tipo_esteira: int
    valor_medido_acc: float
    duracao_treinamento_modelo: int
    limiar_minino_acc: float


def treina_avalia_modelos(storage, saopaulo_timezone, params: dict) -> str:
    try:
        validated_params = TreinaAvaliaModelosParams(**params)
    except ValidationError as e:
        raise ValueError(f"Erro na validação dos parâmetros: {e}")

    nome_modal = validated_params.nome_modal
    nome_projeto = validated_params.nome_projeto
    nome_modelo = validated_params.nome_modelo
    tipo_modelo = validated_params.tipo_modelo
    tipo_esteira = validated_params.tipo_esteira
    valor_medido_acc = validated_params.valor_medido_acc
    duracao_treinamento_modelo = validated_params.duracao_treinamento_modelo
    limiar_minino_acc = validated_params.limiar_minino_acc

    execucao_atual = obtem_estado_execucao_atual_pipeline(storage, nome_modal, nome_projeto, nome_modelo, tipo_esteira)

    if not execucao_atual.empty:
        execucao_atual['id_etapa_execucao_pipeline'] = execucao_atual['id_etapa_execucao_pipeline'].iloc[0] + 1
        execucao_atual['valor_medido_acc'] = valor_medido_acc
        execucao_atual['duracao_treinamento_modelo'] = duracao_treinamento_modelo
        execucao_atual['nome_modelo'] = nome_modelo
        execucao_atual['valor_medido_drift'] = 0
        execucao_atual["etapa_execucao_pipeline"] = "Treina e Avalia Modelos"

        if valor_medido_acc >= limiar_minino_acc:
            execucao_atual['status_execucao_pipeline'] = 'green'
            execucao_atual['status_modelo'] = 'green'
            execucao_atual["resumo_execucao"] = "Treinamento Terminado com Sucesso"
            execucao_atual['etapa_retreino_modelo'] = 0
            execucao_atual["data_fim_etapa_execucao_pipeline"] = datetime.now(saopaulo_timezone)
            exit_message = 'end_loop'
        else:
            if execucao_atual['etapa_retreino_modelo'].iloc[0] < 3:
                execucao_atual["resumo_execucao"] = "Retreino por Desempenho"
                execucao_atual['status_execucao_pipeline'] = 'yellow'
                execucao_atual['status_modelo'] = 'white'
                execucao_atual['etapa_retreino_modelo'] = execucao_atual['etapa_retreino_modelo'].iloc[0] + 1
                execucao_atual["data_fim_etapa_execucao_pipeline"] = datetime.now(saopaulo_timezone)
                exit_message = 'continue_loop'
            else:
                execucao_atual["resumo_execucao"] = "Terminado por Excesso de Retreino"
                execucao_atual['status_execucao_pipeline'] = 'red'
                execucao_atual['status_modelo'] = 'red'
                execucao_atual["data_fim_etapa_execucao_pipeline"] = datetime.now(saopaulo_timezone)
                exit_message = 'end_loop'

        grava_estado_execucao_atual_pipeline(storage, execucao_atual, tipo_esteira)
        return exit_message
    else:
        return 'end_loop'
