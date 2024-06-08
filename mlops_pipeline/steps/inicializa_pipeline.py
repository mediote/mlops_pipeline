from datetime import datetime, timedelta

import pandas as pd
import pytz
from pydantic import BaseModel, ValidationError

from mlops_pipeline.storage.storage_factory import StorageFactory
from mlops_pipeline.utils import obtem_percentual_restante_validade_modelo


class InicializaPipelineParams(BaseModel):
    nome_modal: str
    nome_projeto: str
    nome_modelo: str
    tipo_modelo: str
    dias_validade_modelo: int
    qtd_dias_treino_inicial: int
    qtd_dias_range_retreino_01: int
    qtd_dias_range_retreino_02: int
    qtd_dias_range_retreino_03: int
    limiar_minino_acc: float
    limiar_maximo_drift: float
    qtd_permitida_retreino: int
    tipo_esteira: int
    email_usuario: str
    data_inicio_etapa_execucao_pipeline: datetime


def inicializa_pipeline(storage, params: dict) -> str:
    try:
        validated_params = InicializaPipelineParams(**params)
    except ValidationError as e:
        raise ValueError(f"Erro na validação dos parâmetros: {e}")

    nome_modal = validated_params.nome_modal
    nome_projeto = validated_params.nome_projeto
    nome_modelo = validated_params.nome_modelo
    tipo_modelo = validated_params.tipo_modelo
    dias_validade_modelo = validated_params.dias_validade_modelo
    qtd_dias_treino_inicial = validated_params.qtd_dias_treino_inicial
    qtd_dias_range_retreino_01 = validated_params.qtd_dias_range_retreino_01
    qtd_dias_range_retreino_02 = validated_params.qtd_dias_range_retreino_02
    qtd_dias_range_retreino_03 = validated_params.qtd_dias_range_retreino_03
    limiar_minino_acc = validated_params.limiar_minino_acc
    limiar_maximo_drift = validated_params.limiar_maximo_drift
    qtd_permitida_retreino = validated_params.qtd_permitida_retreino
    tipo_esteira = validated_params.tipo_esteira
    email_usuario = validated_params.email_usuario
    data_inicio_etapa_execucao_pipeline = validated_params.data_inicio_etapa_execucao_pipeline

    saopaulo_timezone = pytz.timezone("America/Sao_Paulo")
    agora = datetime.now(saopaulo_timezone)

    execucao_atual = storage.load_dataframe(f"tbl_controle_esteira_{tipo_esteira}")

    if not execucao_atual.empty:
        execucao_atual["percentual_restante_validade_modelo"] = obtem_percentual_restante_validade_modelo(
            execucao_atual)
        data_validade_modelo = execucao_atual["data_validade_modelo"].iloc[0]

        if datetime.strptime(data_validade_modelo, "%Y-%m-%d").date() > agora.date() and execucao_atual["qtd_medida_retreino"].iloc[0] <= qtd_permitida_retreino:
            if execucao_atual["status_execucao_pipeline"].iloc[0] == "green":
                status_execucao_pipeline = execucao_atual["status_execucao_pipeline"].iloc[0]
                execucao_atual["resumo_execucao"] = "Preparando para drift/predicao"
                execucao_atual["id_execucao_pipeline"] = execucao_atual["id_execucao_pipeline"].iloc[0] + 1
                execucao_atual["id_etapa_execucao_pipeline"] = 0
                execucao_atual["data_inicio_etapa_execucao_pipeline"] = data_inicio_etapa_execucao_pipeline
                execucao_atual["data_fim_etapa_execucao_pipeline"] = datetime.now(
                    saopaulo_timezone)
                storage.save_dataframe(
                    execucao_atual, f"tbl_controle_esteira_{tipo_esteira}")
                return status_execucao_pipeline
            else:
                return "red"
        elif execucao_atual["qtd_medida_retreino"].iloc[0] >= qtd_permitida_retreino:
            execucao_atual["id_execucao_pipeline"] = execucao_atual["id_execucao_pipeline"].iloc[0] + 1
            execucao_atual["id_etapa_execucao_pipeline"] = 0
            execucao_atual["resumo_execucao"] = "Limite de Retreino Por Drift Excedido"
            execucao_atual["status_execucao_pipeline"] = "red"
            execucao_atual["data_inicio_etapa_execucao_pipeline"] = data_inicio_etapa_execucao_pipeline
            execucao_atual["data_fim_etapa_execucao_pipeline"] = datetime.now(
                saopaulo_timezone)
            storage.save_dataframe(
                execucao_atual, f"tbl_controle_esteira_{tipo_esteira}")
            return "red"
        else:
            execucao_atual["id_execucao_pipeline"] = execucao_atual["id_execucao_pipeline"].iloc[0] + 1
            execucao_atual["resumo_execucao"] = "Retreino por Validade"
            execucao_atual["status_execucao_pipeline"] = "yellow"
            execucao_atual["etapa_retreino_modelo"] = execucao_atual["etapa_retreino_modelo"].iloc[0] + 1
            execucao_atual["id_etapa_execucao_pipeline"] = 0
            execucao_atual["valor_medido_metrica_modelo"] = 0
            execucao_atual["valor_medido_drift"] = 0
            execucao_atual["data_inicio_etapa_execucao_pipeline"] = data_inicio_etapa_execucao_pipeline
            execucao_atual["data_fim_etapa_execucao_pipeline"] = datetime.now(
                saopaulo_timezone)
            storage.save_dataframe(
                execucao_atual, f"tbl_controle_esteira_{tipo_esteira}")
            return "white"
    else:
        execucao_atual = pd.DataFrame([{
            "id_experimento": 1,
            "nome_modal": nome_modal,
            "nome_projeto": nome_projeto,
            "id_execucao_pipeline": 0,
            "id_etapa_execucao_pipeline": 0,
            "status_execucao_pipeline": "white",
            "etapa_execucao_pipeline": "Inicializa Pipeline",
            "data_inicio_etapa_execucao_pipeline": data_inicio_etapa_execucao_pipeline,
            "data_fim_etapa_execucao_pipeline": datetime.now(saopaulo_timezone),
            "resumo_execucao": "Preparacao para Treinamento Inicial",
            "nome_modelo": nome_modelo,
            "versao_modelo": "0.0",
            "tipo_modelo": tipo_modelo,
            "status_modelo": "white",
            "data_validade_modelo": (agora + timedelta(days=dias_validade_modelo)).strftime("%Y-%m-%d"),
            "dias_validade_modelo": dias_validade_modelo,
            "percentual_restante_validade_modelo": 1.0,
            "duracao_treinamento_modelo": 0,
            "qtd_linhas_treinamento": 0,
            "qtd_linhas_predicao": 0,
            "limiar_minino_acc": limiar_minino_acc,
            "valor_medido_acc": 0,
            "qtd_dias_treino_inicial": qtd_dias_treino_inicial,
            "qtd_dias_range_retreino_01": qtd_dias_range_retreino_01,
            "qtd_dias_range_retreino_02": qtd_dias_range_retreino_02,
            "qtd_dias_range_retreino_03": qtd_dias_range_retreino_03,
            "etapa_retreino_modelo": 0,
            "qtd_permitida_retreino": qtd_permitida_retreino,
            "qtd_medida_retreino": 0,
            "limiar_maximo_drift": limiar_maximo_drift,
            "valor_medido_drift": 0,
            "nome_cluster_execucao": "adb_dataops_ds_dev",
            "utilizacao_cpu": 0,
            "utilizacao_gpu": 0,
            "utilizacao_memoria": 0,
            "tipo_esteira": tipo_esteira,
            "email_usuario": email_usuario
        }])
        storage.save_dataframe(execucao_atual, f"tbl_controle_esteira_{tipo_esteira}")
        return "white"
