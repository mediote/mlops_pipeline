from datetime import datetime, timedelta

import pandas as pd
import pytz
from pydantic import BaseModel, ValidationError

from mlops_pipeline.storage import (get_pipeline_run_state,
                                    set_pipeline_run_state)
from mlops_pipeline.utils import get_model_remaining_validity_percentage


class InitPipelineParams(BaseModel):
    nome_modal: str
    nome_projeto: str
    nome_modelo: str
    tipo_modelo: str
    tipo_esteira: int
    qtd_permitida_retreino: int
    limiar_maximo_drift: float
    limiar_minino_acc: float
    dias_validade_modelo: int
    qtd_dias_treino_inicial: int
    qtd_dias_range_retreino_01: int
    qtd_dias_range_retreino_02: int
    qtd_dias_range_retreino_03: int
    email_usuario: str
    data_inicio_etapa_execucao_pipeline: datetime


def init_pipeline(params: dict, delta_path: str) -> str:
    """
    Inicializa o pipeline com os parâmetros fornecidos e gerencia o estado da execução do pipeline.

    Args:
        storage (Storage): Instância da classe Storage para acessar e manipular dados no Delta Lake.
        params (dict): Dicionário contendo os parâmetros do pipeline.
            - nome_modal (str): Nome do modal.
            - nome_projeto (str): Nome do projeto.
            - nome_modelo (str): Nome do modelo.
            - tipo_modelo (str): Tipo do modelo.
            - tipo_esteira (int): Tipo de esteira.
            - qtd_permitida_retreino (int): Quantidade permitida de retreino.
            - limiar_maximo_drift (float): Limiar máximo de drift.
            - limiar_minino_acc (float): Limiar mínimo de acurácia.
            - dias_validade_modelo (int): Dias de validade do modelo.
            - qtd_dias_treino_inicial (int): Quantidade de dias de treino inicial.
            - qtd_dias_range_retreino_01 (int): Quantidade de dias para o primeiro range de retreino.
            - qtd_dias_range_retreino_02 (int): Quantidade de dias para o segundo range de retreino.
            - qtd_dias_range_retreino_03 (int): Quantidade de dias para o terceiro range de retreino.
            - email_usuario (str): Email do usuário.
            - data_inicio_etapa_execucao_pipeline (datetime): Data de início da etapa de execução do pipeline.

    Returns:
        str: Status da execução do pipeline, que pode ser "green", "red", "yellow" ou "white".

    Raises:
        ValueError: Se houver erro na validação dos parâmetros.
    """
    try:
        validated_params = InitPipelineParams(**params)
    except ValidationError as e:
        raise ValueError(f"Erro na validação dos parâmetros: {e}")

    nome_modal = validated_params.nome_modal
    nome_projeto = validated_params.nome_projeto
    nome_modelo = validated_params.nome_modelo
    tipo_modelo = validated_params.tipo_modelo
    tipo_esteira = validated_params.tipo_esteira
    qtd_permitida_retreino = validated_params.qtd_permitida_retreino
    limiar_minino_acc = validated_params.limiar_minino_acc
    limiar_maximo_drift = validated_params.limiar_maximo_drift
    dias_validade_modelo = validated_params.dias_validade_modelo
    qtd_dias_treino_inicial = validated_params.qtd_dias_treino_inicial
    qtd_dias_range_retreino_01 = validated_params.qtd_dias_range_retreino_01
    qtd_dias_range_retreino_02 = validated_params.qtd_dias_range_retreino_02
    qtd_dias_range_retreino_03 = validated_params.qtd_dias_range_retreino_03
    email_usuario = validated_params.email_usuario
    data_inicio_etapa_execucao_pipeline = validated_params.data_inicio_etapa_execucao_pipeline

    saopaulo_timezone = pytz.timezone("America/Sao_Paulo")
    now = datetime.now(saopaulo_timezone)

    run_state = get_pipeline_run_state(
        nome_modal, nome_projeto, nome_modelo, delta_path)

    if run_state is not None:
        run_state["percentual_restante_validade_modelo"] = get_model_remaining_validity_percentage(
            run_state)
        data_validade_modelo = run_state["data_validade_modelo"].iloc[0]

        if datetime.strptime(data_validade_modelo, "%Y-%m-%d").date() > now.date() and run_state["qtd_medida_retreino"].iloc[0] <= qtd_permitida_retreino:
            if run_state["status_execucao_pipeline"].iloc[0] == "green":
                status_execucao_pipeline = run_state["status_execucao_pipeline"].iloc[0]
                run_state["resumo_execucao"] = "Preparando para drift/predicao"
                run_state["id_execucao_pipeline"] = run_state["id_execucao_pipeline"].iloc[0] + 1
                run_state["id_etapa_execucao_pipeline"] = 0
                run_state["data_inicio_etapa_execucao_pipeline"] = data_inicio_etapa_execucao_pipeline
                run_state["data_fim_etapa_execucao_pipeline"] = datetime.now(
                    saopaulo_timezone)
                set_pipeline_run_state(run_state, delta_path)
                return status_execucao_pipeline
            else:
                return "red"
        elif run_state["qtd_medida_retreino"].iloc[0] >= qtd_permitida_retreino:
            run_state["id_execucao_pipeline"] = run_state["id_execucao_pipeline"].iloc[0] + 1
            run_state["id_etapa_execucao_pipeline"] = 0
            run_state["resumo_execucao"] = "Limite de Retreino Por Drift Excedido"
            run_state["status_execucao_pipeline"] = "red"
            run_state["data_inicio_etapa_execucao_pipeline"] = data_inicio_etapa_execucao_pipeline
            run_state["data_fim_etapa_execucao_pipeline"] = datetime.now(
                saopaulo_timezone)
            set_pipeline_run_state(run_state, delta_path)
            return "red"
        else:
            run_state["id_execucao_pipeline"] = run_state["id_execucao_pipeline"].iloc[0] + 1
            run_state["resumo_execucao"] = "Retreino por Validade"
            run_state["status_execucao_pipeline"] = "yellow"
            run_state["etapa_retreino_modelo"] = run_state["etapa_retreino_modelo"].iloc[0] + 1
            run_state["id_etapa_execucao_pipeline"] = 0
            run_state["valor_medido_metrica_modelo"] = 0
            run_state["valor_medido_drift"] = 0
            run_state["data_inicio_etapa_execucao_pipeline"] = data_inicio_etapa_execucao_pipeline
            run_state["data_fim_etapa_execucao_pipeline"] = datetime.now(
                saopaulo_timezone)
            set_pipeline_run_state(run_state, delta_path)
            return "white"
    else:
        run_state = pd.DataFrame([{
            "id_experimento": 0,
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
            "data_validade_modelo": (now + timedelta(days=dias_validade_modelo)).strftime("%Y-%m-%d"),
            "dias_validade_modelo": dias_validade_modelo,
            "percentual_restante_validade_modelo": 1.0,
            "duracao_treinamento_modelo": 0,
            "qtd_linhas_treinamento": 0,
            "qtd_linhas_predicao": 0,
            "limiar_minino_acc": limiar_minino_acc,
            "valor_medido_acc": .0,
            "qtd_dias_treino_inicial": qtd_dias_treino_inicial,
            "qtd_dias_range_retreino_01": qtd_dias_range_retreino_01,
            "qtd_dias_range_retreino_02": qtd_dias_range_retreino_02,
            "qtd_dias_range_retreino_03": qtd_dias_range_retreino_03,
            "etapa_retreino_modelo": 0,
            "qtd_permitida_retreino": qtd_permitida_retreino,
            "qtd_medida_retreino": 0,
            "limiar_maximo_drift": limiar_maximo_drift,
            "valor_medido_drift": .0,
            "nome_cluster_execucao": "adb_dataops_ds_dev",
            "utilizacao_cpu": .0,
            "utilizacao_gpu": .0,
            "utilizacao_memoria": .0,
            "tipo_esteira": tipo_esteira,
            "email_usuario": email_usuario,
            "data_criacao": datetime.now(saopaulo_timezone)
        }])
        set_pipeline_run_state(run_state, delta_path)
        return "white"


class ExecutionStepParams(BaseModel):
    nome_modal: str
    nome_projeto: str
    nome_modelo: str
    etapa_execucao_pipeline: str
    data_inicio_etapa_execucao_pipeline: datetime


def update_pipeline_execution_step(params: dict, delta_path: str) -> str:
    """
    Atualiza a execução de uma etapa no pipeline.

    Args:
        storage (Storage): Instância do Storage para gravar o estado.
        execucao_atual (pd.DataFrame): DataFrame com o estado atual da execução.
        etapa (str): Nome da etapa atual da execução.
        data_inicio_etapa_execucao_pipeline (datetime): Data e hora de início da etapa do pipeline.

    Raises:
        Exception: Se ocorrer um erro ao atualizar o estado da execução.
    """
    try:
        validated_params = ExecutionStepParams(**params)
    except ValidationError as e:
        raise ValueError(f"Erro na validação dos parâmetros: {e}")

    nome_modal = validated_params.nome_modal
    nome_projeto = validated_params.nome_projeto
    nome_modelo = validated_params.nome_modelo
    data_inicio_etapa_execucao_pipeline = validated_params.data_inicio_etapa_execucao_pipeline
    etapa_execucao_pipeline = validated_params.etapa_execucao_pipeline

    saopaulo_timezone = pytz.timezone("America/Sao_Paulo")

    run_state = get_pipeline_run_state(
        nome_modal, nome_projeto, nome_modelo, delta_path)

    run_state["id_etapa_execucao_pipeline"] = run_state["id_etapa_execucao_pipeline"].iloc[0] + 1
    run_state["etapa_execucao_pipeline"] = etapa_execucao_pipeline
    run_state["data_inicio_etapa_execucao_pipeline"] = data_inicio_etapa_execucao_pipeline
    run_state["data_fim_etapa_execucao_pipeline"] = datetime.now(saopaulo_timezone)

    try:
        set_pipeline_run_state(run_state, delta_path)
    except Exception as e:
        raise Exception(f"Erro ao atualizar a execução da etapa: {e}")
