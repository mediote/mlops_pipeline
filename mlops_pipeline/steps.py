from datetime import datetime, timedelta
from typing import Dict, Optional

import pandas as pd
import pytz
from pydantic import BaseModel, Field, ValidationError

from mlops_pipeline.storage import (get_pipeline_run_state,
                                    set_pipeline_run_state)
from mlops_pipeline.utils import get_model_remaining_validity_percentage

saopaulo_timezone = pytz.timezone("America/Sao_Paulo")


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
    qtd_dados_treino: int
    qtd_dados_retreino_01: int
    qtd_dados_retreino_02: int
    qtd_dados_retreino_03: int
    email_usuario: str
    data_inicio_etapa_pipeline: datetime
    delta_path: str


def init_pipeline(params: Dict) -> str:
    """
    Inicializa o pipeline com os parâmetros fornecidos e gerencia o estado da execução do pipeline.

    Args:
        params (dict): Dicionário contendo os parâmetros do pipeline. Os seguintes parâmetros são esperados:
            - nome_modal (str): Nome do modal. (Obrigatório)
            - nome_projeto (str): Nome do projeto. (Obrigatório)
            - nome_modelo (str): Nome do modelo. (Obrigatório)
            - tipo_modelo (str): Tipo do modelo. (Obrigatório)
            - tipo_esteira (int): Tipo de esteira. (Obrigatório)
            - qtd_permitida_retreino (int): Quantidade permitida de retreino. (Obrigatório)
            - limiar_maximo_drift (float): Limiar máximo de drift. (Obrigatório)
            - limiar_minino_acc (float): Limiar mínimo de acurácia. (Obrigatório)
            - dias_validade_modelo (int): Dias de validade do modelo. (Obrigatório)
            - qtd_dados_treino (int): Quantidade de dados para treino inicial. (Obrigatório)
            - qtd_dados_retreino_01 (int): Quantidade de dados para o primeiro range de retreino. (Obrigatório)
            - qtd_dados_retreino_02 (int): Quantidade de dados para o segundo range de retreino. (Obrigatório)
            - qtd_dados_retreino_03 (int): Quantidade de dados para o terceiro range de retreino. (Obrigatório)
            - email_usuario (str): Email do usuário. (Obrigatório)
            - data_inicio_etapa_pipeline (datetime): Data de início da etapa de execução do pipeline. (Obrigatório)
            - delta_path (str): Caminho para salvar o estado atualizado. (Obrigatório)

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
    qtd_dados_treino = validated_params.qtd_dados_treino
    qtd_dados_retreino_01 = validated_params.qtd_dados_retreino_01
    qtd_dados_retreino_02 = validated_params.qtd_dados_retreino_02
    qtd_dados_retreino_03 = validated_params.qtd_dados_retreino_03
    email_usuario = validated_params.email_usuario
    data_inicio_etapa_pipeline = validated_params.data_inicio_etapa_pipeline
    delta_path = validated_params.delta_path

    now = datetime.now(saopaulo_timezone)

    run_state = get_pipeline_run_state(
        nome_modal,
        nome_projeto,
        nome_modelo,
        delta_path
    )

    if run_state is not None:
        run_state["percentual_restante_validade_modelo"] = get_model_remaining_validity_percentage(
            run_state)
        data_validade_modelo = run_state["data_validade_modelo"].iloc[0]

        if datetime.strptime(data_validade_modelo, "%Y-%m-%d").date() > now.date() and run_state["qtd_medida_retreino"].iloc[0] <= qtd_permitida_retreino:
            if run_state["status_execucao_pipeline"].iloc[0] == "green":
                status_execucao_pipeline = run_state["status_execucao_pipeline"].iloc[0]
                run_state["resumo_execucao_etapa"] = "Preparando para drift/predicao"
                run_state["id_pipeline"] = run_state["id_pipeline"].iloc[0] + 1
                run_state["id_etapa_pipeline"] = 0
                run_state["data_inicio_etapa_pipeline"] = data_inicio_etapa_pipeline
                run_state["data_fim_etapa_pipeline"] = datetime.now(
                    saopaulo_timezone)
                set_pipeline_run_state(run_state, delta_path)
                return status_execucao_pipeline
            else:
                return "red"
        elif run_state["qtd_medida_retreino"].iloc[0] >= qtd_permitida_retreino:
            run_state["id_pipeline"] = run_state["id_pipeline"].iloc[0] + 1
            run_state["id_etapa_pipeline"] = 0
            run_state["nome_etapa_pipeline"] = "Inicializa Pipeline"
            run_state["resumo_execucao_etapa"] = "Limite de Retreino Excedido"
            run_state["status_execucao_pipeline"] = "red"
            run_state["data_inicio_etapa_pipeline"] = data_inicio_etapa_pipeline
            run_state["data_fim_etapa_pipeline"] = datetime.now(
                saopaulo_timezone)
            set_pipeline_run_state(run_state, delta_path)
            return "red"
        else:
            run_state["id_pipeline"] = run_state["id_pipeline"].iloc[0] + 1
            run_state["id_etapa_pipeline"] = 0
            run_state["nome_etapa_pipeline"] = "Inicializa Pipeline"
            run_state["resumo_execucao_etapa"] = "Retreino por Validade"
            run_state["status_execucao_pipeline"] = "white"
            run_state["valor_medido_metrica_modelo"] = None
            run_state["valor_medido_drift"] = None
            run_state["data_inicio_etapa_pipeline"] = data_inicio_etapa_pipeline
            run_state["data_fim_etapa_pipeline"] = datetime.now(
                saopaulo_timezone)
            set_pipeline_run_state(run_state, delta_path)
            return "white"
    else:
        run_state = pd.DataFrame([{
            "nome_modal": nome_modal,
            "nome_projeto": nome_projeto,
            "id_pipeline": 0,
            "id_etapa_pipeline": 0,
            "nome_etapa_pipeline": "Inicializa Pipeline",
            "status_execucao_pipeline": "white",
            "data_inicio_etapa_pipeline": data_inicio_etapa_pipeline,
            "data_fim_etapa_pipeline": datetime.now(saopaulo_timezone),
            "resumo_execucao_etapa": "Preparacao para Treinamento Inicial",
            "nome_modelo": nome_modelo,
            "versao_modelo": " ",
            "tipo_modelo": tipo_modelo,
            "status_modelo": "white",
            "data_validade_modelo": (now + timedelta(days=dias_validade_modelo)).strftime("%Y-%m-%d"),
            "dias_validade_modelo": dias_validade_modelo,
            "percentual_restante_validade_modelo": 1.0,
            "duracao_treinamento_modelo": .0,
            "qtd_dados_predicao": 0,
            "limiar_minino_acc": limiar_minino_acc,
            "valor_medido_acc": .0,
            "qtd_dados_treino": qtd_dados_treino,
            "qtd_dados_retreino_01": qtd_dados_retreino_01,
            "qtd_dados_retreino_02": qtd_dados_retreino_02,
            "qtd_dados_retreino_03": qtd_dados_retreino_03,
            "passo_etapa_retreino_modelo": 0,
            "qtd_permitida_retreino": qtd_permitida_retreino,
            "qtd_medida_retreino": 0,
            "limiar_maximo_drift": limiar_maximo_drift,
            "valor_medido_drift": .0,
            "nome_cluster_execucao": "adb_dataops_ds_dev",
            "utilizacao_cpu": .0,
            "utilizacao_gpu": .0,
            "utilizacao_memoria": .0,
            "tipo_esteira": tipo_esteira,
            "email_usuario": email_usuario
        }])
        set_pipeline_run_state(run_state, delta_path)
        return "white"


class ExecutionStepParams(BaseModel):
    data_inicio_etapa_pipeline: datetime
    nome_etapa_pipeline: str
    utilizacao_cpu: Optional[float] = .0
    utilizacao_gpu: Optional[float] = .0
    utilizacao_memoria: Optional[float] = .0
    resumo_execucao_etapa: Optional[str] = ""


def update_pipeline_execution_step(params: Dict, run_state: pd.DataFrame, delta_path: str):
    """
    Atualiza a execução de uma etapa no pipeline.

    Args:
        params (dict): Dicionário com os parâmetros de execução da etapa. Os seguintes parâmetros são esperados:
            - nome_modal (str): Nome do modal. (Obrigatório)
            - nome_projeto (str): Nome do projeto. (Obrigatório)
            - nome_modelo (str): Nome do modelo. (Obrigatório)
            - data_inicio_etapa_pipeline (datetime): Data e hora de início da etapa do pipeline. (Obrigatório)
            - nome_etapa_pipeline (str): Nome da etapa atual da execução. (Obrigatório)
            - utilizacao_cpu (float, opcional): Utilização de CPU durante a execução da etapa.
            - utilizacao_memoria (float, opcional): Utilização de memória durante a execução da etapa.
            - resumo_execucao_etapa (str, opcional): Resumo da execução da etapa.

        run_state (pd.DataFrame): DataFrame com o estado atual da execução.
        delta_path (str): Caminho para salvar o estado atualizado.

    Raises:
        ValueError: Se ocorrer um erro na validação dos parâmetros.
    """
    try:
        validated_params = ExecutionStepParams(**params)
    except ValidationError as e:
        raise ValueError(f"Erro na validação dos parâmetros: {e}")

    # Atribuindo parâmetros validados a variáveis locais
    data_inicio_etapa_pipeline = validated_params.data_inicio_etapa_pipeline
    nome_etapa_pipeline = validated_params.nome_etapa_pipeline
    utilizacao_cpu = validated_params.utilizacao_cpu
    utilizacao_memoria = validated_params.utilizacao_memoria
    resumo_execucao_etapa = validated_params.resumo_execucao_etapa

    # Atualizando o estado da execução
    run_state["id_etapa_pipeline"] = run_state["id_etapa_pipeline"].iloc[0] + 1
    run_state["data_inicio_etapa_pipeline"] = data_inicio_etapa_pipeline
    run_state["data_fim_etapa_pipeline"] = datetime.now(saopaulo_timezone)
    run_state["nome_etapa_pipeline"] = nome_etapa_pipeline
    run_state["utilizacao_cpu"] = utilizacao_cpu
    run_state["utilizacao_memoria"] = utilizacao_memoria
    run_state["resumo_execucao_etapa"] = resumo_execucao_etapa

    set_pipeline_run_state(run_state, delta_path)


class HandleTrainEvauateModelParams(BaseModel):
    nome_modelo: str
    valor_medido_acc: float
    duracao_treinamento_modelo: float
    data_inicio_etapa_pipeline: datetime
    utilizacao_cpu: Optional[float] = .0
    utilizacao_gpu: Optional[float] = .0
    utilizacao_memoria: Optional[float] = .0
    versao_modelo: Optional[str] = " "


def handle_train_and_evaluate_model(params: Dict, run_state: pd.DataFrame, delta_path: str) -> str:
    """
    Função para treinar e avaliar o modelo, atualizando o estado da execução.

    Parameters:
    - params (Dict): Dicionário com os parâmetros necessários para a função.
    - run_state (pd.DataFrame): DataFrame com o estado atual da execução.
    - delta_path (str): Caminho para salvar o estado atualizado.

    Returns:
    - str: Mensagem indicando o próximo passo ("end_loop" ou "continue_loop").
    """
    try:
        validated_params = HandleTrainEvauateModelParams(**params)
    except ValidationError as e:
        raise ValueError(f"Erro na validação dos parâmetros: {e}")

    data_inicio_etapa_pipeline = validated_params.data_inicio_etapa_pipeline
    nome_modelo = validated_params.nome_modelo
    valor_medido_acc = validated_params.valor_medido_acc
    duracao_treinamento_modelo = validated_params.duracao_treinamento_modelo
    versao_modelo = validated_params.versao_modelo
    utilizacao_cpu = validated_params.utilizacao_cpu
    utilizacao_gpu = validated_params.utilizacao_gpu
    utilizacao_memoria = validated_params.utilizacao_memoria

    now = datetime.now(saopaulo_timezone)

    if run_state is not None:
        run_state["data_inicio_etapa_pipeline"] = data_inicio_etapa_pipeline
        run_state['id_etapa_pipeline'] = run_state['id_etapa_pipeline'][0] + 1
        run_state['valor_medido_acc'] = valor_medido_acc
        run_state['duracao_treinamento_modelo'] = duracao_treinamento_modelo
        run_state['nome_modelo'] = nome_modelo
        run_state["nome_etapa_pipeline"] = "Treina e Avalia Modelos"
        run_state['utilizacao_cpu'] = utilizacao_cpu
        run_state['utilizacao_gpu'] = utilizacao_gpu
        run_state['utilizacao_memoria'] = utilizacao_memoria

        if valor_medido_acc >= run_state["limiar_minino_acc"].iloc[0]:
            run_state['status_execucao_pipeline'] = 'green'
            run_state['status_modelo'] = 'green'
            run_state["resumo_execucao_etapa"] = "Treinamento Terminado com Sucesso"
            run_state['passo_etapa_retreino_modelo'] = 0
            run_state["data_fim_etapa_pipeline"] = now
            run_state["versao_modelo"] = versao_modelo
            exit_message = 'end_loop'
        else:
            if run_state['passo_etapa_retreino_modelo'][0] < 3:
                run_state["resumo_execucao_etapa"] = "Retreino por Desempenho"
                run_state['status_execucao_pipeline'] = 'yellow'
                run_state['status_modelo'] = 'white'
                run_state['passo_etapa_retreino_modelo'] = run_state['passo_etapa_retreino_modelo'][0] + 1
                run_state["data_fim_etapa_pipeline"] = now
                exit_message = 'continue_loop'
            else:
                run_state["resumo_execucao_etapa"] = "Terminado por Excesso de Retreino"
                run_state['status_execucao_pipeline'] = 'red'
                run_state['status_modelo'] = 'red'
                run_state["data_fim_etapa_pipeline"] = now
                exit_message = 'end_loop'

        set_pipeline_run_state(run_state, delta_path)
        return exit_message

    else:
        return "end_loop"


class HandleDriftPredictParams(BaseModel):
    data_inicio_etapa_pipeline: datetime
    valor_medido_drift: float
    qtd_dados_predicao: Optional[int] = Field(default=0)
    utilizacao_cpu: Optional[float] = .0
    utilizacao_gpu: Optional[float] = .0
    utilizacao_memoria: Optional[float] = .0


def handle_drift_and_predict(params: Dict, run_state: pd.DataFrame, delta_path: str) -> str:
    """
    Função para monitorar o drift de dados e fazer previsões, atualizando o estado da execução.

    Parameters:
    - params (Dict): Dicionário com os parâmetros necessários para a função.
    - run_state (pd.DataFrame): DataFrame com o estado atual da execução.
    - delta_path (str): Caminho para salvar o estado atualizado.

    Returns:
    - str: Status do modelo ("green", "yellow", "red").
    """
    try:
        validated_params = HandleDriftPredictParams(**params)
    except ValidationError as e:
        raise ValueError(f"Erro na validação dos parâmetros: {e}")

    data_inicio_etapa_pipeline = validated_params.data_inicio_etapa_pipeline
    valor_medido_drift = validated_params.valor_medido_drift
    qtd_dados_predicao = validated_params.qtd_dados_predicao
    utilizacao_cpu = validated_params.utilizacao_cpu
    utilizacao_gpu = validated_params.utilizacao_cpu
    utilizacao_memoria = validated_params.utilizacao_memoria

    now = datetime.now(saopaulo_timezone)

    if run_state is not None:
        run_state["id_etapa_pipeline"] = run_state["id_etapa_pipeline"].iloc[0] + 1
        run_state["data_inicio_etapa_pipeline"] = data_inicio_etapa_pipeline
        run_state["data_fim_etapa_pipeline"] = now
        run_state["valor_medido_drift"] = valor_medido_drift
        run_state["nome_etapa_pipeline"] = "Avalia Drift e Faz Predicoes"
        run_state["utilizacao_cpu"] = utilizacao_cpu
        run_state["utilizacao_gpu"] = utilizacao_gpu
        run_state["utilizacao_memoria"] = utilizacao_memoria

        if valor_medido_drift <= run_state["limiar_maximo_drift"].iloc[0]:
            status_modelo = "green"
            run_state["status_modelo"] = status_modelo
            run_state["resumo_execucao_etapa"] = "Sem Drift / Predicoes com Sucesso"
            run_state["qtd_dados_predicao"] = qtd_dados_predicao
            run_state['passo_etapa_retreino_modelo'] = 0
            run_state["status_execucao_pipeline"] = status_modelo

        else:
            status_modelo = "yellow"
            run_state["status_modelo"] = status_modelo
            run_state['passo_etapa_retreino_modelo'] = run_state['passo_etapa_retreino_modelo'].iloc[0] + 1
            run_state["status_execucao_pipeline"] = "yellow"
            run_state["resumo_execucao_etapa"] = "Com Drift / Preparando Retreino"
            run_state['qtd_permitida_retreino'] = run_state['qtd_permitida_retreino'].iloc[0] + 1
            run_state["status_execucao_pipeline"] = status_modelo

        set_pipeline_run_state(run_state, delta_path)
        return status_modelo
    else:
        return "red"
