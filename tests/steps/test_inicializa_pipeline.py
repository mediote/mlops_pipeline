from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import pytz

from mlops_pipeline.steps.inicializa_pipeline import inicializa_pipeline

# Parâmetros de teste
params = {
    "nome_modal": "Rodovias",
    "nome_projeto": "Previsao de Demandas",
    "nome_modelo": "FB Prophet",
    "tipo_modelo": "Forecasting",
    "dias_validade_modelo": 10,
    "qtd_dias_treino_inicial": 150,
    "qtd_dias_range_retreino_01": 250,
    "qtd_dias_range_retreino_02": 300,
    "qtd_dias_range_retreino_03": 366,
    "limiar_minino_acc": 0.8,
    "limiar_maximo_drift": 0.2,
    "qtd_permitida_retreino": 3,
    "tipo_esteira": 3,
    "email_usuario": "andresousa.triad@grupoccr.com.br",
    "data_inicio_etapa_execucao_pipeline": datetime.now(pytz.timezone("America/Sao_Paulo"))
}


@pytest.fixture
def storage():
    return MagicMock()


@pytest.mark.inicializa_pipeline
def test_inicializa_pipeline_green_path(storage):
    """
    Testa o fluxo quando o estado do pipeline é "green".
    Verifica se a função retorna "green" e se a função storage.save_dataframe é chamada.
    """
    execucao_atual = pd.DataFrame({
        "data_validade_modelo": ["2099-12-31"],
        "qtd_medida_retreino": [0],
        "status_execucao_pipeline": ["green"],
        "id_execucao_pipeline": [1]
    })

    with patch('mlops_pipeline.storage.Storage.obtem_estado_execucao_atual_pipeline', return_value=execucao_atual), \
            patch('mlops_pipeline.utils.obtem_percentual_restante_validade_modelo', return_value=100), \
            patch('mlops_pipeline.storage.Storage.grava_estado_execucao_atual_pipeline') as mock_save:

        status = inicializa_pipeline(params)

        assert status == "green"
        assert mock_save.called


@pytest.mark.inicializa_pipeline
def test_inicializa_pipeline_red_path(storage):
    """
    Testa o fluxo quando o estado do pipeline é "red".
    Verifica se a função retorna "red".
    """
    execucao_atual = pd.DataFrame({
        "data_validade_modelo": ["2099-12-31"],
        "qtd_medida_retreino": [0],
        "status_execucao_pipeline": ["red"],
        "id_execucao_pipeline": [1]
    })

    with patch('mlops_pipeline.storage.Storage.obtem_estado_execucao_atual_pipeline', return_value=execucao_atual), \
            patch('mlops_pipeline.utils.obtem_percentual_restante_validade_modelo', return_value=100):

        status = inicializa_pipeline(params)

        assert status == "red"


@pytest.mark.inicializa_pipeline
def test_inicializa_pipeline_retraining_limit_exceeded(storage):
    """
    Testa o fluxo quando o limite de retreino é excedido.
    Verifica se a função retorna "red" e se a função storage.save_dataframe é chamada.
    """
    execucao_atual = pd.DataFrame({
        "data_validade_modelo": ["2099-12-31"],
        "qtd_medida_retreino": [4],
        "status_execucao_pipeline": ["green"],
        "id_execucao_pipeline": [1]
    })

    with patch('mlops_pipeline.storage.Storage.obtem_estado_execucao_atual_pipeline', return_value=execucao_atual), \
            patch('mlops_pipeline.utils.obtem_percentual_restante_validade_modelo', return_value=100), \
            patch('mlops_pipeline.storage.Storage.grava_estado_execucao_atual_pipeline') as mock_save:

        status = inicializa_pipeline(params)

        assert status == "red"
        assert mock_save.called


@pytest.mark.inicializa_pipeline
def test_inicializa_pipeline_validity_retraining(storage):
    """
    Testa o fluxo quando ocorre retreino por validade.
    Verifica se a função retorna "yellow" e se a função storage.save_dataframe é chamada.
    """
    execucao_atual = pd.DataFrame({
        "data_validade_modelo": ["2099-12-31"],
        "qtd_medida_retreino": [1],
        "status_execucao_pipeline": ["green"],
        "id_execucao_pipeline": [1]
    })

    with patch('mlops_pipeline.storage.Storage.obtem_estado_execucao_atual_pipeline', return_value=execucao_atual), \
            patch('mlops_pipeline.utils.obtem_percentual_restante_validade_modelo', return_value=100), \
            patch('mlops_pipeline.storage.Storage.grava_estado_execucao_atual_pipeline') as mock_save:

        status = inicializa_pipeline(params)

        assert status == "yellow"
        assert mock_save.called


@pytest.mark.inicializa_pipeline
def test_inicializa_pipeline_initial_run(storage):
    """
    Testa o fluxo na execução inicial do pipeline.
    Verifica se a função retorna "white" e se a função storage.save_dataframe é chamada.
    """
    execucao_atual = pd.DataFrame()

    with patch('mlops_pipeline.storage.Storage.obtem_estado_execucao_atual_pipeline', return_value=execucao_atual), \
            patch('mlops_pipeline.storage.Storage.grava_estado_execucao_atual_pipeline') as mock_save:

        status = inicializa_pipeline(params)

        assert status == "white"
        assert mock_save.called
