from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from mlops_pipeline.steps import init_pipeline


@pytest.fixture
def params():
    return {
        "nome_modal": "modal_test",
        "nome_projeto": "projeto_test",
        "nome_modelo": "modelo_test",
        "tipo_modelo": "tipo_test",
        "tipo_esteira": 1,
        "qtd_permitida_retreino": 5,
        "limiar_maximo_drift": 0.2,
        "limiar_minino_acc": 0.8,
        "dias_validade_modelo": 30,
        "qtd_dias_treino_inicial": 10,
        "qtd_dias_range_retreino_01": 5,
        "qtd_dias_range_retreino_02": 10,
        "qtd_dias_range_retreino_03": 15,
        "email_usuario": "usuario@test.com",
        "data_inicio_etapa_execucao_pipeline": datetime.now()
    }


@patch('mlops_pipeline.steps.get_pipeline_run_state')
@patch('mlops_pipeline.steps.set_pipeline_run_state')
def test_init_pipeline_first_run(mock_set_pipeline_run_state, mock_get_pipeline_run_state, params):
    mock_get_pipeline_run_state.return_value = None
    result = init_pipeline(params, "delta_path")
    assert result == "white"
    mock_set_pipeline_run_state.assert_called_once()


@patch('mlops_pipeline.steps.get_pipeline_run_state')
@patch('mlops_pipeline.steps.set_pipeline_run_state')
def test_init_pipeline_existing_run_green_status(mock_set_pipeline_run_state, mock_get_pipeline_run_state, params):
    run_state = pd.DataFrame([{
        "nome_modal": "modal_test",
        "nome_projeto": "projeto_test",
        "nome_modelo": "modelo_test",
        "status_execucao_pipeline": "green",
        "qtd_medida_retreino": 3,
        "data_validade_modelo": (datetime.now() + timedelta(days=10)).strftime("%Y-%m-%d")
    }])
    mock_get_pipeline_run_state.return_value = run_state
    result = init_pipeline(params, "delta_path")
    assert result == "green"
    mock_set_pipeline_run_state.assert_called_once()


@patch('mlops_pipeline.steps.get_pipeline_run_state')
@patch('mlops_pipeline.steps.set_pipeline_run_state')
def test_init_pipeline_existing_run_red_status(mock_set_pipeline_run_state, mock_get_pipeline_run_state, params):
    run_state = pd.DataFrame([{
        "nome_modal": "modal_test",
        "nome_projeto": "projeto_test",
        "nome_modelo": "modelo_test",
        "status_execucao_pipeline": "red",
        "qtd_medida_retreino": 6,
        "data_validade_modelo": (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d")
    }])
    mock_get_pipeline_run_state.return_value = run_state
    result = init_pipeline(params, "delta_path")
    assert result == "red"
    mock_set_pipeline_run_state.assert_called_once()
