from datetime import datetime

import pandas as pd


def obtem_percentual_restante_validade_modelo(execucao_atual: pd.DataFrame) -> float:
    """
    Calcula o percentual restante de validade do modelo com base na data de validade e nos dias de validade.

    Args:
        execucao_atual (pd.DataFrame): DataFrame contendo os dados de execução atual do pipeline, incluindo
            "data_validade_modelo" e "dias_validade_modelo".

    Returns:
        float: Percentual de dias restantes antes do modelo expirar. Se o número de dias de validade for zero,
            retorna 0.
    """
    data_validade_modelo = execucao_atual["data_validade_modelo"].iloc[0]
    dias_validade_modelo = execucao_atual["dias_validade_modelo"].iloc[0]
    dias_ate_fim_validade = (
        datetime.strptime(data_validade_modelo, "%Y-%m-%d").date() -
        datetime.now().date()
    ).days
    if dias_validade_modelo > 0:
        percentual_restante_validade_modelo = round(
            (dias_ate_fim_validade / dias_validade_modelo) * 100, 2
        )
    else:
        percentual_restante_validade_modelo = 0
    return percentual_restante_validade_modelo
