from datetime import datetime

import pandas as pd


def get_model_remaining_validity_percentage(run_state: pd.DataFrame) -> float:
    """
    Calcula o percentual restante de validade do modelo com base na data de validade e nos dias de validade.

    Args:
        run_state (pd.DataFrame): DataFrame contendo os dados de execução atual do pipeline, incluindo
            "data_validade_modelo" e "dias_validade_modelo".

    Returns:
        float: Percentual de dias restantes antes do modelo expirar. Se o número de dias de validade for zero,
            retorna 0.
    """
    model_expiration_date = run_state["data_validade_modelo"].iloc[0]
    model_validity_days = run_state["dias_validade_modelo"].iloc[0]
    days_until_expiration = (
        datetime.strptime(model_expiration_date, "%Y-%m-%d").date() -
        datetime.now().date()
    ).days
    if model_validity_days > 0:
        remaining_validity_percentage = round(
            (days_until_expiration / model_validity_days) * 100, 2
        )
        # Garantir que o percentual esteja entre 0 e 100
        remaining_validity_percentage = max(0, min(remaining_validity_percentage, 100))
    else:
        remaining_validity_percentage = 0
    return remaining_validity_percentage
