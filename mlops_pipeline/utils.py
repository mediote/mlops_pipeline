from datetime import datetime

import pandas as pd


def obtem_percentual_restante_validade_modelo(execucao_atual: pd.DataFrame) -> float:
    data_validade_modelo = execucao_atual["data_validade_modelo"].iloc[0]
    dias_validade_modelo = execucao_atual["dias_validade_modelo"].iloc[0]
    dias_ate_fim_validade = (
        datetime.strptime(data_validade_modelo, "%Y-%m-%d").date() - datetime.now().date()
    ).days
    if dias_validade_modelo > 0:
        percentual_restante_validade_modelo = round(
            (dias_ate_fim_validade / dias_validade_modelo) * 100, 2
        )
    else:
        percentual_restante_validade_modelo = 0
    return percentual_restante_validade_modelo
    return percentual_restante_validade_modelo
