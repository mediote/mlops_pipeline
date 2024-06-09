import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("mlops_pipeline").getOrCreate()


def get_pipeline_run_state(nome_modal: str, nome_projeto: str, nome_modelo: str, delta_path: str) -> pd.DataFrame:
    """
    Obtém o estado atual da execução do pipeline com base no modal, projeto e modelo fornecidos.

    Args:
        nome_modal (str): Nome do modal.
        nome_projeto (str): Nome do projeto.
        nome_modelo (str): Nome do modelo.

    Returns:
        pd.DataFrame: DataFrame com a linha mais recente correspondente aos critérios fornecidos.
                      Retorna None se nenhum registro correspondente for encontrado.

    Raises:
        Exception: Se ocorrer um erro ao acessar o DataFrame.
    """
    try:
        # Carrega o DataFrame Delta
        df = spark.read.format("delta").load(delta_path)
        # Filtra com base nos critérios fornecidos
        filtered_df = df.filter((df.nome_modal == nome_modal) &
                                (df.nome_projeto == nome_projeto) &
                                (df.nome_modelo == nome_modelo))
        if filtered_df.count() == 0:
            return None
        # Converte para pandas DataFrame para facilitar a manipulação
        filtered_df = filtered_df.toPandas()
        # Obtém a linha com a data_criacao mais recente
        if filtered_df.empty:
            return None
        latest_record = filtered_df.loc[filtered_df['data_criacao'].idxmax()]
        return latest_record.to_frame().T  # Converte de volta para DataFrame
    except Exception as e:
        raise Exception(f"Error accessing DataFrame: {e}")


def set_pipeline_run_state(run_state: pd.DataFrame, delta_path: str):
    """
    Grava o estado atual da execução do pipeline na tabela Delta Lake.
    Args:
        run_state (pd.DataFrame): DataFrame contendo o estado atual da execução do pipeline.
    Raises:
        Exception: Se ocorrer um erro ao inserir dados na tabela.
    """
    try:
        # Cria um Spark DataFrame a partir do pandas DataFrame
        sdf = spark.createDataFrame(run_state)
        # Escreve no Delta Lake
        sdf.write.format("delta").mode('append').save(delta_path)
    except Exception as e:
        raise Exception(f"Error inserting data into table: {e}")


def load_features(self, training_window: int, origin_table: str):
    """
    Carrega os dados de uma tabela de origem com base em uma janela de treinamento especificada.
    Args:
        training_window (int): Janela de treinamento em dias.
        origin_table (str): Nome da tabela de origem.
    Returns:
        DataFrame: DataFrame contendo os dados carregados.
    Raises:
        Exception: Se ocorrer um erro ao executar a consulta SQL.
    """
    query = f"""
                    SELECT
                        *
                    FROM
                        {origin_table}
                    WHERE
                        date >= (
                            SELECT
                                DATE_SUB(MAX(date), {training_window}) AS data_limit
                            FROM
                                {origin_table}
                            )
                """
    try:
        df = self.spark.sql(query)
        return df
    except Exception as e:
        raise Exception(f"Error executing SQL query: {e}")
