import os

import pandas as pd
from pyspark.sql import SparkSession


class Storage:
    def __init__(self):
        self.base_path = '/mnt/gold/MLOPS'
        self.spark = SparkSession.builder \
            .appName("mlopsutils") \
            .getOrCreate()

    def obtem_estado_execucao_atual_pipeline(self, nome_modal: str, nome_projeto: str, nome_modelo: str, tipo_esteira: int) -> pd.DataFrame:
        """
        Obtém o estado de execução atual do pipeline filtrando um DataFrame Parquet.

        Args:
            nome_modal (str): Nome do modal (e.g., rodovias, aeroportos).
            nome_projeto (str): Nome do projeto.
            nome_modelo (str): Nome do modelo.
            tipo_esteira (int): Tipo da esteira.

        Returns:
            pd.DataFrame: DataFrame contendo o registro mais recente do estado de execução atual do pipeline.

        Raises:
            Exception: Se houver um erro ao acessar ou manipular o DataFrame.
        """
        try:
            # Define o caminho absoluto para o arquivo Parquet
            path = os.path.join(
                self.base_path, f'tray{tipo_esteira}/controle/tbl_controle_esteira_{tipo_esteira}.parquet')

            # Carrega o DataFrame Parquet
            df = self.spark.read.parquet(path)

            # Converte para pandas DataFrame para facilitar a manipulação
            df = df.toPandas()

            # Filtra os dados conforme os critérios especificados
            filtered_df = df[
                (df['nome_modal'] == nome_modal) &
                (df['nome_projeto'] == nome_projeto) &
                (df['nome_modelo'] == nome_modelo)
            ]

            # Obtém a linha com a data_criacao mais recente
            latest_record = filtered_df.loc[filtered_df['data_criacao'].idxmax()]

            return latest_record.to_frame().T  # Converte de volta para DataFrame
        except Exception as e:
            raise Exception(f"Error accessing DataFrame: {e}")

    def grava_estado_execucao_atual_pipeline(self, execucao_atual: pd.DataFrame, tipo_esteira: int):
        """
        Grava o estado de execução atual do pipeline em uma tabela Parquet.

        Args:
            execucao_atual (pd.DataFrame): DataFrame contendo o estado de execução atual do pipeline.
            tipo_esteira (int): Tipo da esteira.

        Raises:
            Exception: Se houver um erro ao inserir os dados na tabela.
        """
        try:
            path = os.path.join(
                self.base_path, f'tray{tipo_esteira}/controle/tbl_controle_esteira_{tipo_esteira}.parquet')
            sdf = self.spark.createDataFrame(execucao_atual)
            sdf.write.mode('append').parquet(path)
        except Exception as e:
            raise Exception(f"Error inserting data into table: {e}")
