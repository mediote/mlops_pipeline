import os

import pandas as pd
from pyspark.sql import SparkSession


class Storage:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("mlopsutils") \
            .getOrCreate()

    def obtem_estado_execucao_atual_pipeline(self, delta_path: str, nome_modal: str, nome_projeto: str, nome_modelo: str) -> pd.DataFrame:
        """
        Obtém o estado de execução atual do pipeline filtrando um DataFrame Delta.

        Args:
            delta_path (str): Caminho completo do arquivo Delta.
            nome_modal (str): Nome do modal (e.g., rodovias, aeroportos).
            nome_projeto (str): Nome do projeto.
            nome_modelo (str): Nome do modelo.

        Returns:
            pd.DataFrame: DataFrame contendo o registro mais recente do estado de execução atual do pipeline.
            None: Se nenhum registro correspondente for encontrado.

        Raises:
            Exception: Se houver um erro ao acessar ou manipular o DataFrame.
        """
        try:
            # Carrega o DataFrame Delta
            df = self.spark.read.format("delta").load(delta_path)

            # Converte para pandas DataFrame para facilitar a manipulação
            df = df.toPandas()

            # Filtra os dados conforme os critérios especificados
            filtered_df = df[
                (df['nome_modal'] == nome_modal) &
                (df['nome_projeto'] == nome_projeto) &
                (df['nome_modelo'] == nome_modelo)
            ]

            if filtered_df.empty:
                return None

            # Obtém a linha com a data_criacao mais recente
            latest_record = filtered_df.loc[filtered_df['data_criacao'].idxmax()]

            return latest_record.to_frame().T  # Converte de volta para DataFrame
        except Exception as e:
            raise Exception(f"Error accessing DataFrame: {e}")

    def grava_estado_execucao_atual_pipeline(self, delta_path: str, execucao_atual: pd.DataFrame):
        """
        Grava o estado de execução atual do pipeline em uma tabela Delta.

        Args:
            delta_path (str): Caminho completo do arquivo Delta.
            execucao_atual (pd.DataFrame): DataFrame contendo o estado de execução atual do pipeline.

        Raises:
            Exception: Se houver um erro ao inserir os dados na tabela.
        """
        try:
            sdf = self.spark.createDataFrame(execucao_atual)
            sdf.write.format("delta").mode('append').save(delta_path)
        except Exception as e:
            raise Exception(f"Error inserting data into table: {e}")
