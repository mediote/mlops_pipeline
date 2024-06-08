import pandas as pd
from pyspark.sql import SparkSession


class Storage:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("mlopsutils") \
            .getOrCreate()

    def obtem_estado_execucao_atual_pipeline(self, nome_modal: str, nome_projeto: str, nome_modelo: str, tipo_esteira: int) -> pd.DataFrame:
        query = f"""(
                    select 
                        *
                    from 
                        hive_metastore.controle.tbl_controle_esteira_{tipo_esteira}
                    where 
                        data_criacao = (
                            select
                                max(data_criacao) 
                            from 
                                hive_metastore.controle.tbl_controle_esteira_{tipo_esteira}
                            where
                                nome_modal = '{nome_modal}' 
                                and nome_projeto = '{nome_projeto}' 
                                and nome_modelo = '{nome_modelo}' 
                        )
                )"""

        try:
            df = self.spark.sql(query)
            return df.toPandas()
        except Exception as e:
            raise Exception(f"Error executing SQL query: {e}")

    def grava_estado_execucao_atual_pipeline(self, execucao_atual: pd.DataFrame, tipo_esteira: int):
        try:
            sdf = self.spark.createDataFrame(execucao_atual)
            sdf.write.mode('append').insertInto(
                f"hive_metastore.controle.tbl_controle_esteira_{tipo_esteira}")
        except Exception as e:
            raise Exception(f"Error inserting data into table: {e}")
