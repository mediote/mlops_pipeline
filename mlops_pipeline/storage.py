import pandas as pd
from pyspark.sql import SparkSession


class Storage:
    """
    Classe para gerenciar o acesso e a manipulação de dados armazenados no Delta Lake.

    Atributos:
        delta_path (str): Caminho para a tabela Delta Lake.
        spark (SparkSession): Sessão Spark utilizada para ler e escrever dados.
    """

    def __init__(self, delta_path):
        """
        Inicializa a classe Storage com o caminho da tabela Delta Lake e cria uma sessão Spark.

        Args:
            delta_path (str): Caminho para a tabela Delta Lake.
        """
        self.delta_path = delta_path
        self.spark = SparkSession.builder.appName("mlops_pipeline").getOrCreate()

    def obtem_estado_execucao_atual_pipeline(self, nome_modal: str, nome_projeto: str, nome_modelo: str) -> pd.DataFrame:
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
            df = self.spark.read.format("delta").load(self.delta_path)
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

    def grava_estado_execucao_atual_pipeline(self, execucao_atual: pd.DataFrame):
        """
        Grava o estado atual da execução do pipeline na tabela Delta Lake.

        Args:
            execucao_atual (pd.DataFrame): DataFrame contendo o estado atual da execução do pipeline.

        Raises:
            Exception: Se ocorrer um erro ao inserir dados na tabela.
        """
        try:
            # Cria um Spark DataFrame a partir do pandas DataFrame
            sdf = self.spark.createDataFrame(execucao_atual)
            # Converte para o esquema da tabela Delta
            sdf = sdf.select(
                "id_experimento", "nome_modal", "nome_projeto", "id_execucao_pipeline",
                "id_etapa_execucao_pipeline", "etapa_execucao_pipeline", "status_execucao_pipeline",
                "data_inicio_etapa_execucao_pipeline", "data_fim_etapa_execucao_pipeline", "resumo_execucao",
                "nome_modelo", "versao_modelo", "tipo_modelo", "status_modelo", "data_validade_modelo",
                "dias_validade_modelo", "percentual_restante_validade_modelo", "duracao_treinamento_modelo",
                "qtd_linhas_treinamento", "qtd_linhas_predicao", "limiar_minino_acc", "valor_medido_acc",
                "qtd_dias_treino_inicial", "qtd_dias_range_retreino_01", "qtd_dias_range_retreino_02",
                "qtd_dias_range_retreino_03", "etapa_retreino_modelo", "qtd_permitida_retreino", "qtd_medida_retreino",
                "limiar_maximo_drift", "valor_medido_drift", "nome_cluster_execucao", "utilizacao_cpu",
                "utilizacao_gpu", "utilizacao_memoria", "tipo_esteira", "email_usuario", "data_criacao"
            )
            # Escreve no Delta Lake
            sdf.write.format("delta").mode('append').save(self.delta_path)
        except Exception as e:
            raise Exception(f"Error inserting data into table: {e}")
