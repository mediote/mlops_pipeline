import pandas as pd

from mlops_pipeline.storage.py import Storage


def run_pipeline(storage_backend='lakehouse', connection_string=None):
    # Inicialize o armazenamento
    storage = Storage(backend=storage_backend, connection_string=connection_string)
    
    # Simulação de criação de um DataFrame
    data = {
        'id_experimento': [1, 2],
        'nome_modal': ['rodovias', 'aeroportos'],
        'nome_projeto': ['Projeto A', 'Projeto B'],
        # Adicione outras colunas conforme necessário
    }
    df = pd.DataFrame(data)
    
    # Salve o DataFrame
    storage.save_dataframe(df, 'tabela_de_controle')

if __name__ == "__main__":
    # Exemplo de execução
    connection_string = "mssql+pyodbc://username:password@server/database?driver=ODBC+Driver+17+for+SQL+Server"
    run_pipeline(storage_backend='sqlserver', connection_string=connection_string)
    # Ou para Lakehouse
    run_pipeline(storage_backend='lakehouse')
    # Ou para Lakehouse
    run_pipeline(storage_backend='lakehouse')
