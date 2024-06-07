import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class Storage:
    def __init__(self, backend='lakehouse', connection_string=None):
        self.backend = backend
        if backend == 'sqlserver' and connection_string:
            self.engine = create_engine(connection_string)
            self.Session = sessionmaker(bind=self.engine)
        else:
            self.engine = None
            self.Session = None

    def save_dataframe(self, df, table_name):
        if self.backend == 'lakehouse':
            self.save_to_lakehouse(df, table_name)
        elif self.backend == 'sqlserver':
            self.save_to_sqlserver(df, table_name)

    def save_to_lakehouse(self, df, table_name):
        # Supondo que o Lakehouse utilize arquivos Parquet
        df.to_parquet(f'{table_name}.parquet')

    def save_to_sqlserver(self, df, table_name):
        if self.engine:
            df.to_sql(table_name, con=self.engine, if_exists='append', index=False)

    def load_dataframe(self, table_name):
        if self.backend == 'lakehouse':
            return self.load_from_lakehouse(table_name)
        elif self.backend == 'sqlserver':
            return self.load_from_sqlserver(table_name)

    def load_from_lakehouse(self, table_name):
        return pd.read_parquet(f'{table_name}.parquet')

    def load_from_sqlserver(self, table_name):
        if self.engine:
            query = f'SELECT * FROM {table_name}'
            return pd.read_sql(query, con=self.engine)

# Configuração para SQL Server
connection_string = "mssql+pyodbc://username:password@server/database?driver=ODBC+Driver+17+for+SQL+Server"
storage_sql = Storage(backend='sqlserver', connection_string=connection_string)

# Configuração para Lakehouse
storage_lakehouse = Storage(backend='lakehouse')

