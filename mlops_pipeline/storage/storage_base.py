import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class StorageBase:
    def save_dataframe(self, df, table_name):
        raise NotImplementedError

    def load_dataframe(self, table_name):
        raise NotImplementedError


class SQLServerStorage(StorageBase):
    def __init__(self, connection_string):
        self.engine = create_engine(connection_string)
        self.Session = sessionmaker(bind=self.engine)

    def save_dataframe(self, df, table_name):
        df.to_sql(table_name, con=self.engine, if_exists='append', index=False)

    def load_dataframe(self, table_name):
        query = f'SELECT * FROM {table_name}'
        return pd.read_sql(query, con=self.engine)


class LakehouseStorage(StorageBase):
    def save_dataframe(self, df, table_name):
        df.to_parquet(f'{table_name}.parquet')

    def load_dataframe(self, table_name):
        return pd.read_parquet(f'{table_name}.parquet')
