from .storage_base import LakehouseStorage, SQLServerStorage


class StorageFactory:
    @staticmethod
    def create_storage(backend, connection_string=None):
        if backend == 'sqlserver':
            if connection_string is None:
                raise ValueError("Connection string is required for SQL Server storage")
            return SQLServerStorage(connection_string)
        elif backend == 'lakehouse':
            return LakehouseStorage()
        else:
            raise ValueError(f"Unsupported backend: {backend}")
