import unittest

import pandas as pd

from mlops_pipeline.storage import Storage


class TestStorage(unittest.TestCase):

    def test_save_load_lakehouse(self):
        storage = Storage(backend='lakehouse')
        data = {'col1': [1, 2], 'col2': ['A', 'B']}
        df = pd.DataFrame(data)
        storage.save_dataframe(df, 'test_table')
        loaded_df = storage.load_dataframe('test_table')
        pd.testing.assert_frame_equal(df, loaded_df)

    def test_save_load_sqlserver(self):
        connection_string = "mssql+pyodbc://username:password@server/database?driver=ODBC+Driver+17+for+SQL+Server"
        storage = Storage(backend='sqlserver', connection_string=connection_string)
        data = {'col1': [1, 2], 'col2': ['A', 'B']}
        df = pd.DataFrame(data)
        storage.save_dataframe(df, 'test_table')
        loaded_df = storage.load_dataframe('test_table')
        pd.testing.assert_frame_equal(df, loaded_df)

if __name__ == '__main__':
    unittest.main()
