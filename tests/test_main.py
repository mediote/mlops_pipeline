# tests/test_main.py
import unittest

from mlops_pipeline.main import run_pipeline


class TestMain(unittest.TestCase):
    def test_run_pipeline(self):
        result = run_pipeline()
        self.assertIsNotNone(result)

if __name__ == '__main__':
    unittest.main()

