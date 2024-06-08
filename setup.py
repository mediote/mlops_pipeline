from setuptools import find_packages, setup

setup(
    name="mlops_pipeline",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "pandas>=1.0.0",
        "pydantic>=1.0",
        "pyspark>=3.0.0",
        "pyodbc>=4.0.0",
        "pytest>=6.0.0",
        "pytest-mock>=3.0.0"
    ],
    entry_points={
        "console_scripts": [
            "run_pipeline=mlops_pipeline.main:run_pipeline",
        ],
    },
)
