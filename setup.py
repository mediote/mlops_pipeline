from setuptools import find_packages, setup

setup(
    name="mlops_pipeline",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "pandas==2.2.2",
        "flake8==7.0.0",
        "pytest==8.2.2",
        "pydantic==2.7.3",
        "tox==4.15.1",
        "pyspark==3.5.1",
        "pyodbc>=4.0.0",
        "pytest-mock>=3.0.0"
    ]
)
