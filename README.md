# MLOps Pipeline

Este projeto é responsável por controlar a execução de um pipeline de MLOps e armazenar dados em diferentes backends (Lakehouse e SQL Server). Inclui testes automatizados, integração contínua e documentação.

## Estrutura do Projeto

```plaintext
mlops_pipeline/
├── mlops_pipeline/
│   ├── __init__.py
│   ├── main.py
│   ├── pipeline.py
│   ├── models.py
│   ├── utils.py
│   └── storage.py
├── tests/
│   ├── __init__.py
│   ├── test_main.py
│   ├── test_pipeline.py
│   ├── test_models.py
│   └── test_storage.py
├── docs/
│   ├── index.md
│   └── ...
├── .gitignore
├── README.md
├── requirements.txt
├── setup.py
└── tox.ini
