# MLOps Pipeline

Este projeto é responsável por controlar a execução de um pipeline de MLOps e armazenar dados em diferentes backends (Lakehouse e SQL Server). Inclui testes automatizados, integração contínua e documentação.

## Estrutura do Projeto

```plaintext
mlops_pipeline/
├── mlops_pipeline/
│   ├── __init__.py
│   ├── steps/
│   │   ├── __init__.py
│   │   └── inicializa_pipeline.py
│   │   └── monitora_e_prediz.py
│   │   └── treina_avalia_modelos.py
│   ├── storage.py
│   ├── utils.py
│   ├── main.py
├── tests/
│   ├── __init__.py
│   ├── steps/
│   │   ├── __init__.py
│   │   └── test_inicializa_pipeline.py
├── setup.py
├── pyproject.toml
├── README.md
├── .gitignore
