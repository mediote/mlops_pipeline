# MLOps Pipeline

Este projeto é responsável por controlar a execução de um pipeline de MLOps e armazenar dados em diferentes backends (Lakehouse e SQL Server). Inclui testes automatizados, integração contínua e documentação.

## Estrutura do Projeto

```plaintext
mlops_pipeline/
├── __init__.py
├── steps/
│   ├── __init__.py
│   ├── inicializa_pipeline.py
│   ├── treina_avalia_modelos.py
│   └── monitora_drift_faz_predicoes.py
├── models.py
├── utils.py
├── storage/
│   ├── __init__.py
│   ├── storage_base.py
│   └── storage_factory.py
└── tests/
    ├── __init__.py
    └── steps/
        ├── __init__.py
        ├── test_inicializa_pipeline.py
        ├── test_treina_avalia_modelos.py
        └── test_monitora_drift_faz_predicoes.py
