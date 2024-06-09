%sql
CREATE EXTERNAL TABLE controle.tbl_controle_esteira_3 (
    -- Projeto
    nome_modal STRING,                          -- Nome do modal
    nome_projeto STRING,                        -- Nome do projeto
    
    -- Execução do Pipeline
    id_pipeline BIGINT,                         -- Identificador único da execução do pipeline
    id_etapa_pipeline BIGINT,                   -- Identificados do etapa atual de excucao
    nome_etapa_pipeline STRING,                 -- Etapa atual da execução do pipeline
    status_execucao_pipeline STRING,            -- Status da execução do pipeline (green, yellow, red)
    data_inicio_etapa_pipeline TIMESTAMP,       -- Data e hora de início da etapa do pipeline
    data_fim_etapa_pipeline TIMESTAMP,          -- Data e hora de término da etapa do pipeline
    resumo_execucao_etapa STRING,               -- Resumo da execucao
    
    -- Modelo e Treinamento/Alaviacao
    nome_modelo STRING,                         -- Nome do modelo
    versao_modelo STRING,                       -- Versão do modelo
    tipo_modelo STRING,                         -- Tipo do modelo (classificação, regressão, etc.)
    status_modelo STRING,                       -- Status do treinamento do modelo
    data_validade_modelo STRING,                -- Data de validade do modelo
    dias_validade_modelo BIGINT,                -- Dias de validade do modelo
    percentual_restante_validade_modelo DOUBLE, -- Percentual de validade restante do modelo
    duracao_treinamento_modelo BIGINT,          -- Duração do treinamento do modelo (em segundos)
    qtd_dados_predicao BIGINT,                  -- Quantidade de linhas usadas na predição
    limiar_minino_acc DOUBLE,                   -- Limiar mínimo da métrica do modelo
    valor_medido_acc DOUBLE,                    --Valor medido da métrica do modelo
    
    -- Retreino
    qtd_dados_treino BIGINT,
    qtd_dados_retreino_01 BIGINT,         -- Quantidade de dados para o primeiro range de retreino
    qtd_dados_retreino_02 BIGINT,         -- Quantidade de dados para o segundo range de retreino
    qtd_dados_retreino_03 BIGINT,         -- Quantidade de dias para o terceiro range de retreino
    passo_etapa_retreino_modelo BIGINT,   -- Etapa do retreino do modelo
    qtd_permitida_retreino BIGINT,        -- Quantidade total de retreinos permitidos
    qtd_medida_retreino BIGINT,           -- Quantidade de retreinos atual

    -- Drift
    limiar_maximo_drift DOUBLE,                -- Valor maximo da metrica de drift
    valor_medido_drift DOUBLE,                 -- Valor medido da metrica de drift
   
    -- Computacao
    nome_cluster_execucao STRING,              -- Nome do cluster de execução
    utilizacao_cpu DOUBLE,                     -- Utilização de CPU 
    utilizacao_gpu DOUBLE,                     -- Utilização de GPU 
    utilizacao_memoria DOUBLE,                 -- Utilização de memória 

    -- Outros
    tipo_esteira BIGINT,                       -- Tipo de esteira
    email_usuario STRING,                      -- Email do usuário responsável
    data_criacao TIMESTAMP                     -- Data de criação do registro
)
USING DELTA
LOCATION '/mnt/gold/MLOPS/tray3/controle/tbl_controle_esteira_3';