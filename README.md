# Desafio BanVic – Engenharia de Dados  

Este projeto implementa uma **pipeline de dados** orquestrada com **Apache Airflow**, responsável por extrair, transformar e carregar (ETL) dados de múltiplas fontes em um **Data Warehouse PostgreSQL**.  

O desafio simula o fluxo completo de ingestão de dados para análises, garantindo:  

- Extração de dados de arquivos CSV e de um banco PostgreSQL de origem  
- Organização em camadas (*staging* e *metadata*)  
- Idempotência via controle de batches  
- Criação automática de tabelas no *staging*  
- Orquestração e agendamento de tarefas com Airflow  
- Persistência de dados em containers Docker (Fonte, DW e Airflow)  

---

## 🚀 Como executar o projeto

### ✅ Pré-requisitos
- Docker 20+    
- Git  

### 🔧 Passo a passo

```bash
# Clone o repositório ou baixe o zip e extraia os arquivos
git clone https://github.com/brunnope/Desafio-BanVic---Engenharia-de-Dados.git

# Vá para a página do projeto
cd desafio-banvic

# Suba os serviços (Airflow, banco fonte, DW)
docker-compose up -d --build

# Inicialize o Airflow por comando ou via web
docker-compose up airflow-init

# Acesse o Airflow
http://localhost:8080
# Usuário: admin / Senha: admin
```

---

## 📌 Estrutura da Pipeline

- **Task 1 – prepare_dirs** <br>
Cria diretórios de extração para a data de execução (/opt/airflow/extracted_data/YYYY-MM-DD).

- **Task 2 – extract_csv** <br>
Extrai dados da pasta data/csv para staging.

- **Task 3 – extract_sql** <br>
Extrai dados do banco fonte (Postgres) para staging.

- **Task 4 – load_dw** <br>
Carrega os dados extraídos no Data Warehouse, adicionando _batch_id e _execution_date.

- **Task 5 – cleanup** <br>
Remove diretórios de execução antigos (mantendo apenas 7 dias).

---

## 🔍 Tecnologias Utilizadas

- **Airflow 2.7.1** → Orquestração de tarefas

- **PostgreSQL 16** → Banco fonte e Data Warehouse

- **Pandas** → Manipulação de dados em Python

- **SQLAlchemy** → Conexão com bancos e execução de queries

- **Docker / Docker Compose** → Containerização e isolamento de ambientes

---

## ⚡ Features de Destaque

- **Idempotência garantida** → batches armazenados em dw_metadata.batches

- **Criação automática de tabelas staging**

- **Estrutura de diretórios organizada por data de execução**

- **Configuração simples via Docker Compose**

- **Limpeza automática de execuções antigas**

---

## ✍️ Autor
Desenvolvido por Cicero Brunno das Neves Pereira

📧 Email: cicerobrnn111@gmail.com <br>
🔗 LinkedIn: [brunno-pereira-dev](http://www.linkedin.com/in/brunno-pereira-dev)
