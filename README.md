# ETL BanVic Data Pipeline

Pipeline de dados orquestrado com **Apache Airflow 3** que extrai dados de duas fontes (um arquivo **CSV** e um dump **SQL** do PostgreSQL), grava as extrações em um **Data Lake local** (sistema de arquivos) e as carrega em um **Data Warehouse PostgreSQL**. Tudo roda em containers Docker.

> Projeto desenvolvido como solução para o **Desafio de Engenharia de Dados do programa Lighthouse da Indicium** (processo seletivo para estágio, turma 2025-11).

---

## Sumário

- [Contexto](#contexto)
- [Arquitetura](#arquitetura)
- [Requisitos do desafio x implementação](#requisitos-do-desafio-x-implementação)
- [Tecnologias](#tecnologias)
- [Estrutura do projeto](#estrutura-do-projeto)
- [Como executar](#como-executar)
- [A DAG](#a-dag)
- [Módulos Python](#módulos-python)
- [Resultado esperado](#resultado-esperado)
- [Características do pipeline](#características-do-pipeline)
- [Melhorias futuras](#melhorias-futuras)
- [Autor](#autor)

---

## Contexto

O desafio simula o **Banco Vitória S.A. (BanVic)**, um banco fictício fundado em 2010 que quer amadurecer sua cultura de dados. Hoje as análises do banco são feitas manualmente em planilhas. Como primeiro passo, a equipe propõe **centralizar diariamente em um Data Warehouse** os dados que estão espalhados por diferentes fontes (ERP, CRM e marketing), para depois usá-los em ferramentas de BI.

Arquivos fornecidos pelo desafio:

| Arquivo | Conteúdo |
|---|---|
| `transacoes.csv` | ~72 mil transações bancárias (`cod_transacao`, `num_conta`, `data_transacao`, `nome_transacao`, `valor_transacao`) |
| `banvic.sql` | Dump PostgreSQL com 6 tabelas: `agencias`, `clientes`, `colaboradores`, `colaborador_agencia`, `contas` e `propostas_credito` |
| `docker-compose.yml` | Sobe o banco de dados fonte (PostgreSQL 16) |

---

## Arquitetura

```
┌───────────────┐        ┌──────────────────────── Apache Airflow ───────────────────────┐
│ Fontes        │        │                                                               │
│               │ Extr.  │   ┌───────────────────────┐   Carga   ┌──────────────────────┐ │
│ csv/          ├───────►│   │ FileSystem local      │ ────────► │ PostgreSQL           │ │
│ transacoes.csv│        │   │ (Data Lake)           │           │ (Data Warehouse)     │ │
│               │ Extr.  │   │ data/<AAAA-MM-DD>/... │           │ banco "banvic"       │ │
│ sql/          ├───────►│   └───────────────────────┘           └──────────────────────┘ │
│ banvic.sql    │        │                                                               │
└───────────────┘        └───────────────────────────────────────────────────────────────┘
```

1. **Extração (em paralelo):**
   - **CSV:** `transacoes.csv` é copiado para `data/<AAAA-MM-DD>/csv/transacoes.csv`.
   - **SQL:** o `banvic.sql` é lido linha a linha. Cada bloco `COPY ... FROM stdin` vira um CSV em `data/<AAAA-MM-DD>/sql/<tabela>.csv`.
2. **Carga:** só acontece se **as duas extrações** terminarem com sucesso. Os CSVs do Data Lake são inseridos no PostgreSQL com `INSERT ... ON CONFLICT DO NOTHING`, então registros já existentes não são duplicados.

---

## Requisitos do desafio x implementação

| # | Requisito | Situação | Como foi atendido |
|---|---|:---:|---|
| 1 | Usar Apache Airflow 2 ou 3 como orquestrador | ✅ | `apache/airflow:3.0.6` com CeleryExecutor |
| 2 | Extrações idempotentes | ✅ | Cada execução gera uma partição datada e imutável no Data Lake, e a carga usa `ON CONFLICT DO NOTHING`. Detalhes em [Características do pipeline](#características-do-pipeline) |
| 3 | Extrair todos os dados fornecidos | ✅ | CSV completo e as 6 tabelas do dump SQL |
| 4 | Gravar em CSV no padrão `<ano>-<mês>-<dia>/<fonte>/<tabela>.csv` | ✅ | `data/2025-10-27/csv/transacoes.csv`, `data/2025-10-27/sql/agencias.csv`, ... |
| 5 | Extrações em paralelo | ✅ | `start >> [extract_csv, extract_sql]` |
| 6 | Carga só se as duas extrações tiverem sucesso | ✅ | A carga depende das duas extrações (regra padrão `all_success`) |
| 7 | Execução diária às 04:35 | ✅ | `schedule="35 7 * * *"`: o Airflow opera em UTC, e 07:35 UTC corresponde a 04:35 no horário de Brasília (UTC-3) |
| 8 | Projeto reproduzível em outros ambientes | ✅ | Todo o ambiente sobe pelo Docker Compose, com imagens e dependências em versões fixas. Os scripts `setup.bat` / `setup.sh` inicializam o ambiente com um único comando, e o `.gitattributes` garante quebras de linha corretas em qualquer sistema |

---

## Tecnologias

| Ferramenta | Versão | Uso |
|---|---|---|
| Apache Airflow | 3.0.6 | Orquestração (CeleryExecutor + Redis) |
| PostgreSQL | 16 | Banco fonte e Data Warehouse (`db`) |
| PostgreSQL | 13 | Metadados do Airflow (`postgres`) |
| Redis | 7.2 | Broker do Celery |
| Python | 3.12 (imagem do Airflow) | Scripts de extração e carga |
| pandas | 2.1.4 | Leitura e escrita de CSV |
| psycopg2-binary | 2.9.10 | Conexão com o PostgreSQL |
| Docker / Docker Compose | — | Infraestrutura |

---

## Estrutura do projeto

```
ETL_BanVic_Data_Pipeline/
├── config/
│   └── requirements.txt        # Dependências instaladas nos containers do Airflow
├── csv/
│   └── transacoes.csv          # Fonte 1: transações (CSV)
├── sql/
│   └── banvic.sql              # Fonte 2: dump PostgreSQL (também inicializa o banco "db")
├── dags/
│   └── lh_de_igor_cechinato_de_lima.py   # Definição da DAG
├── plugins/
│   └── scripts/
│       ├── info.py                 # Caminhos e credenciais centralizados
│       ├── data_lake.py            # Fachada das extrações (define a data do dia)
│       ├── csv_data_estractor.py   # Extração do CSV
│       ├── table_estractor.py      # Extração das tabelas do banvic.sql
│       ├── postgre_connection.py   # Context manager de conexão com o DW
│       ├── csv_loader.py           # Cria e carrega a tabela transacoes
│       ├── table_loader.py         # Carrega as 6 tabelas do dump
│       ├── sql_estractor.py        # (legado, não usado pela DAG)
│       ├── postgre_loader.py       # (legado, não usado pela DAG)
│       └── exceptions/
│           └── exception_program.py  # Exceção customizada com mensagens por tipo de erro
├── data/                       # Data Lake (gerado pela DAG)
├── logs/                       # Logs do Airflow (gerado em tempo de execução)
├── dbdata/                     # Volume do PostgreSQL "db" (gerado; ignorado pelo git)
├── docker-compose.yaml         # Airflow + PostgreSQL + Redis
├── .gitattributes              # Quebras de linha por tipo de arquivo (LF no .sh, CRLF no .bat)
├── .env                        # AIRFLOW_UID (gerado pelo setup; ignorado pelo git)
├── setup.bat                   # Inicialização no Windows
└── setup.sh                    # Inicialização no Linux/macOS
```

---

## Como executar

### Pré-requisitos

- **Docker Desktop** (Windows/macOS) ou **Docker Engine + Docker Compose** (Linux)
- No Windows, **WSL 2** atualizado (`wsl --update`)
- Pelo menos **4 GB de RAM** e **2 CPUs** livres para o Docker. É o mínimo recomendado pelo Airflow
- Portas livres: **8080** (Airflow) e **55432** (PostgreSQL)

Para conferir a instalação:

```bash
docker --version
docker compose version
```

### 1. Clonar o repositório

```bash
git clone https://github.com/IgorCdeLima/ETL_BanVic_Data_Pipeline.git
cd ETL_BanVic_Data_Pipeline
```

### 2. Subir o ambiente

Com o Docker em execução:

**Windows:** dê dois cliques em `setup.bat` ou rode no terminal:

```bat
setup.bat
```

**Linux/macOS:**

```bash
chmod +x setup.sh
./setup.sh
```

Os scripts:

1. verificam se o Docker está em execução;
2. criam as pastas `logs/` e `data/`;
3. geram o `.env` com o `AIRFLOW_UID`: `50000` no Windows e o UID do usuário atual no Linux/macOS. Sem essa variável, o `airflow-init` grava o `config/airflow.cfg` como root e os demais serviços não conseguem lê-lo;
4. executam o `airflow-init` (migração do banco de metadados e criação do usuário administrador) e aguardam a conclusão;
5. sobem todos os serviços em segundo plano.

Ou manualmente:

```bash
echo "AIRFLOW_UID=50000" > .env
docker compose up airflow-init
docker compose up -d
```

Na primeira subida, as imagens são baixadas e as dependências instaladas, o que pode levar alguns minutos. O container `db` executa o `sql/banvic.sql` automaticamente e cria o banco `banvic`.

### 3. Acessar o Airflow

- URL: **http://localhost:8080**
- Usuário: `airflow`
- Senha: `airflow`

### 4. Executar a DAG

1. Na lista de DAGs, procure **`lh_de_igor_cechinato_de_lima`** (tag `benvic`).
2. Ative a DAG no botão de *toggle*. Ela vem pausada por padrão.
3. Para uma execução manual, clique em **Trigger DAG** (▶).

### 5. Conferir os resultados

**Data Lake:** os arquivos aparecem em `data/<AAAA-MM-DD>/`.

**Data Warehouse:** conecte-se ao PostgreSQL com qualquer cliente (DBeaver, pgAdmin, psql):

| Parâmetro | Valor |
|---|---|
| Host | `localhost` |
| Porta | `55432` |
| Banco | `banvic` |
| Usuário | `data_engineer` |
| Senha | `v3rysecur&pas5w0rd` |

```sql
SELECT COUNT(*) FROM public.transacoes;
SELECT COUNT(*) FROM public.contas;
```

### Encerrar o ambiente

```bash
docker compose down        # para os containers
docker compose down -v     # para os containers e apaga os volumes do Airflow
```

Para recriar o banco `banvic` do zero, apague também a pasta `dbdata/`.

---

## A DAG

**ID:** `lh_de_igor_cechinato_de_lima` · **Tag:** `benvic` · **Agendamento:** `35 7 * * *` (todos os dias às 04:35, horário de Brasília) · **catchup:** `False` · **max_active_runs:** `1`

```
                    ┌─► extract_transacoes_csv ─┐
start_stract ───────┤                           ├─► postgree_connection ─┬─► create_transacoes_table ─► insert_transacoes ─┐
                    └─► extract_tables_sql ─────┘                        ├─► insert_agencias ─────────────────────────────────┤
                                                                         ├─► insert_clientes ─────────────────────────────────┤
                                                                         ├─► insert_colaborador_agencia ──────────────────────┼─► end_extract
                                                                         ├─► insert_colaboradores ────────────────────────────┤
                                                                         ├─► insert_contas ───────────────────────────────────┤
                                                                         └─► insert_proposta_credito ─────────────────────────┘
```

| Task | O que faz |
|---|---|
| `start_stract` | Marca o início (`EmptyOperator`) |
| `extract_transacoes_csv` | Copia `transacoes.csv` para o Data Lake e publica o caminho via XCom |
| `extract_tables_sql` | Converte os 6 blocos `COPY` do `banvic.sql` em CSVs e publica os caminhos via XCom |
| `postgree_connection` | Testa a conexão com o DW. Só roda se as duas extrações tiverem sucesso |
| `create_transacoes_table` | Cria `public.transacoes` (PK `cod_transacao`, FK `num_conta → contas`) se ela ainda não existir |
| `insert_transacoes` | Carrega o CSV de transações no DW |
| `insert_<tabela>` | Carrega cada uma das 6 tabelas do dump no DW, em paralelo |
| `end_extract` | Marca o fim (`EmptyOperator`) |

A comunicação entre extração e carga é feita por **XCom**: as tasks de extração publicam os caminhos dos arquivos gerados e as de carga os consomem.

---

## Módulos Python

| Módulo | Responsabilidade |
|---|---|
| `info.py` | Centraliza os caminhos (`csv/`, `sql/`, `data/`) e os parâmetros de conexão do DW. O diretório base pode ser sobrescrito pela variável `PIPELINE_BASE_DIR` |
| `data_lake.py` | Classe `DataLake`: define a data do dia e chama os extratores |
| `csv_data_estractor.py` | `CsvDataEstractor.bkp_csv()`: copia o CSV para a partição do dia no Data Lake e, concluída a cópia, esvazia o arquivo de origem, deixando só o cabeçalho |
| `table_estractor.py` | `SqlEstractor.data_search()`: faz o parsing do dump, identifica cada `COPY ... FROM stdin` até o marcador `\.` e grava um CSV por tabela |
| `postgre_connection.py` | `PostgreConnection`: context manager que abre a conexão, faz rollback em caso de erro e sempre fecha a conexão |
| `csv_loader.py` | `CsvLoader`: cria a tabela `transacoes` com PK/FK e insere os registros com `ON CONFLICT DO NOTHING` |
| `table_loader.py` | `TableLoader`: um método de carga por tabela (`agencias`, `clientes`, `colaborador_agencia`, `colaboradores`, `contas`, `propostas_credito`) |
| `exceptions/exception_program.py` | `ExceptionProgram`: traduz exceções (arquivo, SQL, conexão, integridade...) em mensagens padronizadas |

---

## Resultado esperado

Depois de uma execução bem-sucedida:

```
data/
└── 2025-10-27/
    ├── csv/
    │   └── transacoes.csv
    └── sql/
        ├── agencias.csv
        ├── clientes.csv
        ├── colaborador_agencia.csv
        ├── colaboradores.csv
        ├── contas.csv
        └── proposta_credito.csv
```

No PostgreSQL (`banvic`), a tabela `public.transacoes` é criada e populada, e as demais tabelas recebem os registros que ainda não existiam.

---

## Características do pipeline

### CSV consumido e transformado em Data Lake

O `csv/transacoes.csv` funciona como uma **área de entrada** (*landing zone*): o arquivo é renovado diariamente às 04:00, e a DAG roda às 04:35 (horário de Brasília) para consumi-lo.

1. O conteúdo do arquivo é copiado para a partição do dia em `data/<AAAA-MM-DD>/csv/transacoes.csv`.
2. Só **depois que a cópia termina com sucesso**, o arquivo de origem é esvaziado e mantém apenas o cabeçalho, pronto para receber o lote do dia seguinte.
3. Se a cópia falhar, o arquivo de origem não é tocado e nenhum dado se perde.

O histórico completo fica no Data Lake, organizado por data, e cada lote diário é processado uma única vez.

> Para executar a DAG mais de uma vez em ambiente de testes, restaure o arquivo de origem com `git checkout -- csv/transacoes.csv`.

### Idempotência

- **Partições imutáveis:** cada execução grava em uma partição identificada pela data (`data/<AAAA-MM-DD>/`). Uma partição já gravada nunca é sobrescrita: uma nova extração para a mesma data é recusada, o que preserva os dados já consolidados.
- **Carga sem duplicidade:** todas as inserções no Data Warehouse usam `INSERT ... ON CONFLICT DO NOTHING` sobre as chaves primárias. Recarregar os mesmos arquivos não duplica nenhum registro.
- **Criação condicional de tabela:** `public.transacoes` e suas restrições (PK e FK) só são criadas se a tabela ainda não existir.

### Baixo consumo de memória

Os CSVs são lidos em *chunks* (`pd.read_csv(..., chunksize=...)`) e inseridos de forma incremental. O arquivo nunca é carregado inteiro em memória, então o pipeline roda até em máquinas com poucos recursos.

---

## Melhorias futuras

1. **Banco fonte separado do Data Warehouse:** hoje o container `db` cumpre os dois papéis. Ele é inicializado com o `banvic.sql`, então as 6 tabelas do dump já existem antes da carga. Uma evolução natural é ter um PostgreSQL só para o Data Warehouse e extrair a fonte SQL por consulta ao banco.
2. **Performance da carga:** aumentar o tamanho do chunk (hoje `CHUNKSIZE = 1`) e inserir em lote com `psycopg2.extras.execute_values`. Assim se mantém o baixo uso de memória e se reduz o número de viagens ao banco.
3. **Credenciais fora do código:** mover usuário e senha do DW, hoje fixos em `info.py`, para *Connections* do Airflow ou variáveis de ambiente.
4. **Remoção de código legado:** `sql_estractor.py` e `postgre_loader.py` são versões anteriores dos extratores e loaders e não são usados pela DAG.
5. **Padronização de nomes:** alinhar `proposta_credito.csv` com o nome da tabela `propostas_credito`.

---

## Autor

**Igor Cechinato de Lima**
GitHub: [@IgorCdeLima](https://github.com/IgorCdeLima)
