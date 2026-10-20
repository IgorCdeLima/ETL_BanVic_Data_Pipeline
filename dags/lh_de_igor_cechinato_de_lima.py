from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import pendulum
from scripts.data_lake import DataLake
from scripts.postgre_connection import PostgreConnection
from scripts.csv_loader import CsvLoader
from scripts.table_loader import TableLoader


def connection_bd():

    with PostgreConnection() as conn:
        print("Verify to connection")
    print("Connection to Postgre is already ")
    
def extract_transacoes_csv(ti):
    data = DataLake()
    address_csv = str(data.csv_estractor())
    ti.xcom_push(key='address_csv', value=address_csv)

def extract_tables_sql(ti):
    data = DataLake()
    address_tables = [str(path) for path in data.sql_estractor()]
    ti.xcom_push(key='address_tables', value=address_tables)

def create_transacoes_table():
    with PostgreConnection() as conn:
        table_creater = CsvLoader(conn) 
        table_creater.creater_table_transacoes()
    
def insert_transacoes(ti):
    address_csv = ti.xcom_pull(key='address_csv', task_ids='extract_transacoes_csv')
    with PostgreConnection() as conn:
        csv_insert = CsvLoader(conn)
        csv_insert.table_transacoes_insert(address_csv)

def insert_agencias(ti):
    address_tables = ti.xcom_pull(key='address_tables', task_ids='extract_tables_sql')
    with PostgreConnection() as conn:
        agencias = TableLoader(conn)
        agencias.agencias_table(address_tables[0])

def insert_clientes(ti):
    address_tables = ti.xcom_pull(key='address_tables', task_ids='extract_tables_sql')
    with PostgreConnection() as conn:
        clientes = TableLoader(conn)
        clientes.clientes_table(address_tables[1])

def insert_colaborador_agencia(ti):
    address_tables = ti.xcom_pull(key='address_tables', task_ids='extract_tables_sql')
    with PostgreConnection() as conn:
        colaborador_agencia = TableLoader(conn)
        colaborador_agencia.colaborador_agencia_table(address_tables[2])
    
def insert_colaboradores(ti):
    address_tables = ti.xcom_pull(key='address_tables', task_ids='extract_tables_sql')
    with PostgreConnection() as conn:
        colaboradores = TableLoader(conn)
        colaboradores.colaboradores_table(address_tables[3])

def insert_contas(ti):
    address_tables = ti.xcom_pull(key='address_tables', task_ids='extract_tables_sql')
    with PostgreConnection() as conn:
        contas = TableLoader(conn)
        contas.contas_table(address_tables[4])
    
def insert_proposta_credito(ti):
    address_tables = ti.xcom_pull(key='address_tables', task_ids='extract_tables_sql')
    with PostgreConnection() as conn:
        proposta_credito = TableLoader(conn)
        proposta_credito.propostas_credito_table(address_tables[5])
    

        

with DAG(
    dag_id='lh_de_igor_cechinato_de_lima',
    start_date=pendulum.datetime(2025,1,1),
    schedule= "35 7 * * *",
    catchup=False, 
    max_active_runs= 1,
    tags=['benvic']
) as dag:
    
    start = EmptyOperator(task_id= "start_stract")
            
    extract_csv = PythonOperator(
        task_id= 'extract_transacoes_csv',
        python_callable = extract_transacoes_csv
    )

    extract_sql = PythonOperator(
        task_id = 'extract_tables_sql',
        python_callable = extract_tables_sql
    )

    postgree_connection = PythonOperator(
        task_id = 'postgree_connection',
        python_callable = connection_bd
    )

    transacoes_table = PythonOperator(
        task_id = 'create_transacoes_table',
        python_callable = create_transacoes_table
    )

    transacoes_insert = PythonOperator(
        task_id = 'insert_transacoes',
        python_callable = insert_transacoes
    )
    agencias_insert = PythonOperator(
        task_id = 'insert_agencias',
        python_callable = insert_agencias
    )
    clientes_insert = PythonOperator(
        task_id = 'insert_clientes',
        python_callable = insert_clientes
    )
    colaborador_agencia_insert = PythonOperator(
        task_id = 'insert_colaborador_agencia',
        python_callable = insert_colaborador_agencia
    )
    colaboradores_insert = PythonOperator(
        task_id = 'insert_colaboradores',
        python_callable = insert_colaboradores
    ) 
    contas_insert = PythonOperator(
        task_id = 'insert_contas',
        python_callable = insert_contas
    )
    proposta_credito_insert = PythonOperator(
        task_id = 'insert_proposta_credito',
        python_callable = insert_proposta_credito
    )

    end = EmptyOperator(task_id="end_extract")

start >> [extract_csv, extract_sql] >> postgree_connection

postgree_connection>> transacoes_table >> transacoes_insert >> end

postgree_connection >> [agencias_insert, clientes_insert, colaborador_agencia_insert, colaboradores_insert, contas_insert, proposta_credito_insert]  >> end
    