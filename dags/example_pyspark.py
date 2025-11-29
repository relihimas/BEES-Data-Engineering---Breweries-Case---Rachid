from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Importa o PySpark
# Certifique-se de que o PySpark está instalado no ambiente do Airflow: pip install pyspark
from pyspark.sql import SparkSession

def run_pyspark_job():
    """
    Função que será executada pelo PythonOperator e contém a lógica PySpark.
    """
    print("Iniciando a sessão Spark LOCAL...")
    
    # 1. Cria a SparkSession (em modo local [*])
    spark = SparkSession.builder \
        .appName("LocalPySparkJob") \
        .master("local[*]") \
        .getOrCreate()
    
    # 2. Lógica PySpark: Cria um DataFrame simples
    data = [("Mesa", 10), ("Cadeira", 25), ("Luminária", 5)]
    columns = ["Produto", "Quantidade"]
    df = spark.createDataFrame(data, columns)
    
    # 3. Executa uma transformação/visualização
    print("\n--- DataFrame Criado ---")
    df.show()
    
    total_quantity = df.agg({"Quantidade": "sum"}).collect()[0][0]
    print(f"\nQuantidade total de itens: {total_quantity}")
    
    # 4. Encerra a SparkSession
    spark.stop()
    print("Sessão Spark encerrada.")


# --- Definição do DAG do Airflow ---

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='pyspark_local_python_operator',
    default_args=default_args,
    description='Executa PySpark localmente usando PythonOperator',
    schedule=None,
    catchup=False,
    tags=['spark', 'local'],
) as dag:
    
    execute_local_spark = PythonOperator(
        task_id='execute_local_pyspark',
        python_callable=run_pyspark_job, # Chama a função que contém o código PySpark
        dag=dag,
    )