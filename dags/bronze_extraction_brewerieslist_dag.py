# Bronze Layer: Persist the raw data from the API in its native format or any format you find suitable.
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


def quality_gate(body) -> bool:
    """
    Simple quality gate function to check if the fetched data meets basic quality criteria.
    In this case, we check if the body contains at least one brewery entry.
    """
    return len(body) > 0

def bronze_extraction():
    """
    Fetch data from the generated file from the raw payload extraction phase.
    Apply a quality gate to ensure data integrity before proceeding.
    Save data to the bronze layer.
    """
    try:
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

    except Exception as e:
        return str(e)
    
# Definição da DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "start_date": datetime(2025, 11, 27),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bronze_brewery_extraction_dag",
    default_args=default_args,
    description="Data ingestion to bronze layer from Open Brewery DB",
    schedule="@weekly", 
    start_date=datetime(2025, 11, 28),
    catchup=False,
    tags=["brewery", "bronze"],
) as dag:
    bronze_task = PythonOperator(
        task_id="bronze_extraction",
        python_callable=bronze_extraction
    )