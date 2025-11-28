
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import math
import json
import uuid_utils as uuid
import os
import constants as cst

def raw_extraction():
    """
    Fetch data from the Open Brewery DB API and return a JSON-like dict containing both metadata and the raw payload.
    """
    lst_breweries = []
    file_name = f"bees_listbreweries_{datetime.now().date()}.json"

    try:
        response_metadata = requests.get(cst.url_metadata)
        if response_metadata.status_code == 200:
            total_amount = response_metadata.json()["total"]
        else:
            raise requests.exceptions.HTTPError(f"Status code inválido: {response_metadata.status_code}")

        total_page = math.ceil(total_amount / 200) + 1

        for i in range(1, total_page):
            params = {"page": i, "per_page": 200}
            response_breweries = requests.get(cst.url_lstbreweries, params=params)

            if response_breweries.status_code == 200:
                breweries = response_breweries.json()
                lst_breweries.extend(breweries)
            else:
                raise requests.exceptions.HTTPError(f"Status code inválido: {response_breweries.status_code}")

        rawjson = {
            "file_name": file_name,
            "total_amount_breweries": total_amount,
            "source_endpoint": f"{cst.url_lstbreweries}",
            "source_query": f"page={total_page}&per_page=200",
            "creation_timestamp": datetime.now().timestamp(),
            "batch_id": str(uuid.uuid7()),
            "bronze_target": "breweries_bronze",
            "body": lst_breweries
        }

        with open(file_name, "w", encoding="utf-8") as f:
            json.dump(rawjson, f, ensure_ascii=False, indent=2)

        return f"Arquivo salvo em {file_name}"

    except Exception as e:
        return str(e)


# Definição da DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="raw_brewery_extraction_dag",
    default_args=default_args,
    description="DAG para extrair dados da Open Brewery DB",
    schedule="@weekly", 
    start_date=datetime(2025, 11, 27),
    catchup=False,
    tags=["brewery", "raw"],
) as dag:
    extract_task = PythonOperator(
        task_id="raw_extract_breweries",
        python_callable=raw_extraction
    )
