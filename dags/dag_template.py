from __future__ import annotations
import datetime

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# 1. Default Arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

# 2. Context Manager
with DAG(
    dag_id="meu_primeiro_template_dag",
    default_args=default_args,
    description="Um DAG de exemplo seguindo boas práticas",
    schedule=datetime.timedelta(days=1),
    start_date=datetime.datetime(2023, 1, 1),
    catchup=False,
    tags=["exemplo", "melhores_praticas"],
) as dag:
    # 3. Definição das Tarefas
    tarefa_a = BashOperator(
        task_id="iniciar_processo",
        bash_command="echo 'Iniciando o ETL'",
    )

    tarefa_b = BashOperator(
        task_id="processar_dados",
        bash_command="echo 'Processando a partição {{ ds }}'", # Uso de template Jinja
    )

    tarefa_c = BashOperator(
        task_id="finalizar_processo",
        bash_command="echo 'Processo concluído'",
    )

    # 4. Definição das Dependências
    tarefa_a >> tarefa_b >> tarefa_c