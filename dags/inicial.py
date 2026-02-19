from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator



default_args = {
    "owner": "Nelio Cruel",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="dag_teste",
    start_date=datetime(2024, 1, 1),
    schedule="@once",  # roda apenas uma vez
    catchup=False,
    tags=["teste", "exemplo"],
    max_active_tasks= 30,
    default_args=default_args
)


def teste_dag():
    
    @task
    def tarefa_hello():
        print("Airflow com decorators está funcionando!")
    @task
    def tarefa_world():
        print("Segunda tarefa executada!")


    tarefa_hello() >> tarefa_world()

teste_dag()