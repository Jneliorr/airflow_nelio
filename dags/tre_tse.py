from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.param import Param, ParamsDict
from sqlalchemy import create_engine, VARCHAR, Integer,Date
import os
import pandas as pd
from datetime import datetime
from operators.unzipNelio import UnzipFilesOperator
import numpy as np
from airflow.operators.bash import BashOperator

params = {
        "Usuario": Param(
        default="José Nélio"
        ,type='string'
        ,enum=["José Nélio"]
        ,description="""
        Escolher usuario 
        """
        )
    ,'database': Param(
        default='mydatabase'
        ,type="string"
        ,description="Nome da database para subir ao postgres")
    ,'user': Param(
        default='postgres'
        ,type="string"
        ,description="Nome do user para subir ao postgres")
    ,'password': Param(
        default='postgres'
        ,type="string"
        ,description="Password para subir ao postgres")
    ,'zip_path': Param(
        default='/opt/airflow/TRE2026/DADOS/RJ/perfil_eleitorado_secao/zip'
        ,type="string"
        ,description="Password para subir ao postgres")
    ,'unzip_path': Param(
        default='/opt/airflow/TRE2026/DADOS/RJ/perfil_eleitorado_secao/csv'
        ,type="string"
        ,description="Password para subir ao postgres")
    }


default_args = {
    "owner": "Nelio Cruel",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="TRE",
    start_date=datetime(2024, 1, 1),
    schedule="@once", 
    params=params,
    catchup=False,
    tags=["eleicao"],
    max_active_tasks= 30,
    default_args=default_args
)


def tre():

    unzip_files = UnzipFilesOperator(
        task_id="unzip_files",
        zip_path="{{ params.zip_path }}",
        file_save="/tmp/unzipped_files"
    )

    # Exemplo de tarefa vazia para representar o próximo passo
    next_step = EmptyOperator(task_id="next_step")

    unzip_files >> next_step

tre_dag = tre()