from airflow.sdk import dag, task
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.param import Param, ParamsDict
from sqlalchemy import create_engine, VARCHAR, Integer,Date
import os
import pandas as pd
from datetime import datetime
import zipfile
import numpy as np
from airflow.operators.bash import BashOperator
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
import os
import requests


default_args = {
    "owner": "Nelio Cruel",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
PATH_DOCKER = "/opt/airflow/cinema2026/data/raw/zip/bilheteria_diaria" 
@dag(
    dag_id="dag_scraping",
    start_date=datetime(2024, 1, 1),
    schedule="@once", 
    # params=params,
    catchup=False,
    tags=["cinema"],
    max_active_tasks= 30,
    default_args=default_args
)


def scraping():

    @task
    def download_file(url,path_save):
        # 1. Garante que o diretório existe
        os.makedirs(path_save, exist_ok=True)        
        file_name = url.split('/')[-1]
        full_path = os.path.join(path_save, file_name)
        print(f"Iniciando download de: {url}")        
        try:
            with requests.get(url, stream=True, timeout=60) as r:
                r.raise_for_status()
                with open(full_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)            
            print(f"Download concluído com sucesso! Salvo em: {full_path}")
            return full_path
        except requests.exceptions.RequestException as e:
            print(f"Erro ao baixar o arquivo: {e}")
            raise


    
    download = download_file("https://dados.ancine.gov.br/dados-abertos/bilheteria-diaria-obras-por-exibidoras-csv.zip",PATH_DOCKER)

    download

scraping_dag = scraping()