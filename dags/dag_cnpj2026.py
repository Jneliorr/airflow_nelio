"""
Dag de Pipeline cnpj_download_complet: download completo de dados de CNPJ iniciando todo dia 20 de cada mes as 6h da manhã se não achar vai repetir todo o dia ate achar e recomeçar com 1 mes depois
"""

# ─── Core do Airflow ─────────────────────────────────────────────────────────────
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# ─── Provedor Google no Airflow ─────────────────────────────────────────────────
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from cosmos.profiles import PostgresUserPasswordProfileMapping
# ─── COSMOS ───────────────────────────────────────────
from cosmos import ProjectConfig, ProfileConfig, DbtTaskGroup, RenderConfig

# ─── Clientes oficiais do Google Cloud ───────────────────────────────────────────
from google.cloud import storage

# ─── Plugins personalizados ───────────────────────────────────────────────────────
from plugins.auxi.cnpj_functions import (convert_date, remove_table, get_path_files)
from plugins.operators.unzip import UnzipGCS


# ─── Bibliotecas de terceiros ─────────────────────────────────────────────────────
import pandas as pd
import pandas_gbq as bq
import requests
import pendulum
from kubernetes.client import models as k8s  


# ─── Biblioteca padrão do Python ─────────────────────────────────────────────────
from datetime import datetime, timedelta
import logging
import os
import re
import urllib.parse
from pathlib import Path


#######CONEXÕES E CREDENCIAIS###########
conn_id = "gcs_default"
hook = GCSHook(gcp_conn_id=conn_id)
credentials = hook.get_credentials()
# Fuso de São Paulo
BRT = pendulum.timezone("America/Sao_Paulo")
########################################


# Configuração de onde o dbt está no seu Docker/Worker
DBT_PROJECT_PATH = Path("/opt/airflow/dbt/cnpj2026")

profile_config = ProfileConfig(
    profile_name="cnpj2026",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_default", # O ID da conexão que você cria no Airflow UI
        profile_args={"schema": "public"},
    ),
)


"""
Definindo as colunas de cada tabela do CNPJ
"""

colunas_estabelecimento = ['CNPJ_BASICO',
                        'CNPJ_ORDEM',
                        'CNPJ_DV',
                        'MATRIZ_FILIAL',
                        'NOME_FANTASIA',
                        'SITUACAO_CADASTRAL',
                        'DATA_SITUACAO_CADASTRAL',
                        'MOTIVO_SITUACAO',
                        'NOME_CIDADE_EXT',
                        'PAIS',
                        'Data_Inicio_Atividade',
                        'CNAE_PRINCIPAL',
                        'CNAE_SECUNDARIA',
                        'TIPO_LOGRADDOURO',
                        'LOGRADOURO',
                        'NUM',
                        'COMPLEMENTO',
                        'BAIRRO',
                        'CEP',
                        'UF',
                        'MUNICIPIO',
                        'DDD1',
                        'TEL1',
                        'DDD2',
                        'TEL2',
                        'DDD_FAX',
                        'TEL_FAX',
                        'E_MAIL',
                        'SITUACAO_ESPECIAL',
                        'DATA_SIT_ESPECIAL'
                        ]

colunas_empresa = [
                'CNPJ_BASICO',
                'RAZAO_SOCIAL',
                'COD_NATUREZA_JUR',
                'QUALIFICACAO_RESPONSAVEL',
                'CAPITAL_SOCIAL',
                'COD_PORTE_EMPRESA',
                'ENTE_RESPONSAVEL'
            ]

colunas_simples = ['CNPJ_BASICO', 
                   'OPCAO_PELO_SIMPLES', 
                   'DATA_OPCAO_SIMPLES', 
                   'DATA_EXCLUSAO_DO_SIMPLES',
                   'OPCAO_PELO_MEI',
                   'DATA_OPCAO_MEI',
                   'DATA_EXCLUSAO_DO_MEI']

colunas_socio = ['CNPJ_BASICO'
        ,'COD_TIPO_SOCIO'
        ,'NOME_SOCIO'
        ,'CNPJ_CPF_SOCIO'
        ,'COD_QUALI_SOCIO'
        ,'DATA_ENT_SOCIEDADE'
        ,'PAIS'
        ,'REPRESENTANTE_LEGAL_CPF'
        ,'NOME_REPRESENTANTE'
        ,'COD_QUALI_REPRESENTANTE'
        ,'COD_FAIXA_ETARIA'
        ]


pod_override = k8s.V1Pod(
    metadata=k8s.V1ObjectMeta(
        labels={"workload": "airflow-etl"}
    ),
    spec=k8s.V1PodSpec(
        node_selector={"cloud.google.com/gke-nodepool": "pool-airflow-workers"},

        tolerations=[
            k8s.V1Toleration(
                key="airflow-only",
                operator="Equal",
                value="true",
                effect="NoSchedule"
            )
        ],
        affinity=k8s.V1Affinity(
            node_affinity=k8s.V1NodeAffinity(
                required_during_scheduling_ignored_during_execution=k8s.V1NodeSelector(
                    node_selector_terms=[
                        k8s.V1NodeSelectorTerm(
                            match_expressions=[
                                k8s.V1NodeSelectorRequirement(
                                    key="cloud.google.com/gke-provisioning",
                                    operator="In",
                                    values=["preemptible"]
                                )
                            ]
                        )
                    ]
                )
            )
        ),
        containers=[
            k8s.V1Container(
                name="airflow-custom-k8s",
                image="us-central1-docker.pkg.dev/infra-itaborai/registry-ita/airflow-custom:1.4.4",
                resources=k8s.V1ResourceRequirements(
                    requests={"cpu": "2000m", "memory": "4Gi"},
                    limits={"cpu": "6000m", "memory": "30Gi"}
                )
            )
        ]
    )
)


default_args = {
    "owner": "Nelio",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
    
""" 
Definindo nome, e parâmetros de verificação mensal da dag
"""        
@dag(

dag_id='cnpj2026',
start_date=BRT.datetime(2025, 6, 17, 6, 00),
schedule_interval="0 6 20 * *",
catchup=False,
tags=["DEV", "CNPJ", "DOWNLOAD"],
max_active_tasks= 30,
default_args=default_args,
)
    
def cnpj2026():
    """ Função principal do DAG cnpj_download_complet. Realiza o download dos dados do CNPJ, descompacta os arquivos, processa-os e carrega os dados no BigQuery."""

    download_files = {
    'empresas':  [f'Empresas{i}.zip' for i in range(10)],
    'estabelecimentos': [f'Estabelecimentos{i}.zip' for i in range(10)],
    'socios': [f'Socios{i}.zip' for i in range(10)],
    'dimensoes': ['Cnaes.zip', 'Municipios.zip', 'Naturezas.zip', 'Paises.zip', 'Qualificacoes.zip','Simples.zip'],
    'regimes': ['https://arquivos.receitafederal.gov.br/public.php/dav/files/MPPfFit7g7zdA8C/entidades-lucro-real.zip',
    'https://arquivos.receitafederal.gov.br/public.php/dav/files/MPPfFit7g7zdA8C/entidades-lucro-presumido.zip',
    'https://arquivos.receitafederal.gov.br/public.php/dav/files/MPPfFit7g7zdA8C/entidades-lucro-arbitrado.zip'
    ]
    }

    @task()
    def periodo_apuracao_PA():
        pa = datetime.now(BRT).strftime("%Y-%m")
        logging.info(f"Período de apuração definido como: {pa}")
        return pa #pa  # Exemplo de período fixo, pode ser modificado conforme necessário

    # função checadora chamada pelo PythonOperator
    def check_site_url(pa: str, **context):
        webdav_url = f"https://arquivos.receitafederal.gov.br/public.php/dav/files/YggdBLfdninEJX9/{pa}/"
        resp = requests.request("PROPFIND", webdav_url, headers={"Depth": "1"}, timeout=30, verify=False)
        resp.raise_for_status()
        qtd_arquivos = len(re.findall(r"<(?:d|D):response", resp.text)) - 1
        if qtd_arquivos < 10:
            print(f"Dados disponíveis para {pa}: {qtd_arquivos} arquivos.")
        return True

    @task (task_id='download_cnpj_DEV')
    def task_download_cnpj(PA:str, categoria:str, bucket_name:str,arquivo:str):

        url = f"https://arquivos.receitafederal.gov.br/public.php/dav/files/YggdBLfdninEJX9/{PA}/{arquivo}"
        logging.info(f'URL: {url}')
        caminho = f"raw/cnpj/{PA}/{categoria}/{arquivo}"
        logging.info(f'Caminho: {caminho}')
        response = requests.get(url, stream=True, timeout=300, verify=False)
        response.raise_for_status()
        with open(arquivo, "wb") as file:
            for chunk in response.iter_content(chunk_size=1024*1024):
                file.write(chunk)
        client= storage.Client(credentials=credentials)
        logging.info(f'Cliente: {client}')
        logging.info(f'Bucket: {bucket_name}')
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(caminho)
        blob.upload_from_filename(arquivo)
        print(f'Arquivo {arquivo} enviado com sucesso para o GCS em {caminho}.')

    @task(task_id="download_regimes")
    def task_download_regimes(PA: str, bucket_name: str, categoria: str):

        client = storage.Client(credentials=credentials)
        bucket = client.bucket(bucket_name)

        url = categoria
        logging.info(f"Iniciando download de {url}")
        resp = requests.get(url, stream=True, timeout=300, verify=False)
        resp.raise_for_status()
        raw_name = os.path.basename(url)
        logging.info(f"Nome do arquivo: {raw_name}") #arquivo original
        nome_limpo = urllib.parse.unquote(raw_name)
        nome_sem_extensao, _ = os.path.splitext(nome_limpo) #retirado a extensão do arquivo
        logging.info(f"Nome sem extensão: {nome_sem_extensao}") #retirado o caracteres especiais %
        local_filename = re.sub(r'[^A-Za-z0-9]', '', nome_sem_extensao) + ".zip" #retirado os caracteres especiais do decoded 
        logging.info(f"Salvando arquivo localmente como: {local_filename}")
        with open(local_filename, "wb") as f:
            for chunk in resp.iter_content(1024 * 1024):
                f.write(chunk)
        object_path = f"raw/cnpj/{PA}/regimes/{local_filename}"
        blob = bucket.blob(object_path)
        blob.upload_from_filename(local_filename)
        logging.info(f"Enviado para gs://{bucket_name}/{object_path}")

    """ Função para carregar os dados do RFB no BigQuery """
    @task
    def load_rfb(path_file, serivce ,cloud=False, clean_tb=True, bucket=None, projeto='infra-itaborai', dataset='mds_cnpj'):
        
        if serivce == 'est':
            table = 'estabelecimento'
            cols = colunas_estabelecimento

        elif serivce == 'emp':
            table = 'empresa'
            cols = colunas_empresa
            
        elif serivce == 'sn':
            table = 'simples_nacional'
            cols = colunas_simples
            
        elif serivce == 'soc':
            table = 'socios'
            cols = colunas_socio

        files = get_path_files(path=path_file, cloud=cloud,bucket=bucket)    
        print(files)
        
        if clean_tb:
            remove_table(projeto, dataset, table)

        for file in files:
            df = pd.read_csv(file,
                            sep=';',
                            encoding="ISO-8859-1",
                            names=cols,
                            dtype=str
                            )        
            df = convert_date(df)
        
            bq.to_gbq(df,
                f"{dataset}.{table}",
                project_id=projeto,
                if_exists='append',
                credentials=credentials
                )
            print(f'arquivo {file} carregado para {table} no dataset {dataset}')
        
    @task
    def load_dim(path_file, cloud=False, clean_tb=True, bucket=None, projeto='infra-itaborai', dataset='mds_cnpj'):
        
        #dims = os.listdir(path_file)
        files = get_path_files(path=path_file, cloud=cloud,bucket=bucket)    
        print(f"teste impressao {files}") 
            
        for path in files:
            service = path.split('/')[-1].split('.')[0]
            print(service)
            
            """ Verifica qual tabela deve ser carregada com base no nome do arquivo """
            if service.lower() == 'cnaes':
                table = 'd_cnae'
                cols = ['id', 'descricao']

            elif service.lower() == 'moti':
                table = 'd_motivo'
                cols = ['id', 'descricao']
                
            elif service.lower() == 'municipios':
                table = 'd_municipio'
                cols = ['id', 'descricao']
                
            elif service.lower() == 'naturezas':
                table = 'd_natjuridica'
                cols = ['id', 'descricao']
                
            elif service.lower() == 'paises':
                table = 'd_pais'
                cols = ['id', 'descricao']
                
            elif service.lower() == 'qualificacoes':
                table = 'd_qualifsocio'
                cols = ['id', 'descricao']
            
            elif service.lower() == 'simples':
                table = 'simples_nacional'
                cols = colunas_simples
            
            
            if clean_tb:
                remove_table(projeto, dataset, table)

            print( f"teste impressao {path}" )
            df = pd.read_csv(path,
                            sep=';',
                            encoding="latin1",
                            names=cols,
                            dtype=str
                            )
            print(f"Arquivo lido e preparando para o bigquery:{path}")
            bq.to_gbq(df,
                f"{dataset}.{table}",
                project_id=projeto,
                if_exists='replace',
                credentials=credentials
                )
            print(f'arquivo {service} carregado')
        

    

    """ Arquitetando o funcionamento da DAG, iniciando com o EmptyOperator e definindo o fluxo de tarefas """
        
# ───Definindo Fluxo───────────────────────────────────────
    start = EmptyOperator(task_id = 'start')
    pa = periodo_apuracao_PA()

    check_site_task = PythonOperator(
        task_id="check_site",
        python_callable=check_site_url,
        op_args=["{{ ti.xcom_pull(task_ids='periodo_apuracao_PA') }}"],
        retries=10,                         # por ex., tenta o numero de vezes até conseguir
        retry_delay=timedelta(days=1),#(days=1),      # espera 1 dia entre cada retry
    )
    """ Task para baixar os arquivos do CNPJ  e separar por tópicos """
    empresas = task_download_cnpj.override(task_id="download_empresas").partial(PA=pa,categoria='empresas',bucket_name="k8s-dataita").expand(arquivo=download_files['empresas'])
    estabelecimento = task_download_cnpj.override(task_id="download_estabelecimento").partial(PA=pa, categoria='estabelecimentos',bucket_name="k8s-dataita").expand(arquivo=download_files['estabelecimentos'])
    socios = task_download_cnpj.override(task_id="download_socios").partial(PA=pa, categoria='socios',bucket_name="k8s-dataita").expand(arquivo=download_files['socios'])
    dimensoes = task_download_cnpj.override(task_id="download_dimensoes").partial(PA=pa, categoria='dimensoes',bucket_name="k8s-dataita").expand(arquivo=download_files['dimensoes'])
    regimes = task_download_regimes.override(task_id="download_regimes").partial(PA=pa, bucket_name="k8s-dataita").expand(categoria=download_files['regimes'])


    unzip_files = UnzipGCS(
    task_id='unzip_files',
    bucket_name= "k8s-dataita",
    prefix= f'raw/cnpj/{pa}/estabelecimentos',
    executor_config={"pod_override": pod_override}
    )



    """ Pega os arquivos descompactados em raw/cnpj/{pa}/ de empresas, socios e dimensoes e carrega no BigQuery  em mds_cnpj nas tabelas empresa, socios e dimensoes """
    etl_empresas = load_rfb.override(task_id="etl_empresas")(path_file=f'raw/cnpj/{pa}/empresas/', serivce='emp',cloud=True, clean_tb=True, bucket='k8s-dataita', dataset="mds_cnpj")
    etl_socios = load_rfb.override(task_id="etl_socios")(path_file=f'raw/cnpj/{pa}/socios/', serivce='soc',cloud=True, clean_tb=True, bucket='k8s-dataita', dataset="mds_cnpj")
    etl_dims = load_dim.override(task_id="etl_dims")(path_file=f'raw/cnpj/{pa}/dimensoes', cloud=True, clean_tb=True, bucket='k8s-dataita', dataset="mds_cnpj")
        
    """ Pega os arquivos descompactados em raw/cnpj/{pa}/ de estabelecimentos e carrega no BigQuery  em mds_cnpj na tabela estabelecimento usando Spark no Kubernetes """
    etl_estab = SparkKubernetesOperator(
        task_id='etl_estabelecimento',
        kubernetes_conn_id='kubernetes_default',
        namespace='spark-jobs',
        application_file='/util/spark/spark-app-estabelecimentos.yaml',
        do_xcom_push=False
    )


# ─── RUN DBT ────────────────────

    dbt_run_task = DbtTaskGroup(
            group_id="camada_transformacao_dbt",
            project_config=ProjectConfig(DBT_PROJECT_PATH),
            profile_config=profile_config,
            render_config=RenderConfig(
                select=["path:models/cinema"]
            ),
            operator_args={"install_deps": True},
        )

    """ Task para mover os arquivos descompactados para a pasta de arquivos e deletar os temporários """    
    delete_temp_files = GCSDeleteObjectsOperator(
    task_id="delete_temp_files",
    bucket_name="k8s-dataita",
    prefix=f'tmp/raw/CNPJ/waiting/',
    gcp_conn_id=conn_id,
)
        

    end = EmptyOperator(task_id = 'end')

    start >> pa >> check_site_task >> [regimes , empresas , estabelecimento , socios , dimensoes] >> unzip_files >>  [etl_dims , etl_empresas ,etl_socios] >> etl_estab >> dbt_run_task >> delete_temp_files >> end



main = cnpj2026()

