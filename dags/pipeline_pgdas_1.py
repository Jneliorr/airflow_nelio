"""
Dag de Pipeline de ETL dos arquivos PGDAS: zip >> BigQuery
"""

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.models.param import Param, ParamsDict
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from google.cloud import storage, bigquery
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator


from airflow.datasets import Dataset

from kubernetes.client import models as k8s  

from datetime import datetime
from zipfile import ZipFile
from io import BytesIO
import logging
import os
from ast import literal_eval
import gcsfs
from pendulum import duration

from operators.pgdas import PgdasETLOperator
from operators.gcs_operators import GCSListObjectsOperators
from operators.pgdas_convert import PgdasToParquetOperator
from operators.gcs_operators import GCSParquetToGBQ   
from operators.move_files import MoveGCSFiles
from operators.move_files import MoveGCSFiles

# from datahub_airflow_plugin.entities import Dataset as DatahubDataset

# Defina os parâmetros do DAG (valores padrão podem ser sobrescritos ao iniciar o DAG)
conn_id = "gcs_default"
params_dict = {
    "Usuario": Param(
        default="José Nélio"
        ,type='string'
        ,enum=["José Nélio", "Vinicius B. Soares", "Igor Kalon"]
        )
    ,'BUCKET_NAME': Param(
        default="k8s-dataita",
        type='string',
        enum=["dataita", "k8s-dataita", "nelio_teste"])
    ,'prefix': Param(
        default='raw/simples_nacional/pgdas/waiting/202510'
        ,type="string"
        ,description="Caminho arquivo bruto: ano&mês")
    ,'RAW_ARCHIVE': Param(
        default='raw/simples_nacional/pgdas/archive/202510'
        ,type="string"
        ,description="Caminho para arquivo: ano&mês")
    ,'prefix_staged': Param(
        default='staged/simples_nacional/pgdas/waiting/202510'
        ,type="string"
        ,description="Caminho apos leitura: ano&mês")

    ,'DESTINATION': Param(
        default="staged/simples_nacional/pgdas/ready"
        , type='string'
        ,description="Parametro utilizado para salvar os arquvios parquets transformados nos agrupamentos por PA disponiveis para etapa ready")
    ,"PA": Param(
        default="202510"
        , type='string'
        ,description="Parametro utilizado para adiconar o PA aos nome dos parquet convertido na etapa ready, no caso de haver decendio é necessário prencher, para evitar sobrescrição de arquivos"
        )
    ,'prefix_archive_staged': Param(
        default='staged/simples_nacional/pgdas/archive/202510'
        ,type="string"
        ,description="Caminho arquivo lido : ano&mês")
#    ,'MUNICIPIO': Param(
#         default='5837'
#         ,type="string"
#         ,description="Municipio Escolhido para Filtros das dags 2 e 4")
}


params = ParamsDict(params_dict)
hook = GoogleBaseHook(gcp_conn_id="gcs_default")
credentials = hook.get_credentials()


gcs_ready_pgdas = Dataset("gs://k8s-dataita/staged/simples_nacional/pgdas/ready")

pod_override = k8s.V1Pod(
    metadata=k8s.V1ObjectMeta(labels={"workload": "gpu"}),
    spec=k8s.V1PodSpec(
        node_selector={"cloud.google.com/gke-nodepool": "pool-spot-highmem"},
        containers=[
            k8s.V1Container(
                name="airflow-custom-k8s",
                image="us-central1-docker.pkg.dev/infra-itaborai/registry-ita/airflow-custom:1.3.3",
                resources=k8s.V1ResourceRequirements(
                    requests={"cpu": "2000m", "memory": "4Gi"},
                    limits={"cpu": "3500m", "memory": "30Gi"}
                )
            )
        ]
    )
)






@dag(
    dag_id='PGDAs_ELT_1'
    ,start_date=datetime(2025,2,19)
    ,schedule= None
    ,doc_md=__doc__
    ,catchup=False
    ,params = params_dict
    ,tags=["PROD", "PGDAS"]
    ,max_active_tasks=8
    ,default_args={
        "retries": 3
        ,"retry_delay": duration(seconds=5)
    }
    )

def pgdas_pipeline(**kwargs):
    @task
    def salvar_parametros_nas_variaveis(**context):
      
        bucket_name = context['params'].get('BUCKET_NAME', 'k8s-dataita')

        
        parametros = {
            'BUCKET_NAME': bucket_name,
            # 'ultima_execucao_manual': context['execution_date'].isoformat(),
            'ultima_execucao_manual': context['logical_date'].isoformat(),
            'usuario': context['params'].get('Usuario', 'José Nélio')
        }
        

        Variable.set("PARAMETROS_PGDAS_ATUAIS", parametros, serialize_json=True)
        
        print(f"💾 PARÂMETROS SALVOS PARA DAG 2:")
        print(f"   BUCKET_NAME: {bucket_name}")

        print(f"   Usuário: {context['params'].get('Usuario', 'José Nélio')}")
        
        return parametros
    
    # @task
    # def verificar_e_carregar_parametros(**context):
    #     """Verifica se temos parâmetros válidos antes de executar o Spark"""
        
    #     # Verificar se foi acionada por trigger com parâmetros
    #     bucket_trigger = context['params'].get('BUCKET_NAME')
    #     municipio_trigger = context['params'].get('MUNICIPIO')
        
    #     if bucket_trigger and municipio_trigger:
    #         return {
    #             'BUCKET_NAME': bucket_trigger,
    #             'MUNICIPIO': municipio_trigger
    #         }
        
    #     # Verificar se temos parâmetros salvos recentemente
    #     try:
    #         parametros = Variable.get("PARAMETROS_PGDAS_ATUAIS", deserialize_json=True)
    #         if parametros and 'BUCKET_NAME' in parametros and 'MUNICIPIO' in parametros:
    #             return parametros
    #     except:
    #         pass
        
    #     # Se não encontrou parâmetros, usar defaults
    #     print("⚠️  Usando parâmetros padrão - nenhum parâmetro manual encontrado")
    #     return {
    #         'BUCKET_NAME': 'k8s-dataita',
    #         'MUNICIPIO': '5837'
    #     }

    #### Python Operators Personoalizados #####==========================================#
    

    
            
        
    @task(map_index_template='{{ path_files }}', task_id='unzip_to_gcs')
    def zip_to_gcs(**kwargs):
                
        """
        Descompacta o arquivo zip do GCS para um diretório temporário no GCS.

        Args:
            **kwargs: 
                path_files (str): Caminho do arquivo zip no GCS

        Returns:
            str: Caminho do diretório temporário no GCS com o nome do arquivo zip descompactado
        """
        params : ParamsDict = kwargs["params"]
        bucket_name = params["BUCKET_NAME"]
        hook = GCSHook(gcp_conn_id=conn_id)
        files = [kwargs['path_files']]
        
        logging.info(f'connect in bucket: {bucket_name}')
        
        path_descompacted_files = f'tmp/{kwargs['path_files'].split("/")[-1].replace(".zip","")}/'
        for i in range(len(files)):
            zip_data = BytesIO(hook.download(bucket_name, files[i]))
            with ZipFile(zip_data, 'r') as zip_ref:
                for file in zip_ref.namelist():
                    if file.endswith('.zip'):
                        continue  # Skip directories in the zip file
                    file_data = zip_ref.read(file)
                    print(path_descompacted_files + file)
                    print(logging.info(path_descompacted_files + file))
                    hook.upload(bucket_name, object_name=path_descompacted_files + file, data=file_data)


        return path_descompacted_files + file

    ############################################======================================#    
    start = EmptyOperator(task_id='start')



    # ✅ NOVA TASK - Salva parâmetros nas variáveis
    salvar_parametros_task = salvar_parametros_nas_variaveis()
    


    list_files_zip = GCSListObjectsOperators(
        task_id="list_files",
        bucket_name="{{ params.BUCKET_NAME }}",
        prefix= "{{ params.prefix }}",  # Define a pasta onde estão os arquivos
        delimiter=".zip",
        gcp_conn_id=conn_id
    )


    list_files_txt = zip_to_gcs.expand(path_files=list_files_zip.output)


 
    pgdas = PgdasToParquetOperator.partial(
        task_id='pgdas_to_parquet',
        bucket_name="{{ params.BUCKET_NAME }}",
        destination_parquet = "{{ params.prefix_staged}}",
        cloud=True,
        project_id=Variable.get("project_id"),
        credentials=credentials,
        executor_config={"pod_override": pod_override},
            
    ).expand(file=list_files_txt)



    floatconvert = SparkKubernetesOperator(
        task_id='floatconvert',
        kubernetes_conn_id='kubernetes_default',
        namespace='spark-jobs',
        application_file='util/spark/pgdas/spark-job-convert-pgdas-schema.yaml',
        do_xcom_push=False,
        executor_config={"pod_override": pod_override}
       
    )


    delete_tmp = GCSDeleteObjectsOperator.partial(
        task_id='delete_tmp',
        bucket_name="{{ params.BUCKET_NAME }}"
        ).expand(prefix=list_files_txt)
    
    mover_staged = GCSToGCSOperator(
        task_id='mover_staged',
        source_bucket="{{ params.BUCKET_NAME }}",
        source_object="{{ params.prefix_staged }}",  # SEM barra final
        destination_bucket="{{ params.BUCKET_NAME }}",
        destination_object="{{ params.prefix_archive_staged }}",  # SEM barra final
        move_object=True,
        replace=True
    )
    
    mover_raw = GCSToGCSOperator(
        task_id='mover_raw',
        source_bucket="{{ params.BUCKET_NAME }}",
        source_object="{{ params.prefix }}",  # SEM barra final
        destination_bucket="{{ params.BUCKET_NAME }}",
        destination_object="{{ params.RAW_ARCHIVE }}",  # SEM barra final
        move_object=True,
        replace=True
    )


    end = EmptyOperator(task_id='end', outlets=[gcs_ready_pgdas])

    start >> salvar_parametros_task >> list_files_zip >> list_files_txt >> pgdas >> floatconvert >> [delete_tmp, mover_raw, mover_staged] >>  end # specific_pgdas_result #>>paths >> delete_tmp >> end


pgdas_pipeline()
