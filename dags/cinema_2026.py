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
import requests
from cosmos import DbtDag, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from datetime import datetime
from pathlib import Path

params = {
        "Usuario": Param(
        default="José Nélio"
        ,type='string'
        ,enum=["José Nélio"]
        ,description="""
        Escolher usuario 
        """
        )
    ,'anos': Param(
            default='2026-02'
            ,type="string"
            ,description="Escolher ano ou Todos: 2026 ou 2026-02 ou TODOS")
    ,'estados': Param(
            default='RR'
            ,type="string"
            ,description="Estado Filtrado")
    ,'senha': Param(
        default='nelio'
        ,type="string"
        ,description="Senha para subir ao postgres")
    ,'database': Param(
        default='cinema_teste'
        ,type="string"
        ,enum=["cinema_teste","cinema_RR","cinema_BR","cinema_RJ"]
        ,description="Escolher o nome do banco de dados para subir os dados tratados")

    ,'host': Param(
        default="172.19.0.2"
        ,type='string'
        ,description="""
        Escolher o ip do docker  "172.19.0.3","172.20.0.3","172.19.0.2"
        """        
    )
    }

# Configuração de onde o dbt está no seu Docker/Worker
DBT_PROJECT_PATH = Path("/opt/airflow/dbt/cinema")

profile_config = ProfileConfig(
    profile_name="cinema",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_default", # O ID da conexão que você cria no Airflow UI
        profile_args={"schema": "public"},
    ),
)


default_args = {
    "owner": "Nelio Cruel",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="dag_cinema",
    start_date=datetime(2024, 1, 1),
    schedule="@once", 
    params=params,
    catchup=False,
    tags=["cinema"],
    max_active_tasks= 30,
    default_args=default_args
)


def cinema2026():

    @task
    def download_file(url,path_save):
        # 1. Garante que o diretório existe
        os.makedirs(path_save, exist_ok=True)        
        file_name = url.split('/')[-1]
        full_path = os.path.join(path_save, file_name)
        print(f"Iniciando download de: {url}")
        
        try:
            with requests.get(url, stream=True, timeout=60) as r:
                r.raise_for_status() # Levanta erro se o site estiver fora (404, 500, etc)
                
                with open(full_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            
            print(f"Download concluído com sucesso! Salvo em: {full_path}")
            return full_path

        except requests.exceptions.RequestException as e:
            print(f"Erro ao baixar o arquivo: {e}")
            raise

    
    @task
    def unzip_file(zip_path, file_save, anos=""):

        zip_name = os.path.splitext(os.path.basename(zip_path))[0]
        extract_to = os.path.join(file_save, zip_name)
        os.makedirs(extract_to, exist_ok=True)

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            print(f"Descompactando: {zip_path} para {extract_to}")
            for file_name in zip_ref.namelist():
                print(f"Verificando arquivo: {file_name} para extração")
                if anos == 'TODOS' or str(anos) in file_name:
                    zip_ref.extract(file_name, extract_to)
                    print(f"Extraído: {file_name}")

    @task
    def read_bilheteria(caminho_arquivo,anos,estados, row, caminho_saida,database, senha, host):     
        print (f"acessando: postgresql+psycopg2://postgres:{senha}@{host}:5432/{database}")
        engine = create_engine(f"postgresql+psycopg2://postgres:{senha}@{host}:5432/{database}")
            ##172.20.0.3
        dataframe = []
        colunas_datas = ['DATA_EXIBICAO', 'SESSAO']
        colunas_inteiros = ['PUBLICO']
        for arquivo in os.listdir(caminho_arquivo):
            if arquivo.endswith('.csv') and anos == "" or str(anos) in arquivo:
                caminho_completo = os.path.join(caminho_arquivo, arquivo)
                print(f"Processando arquivo: {caminho_completo}")
                df = pd.read_csv(caminho_completo, delimiter=';', dtype=str)
                df['NOME_ARQUIVO'] = arquivo
                if estados:
                    df = df[df['UF_SALA_COMPLEXO'] == estados]
                df['TITULO_FILME'] = df['TITULO_BRASIL'].fillna(df['TITULO_ORIGINAL'])
                df.insert(1, 'TITULO_FILME', df.pop('TITULO_FILME'))
                df = df[df['REGISTRO_SALA'].notnull()]
                for coluna in colunas_datas:
                    df[coluna] = pd.to_datetime(df[coluna], errors='coerce', dayfirst=True)
                for coluna in colunas_inteiros:
                    df[coluna] = pd.to_numeric(df[coluna], errors='coerce').fillna(0).astype(int)
                df.columns = [col.lower() for col in df.columns]
                df.to_sql("bilheteria", engine, index=False, if_exists='append')
                print(f"Arquivo Carregado no Bando de dados: {arquivo}")

                dataframe.append(df)

        df_bilheteria = pd.concat(dataframe, ignore_index=True)     


        nome_arquivo_saida = os.path.join(caminho_saida, "bilheteria_diaria_tratada.csv")
        df_bilheteria.to_csv(nome_arquivo_saida, sep=';', index=False, encoding='utf-8')
        print(f"Salvo com sucesso em: {nome_arquivo_saida}")

       


    @task
    def d_cinemas_salas(caminho, caminho_saida, estados,database, senha, host):
        print (f"acessando: postgresql+psycopg2://postgres:{senha}@{host}:5432/{database}")
        engine = create_engine(f"postgresql+psycopg2://postgres:{senha}@{host}:5432/{database}")
        colunas_datas = ['DATA_SITUACAO_SALA', 'DATA_INICIO_FUNCIONAMENTO_SALA', 'DATA_SITUACAO_COMPLEXO']
        colunas_inteiros = ['ASSENTOS_SALA','ASSENTOS_CADEIRANTES','ASSENTOS_MOBILIDADE_REDUZIDA','ASSENTOS_OBESIDADE','ACESSO_ASSENTOS_COM_RAMPA']
        df_sala = pd.read_csv(caminho, delimiter=';', dtype=str)
        print(f"lendo: {caminho}")
        if estados:
            df_sala = df_sala[df_sala['UF_COMPLEXO'] == estados]
        for coluna in colunas_datas:
            df_sala[coluna] = pd.to_datetime(df_sala[coluna], errors='coerce', dayfirst=True)
        for coluna in colunas_inteiros:
            df_sala[coluna] = pd.to_numeric(df_sala[coluna], errors='coerce').fillna(0).astype(int)
        print("passou tratamento de dados")
        df_sala.columns = [col.lower() for col in df_sala.columns]
        print("passou tratamento de colunas")
        nome_arquivo_saida = os.path.join(caminho_saida, "d_cinema.csv")
        print("salvando arquivo tratado")
        df_sala.to_csv(nome_arquivo_saida, sep=';', index=False, encoding='utf-8')
        print(f"d_cinemas salvo com sucesso em local: {nome_arquivo_saida}")
        df_sala.to_sql("salas", engine, index=False, if_exists='replace')
        
        return print(f"d_cinemas salvo com sucesso no banco de dados: {database}")


    @task
    def d_filmes(caminho, colunas,caminho_saida, database, senha,  host):
        print (f"acessando: postgresql+psycopg2://postgres:{senha}@{host}:5432/{database}")
        engine = create_engine(f"postgresql+psycopg2://postgres:{senha}@{host}:5432/{database}")
        df_filmes = pd.read_csv(caminho, delimiter=';',dtype=str, usecols=colunas)
        df_filmes = df_filmes.drop_duplicates(subset=['cpb_roe'])
        df_filmes['nacionalidade'] = np.where(df_filmes['pais_obra'] == 'BRASIL', 'Brasileiro', 'Internacional')

        nome_arquivo_saida = os.path.join(caminho_saida, "d_filmes.csv")
        df_filmes.to_csv(nome_arquivo_saida, sep=';',index=False, encoding='utf-8')
        print(f"d_filmes salvo com sucesso em: {nome_arquivo_saida}")
        df_filmes.to_sql("filmes", engine, index=False, if_exists='replace')

        return df_filmes   


    @task
    def lancamentos (caminhodist,caminho_saida,database,senha, host):
        print (f"acessando: postgresql+psycopg2://postgres:{senha}@{host}:5432/{database}")
        engine = create_engine(f"postgresql+psycopg2://postgres:{senha}@{host}:5432/{database}")
        colunas_datas = ['data_lancamento_obra']
        colunas_inteiros = ['publico_total']
        colunas_moeda = ['renda_total']

        df_lancamento = pd.read_csv(caminhodist, delimiter=';',dtype=str)
        df_lancamento['PAIS_OBRA'] = np.where(df_lancamento['PAIS_OBRA'] == 'BRASIL', 'NACIONAL', 'ESTRANGEIRO')
        df_lancamento['nacionalidade'] = np.where(df_lancamento['PAIS_OBRA'] == 'BRASIL', 'Brasileiro', 'Internacional')
        df_lancamento.columns = [col.lower() for col in df_lancamento.columns]
        for coluna in colunas_datas:
            df_lancamento[coluna] = pd.to_datetime(df_lancamento[coluna], errors='coerce', dayfirst=True)
        for coluna in colunas_inteiros:
            df_lancamento[coluna] = pd.to_numeric(df_lancamento[coluna], errors='coerce').fillna(0).astype(int)
        for coluna in colunas_moeda:
            df_lancamento[coluna] = (
                df_lancamento[coluna]
                .str.replace("R$", "", regex=False)   # remove símbolo de moeda
                .str.replace(".", "", regex=False)    # remove separador de milhar
                .str.replace(",", ".", regex=False)   # troca vírgula por ponto decimal
                .astype(float)                        # converte para float
            )
        nome_arquivo_saida = os.path.join(caminho_saida, "lancamentos.csv")
        df_lancamento.to_csv(nome_arquivo_saida, sep=';',index=False, encoding='utf-8')
        df_lancamento.to_sql("lancamentos", engine, index=False, if_exists='replace')

        return df_lancamento






    # @task
    # def upload_to_postgres(df, table_name, database, user, password):
    #     # engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    #     engine = create_engine(f"postgresql+psycopg2://{user}:{password}@172.17.0.2:5432/{database}")
    #     print(engine)
    #     df = pd.read_csv(df, sep=';')
    #     df.to_sql(table_name, engine, index=False, if_exists='replace')


#### FIM DA FUNÇÃO / CHAMADAS DAS FUNÇÕES PARA CADA TAREFA ###

    downloadbilheteria = download_file.override(task_id="download_bilheteria")(
        url = "https://dados.ancine.gov.br/dados-abertos/bilheteria-diaria-obras-por-exibidoras-csv.zip" ,
        path_save = "/opt/airflow/cinema2026/data/raw/zip/bilheteria_diaria")
# "C:\Users\Nelio\OneDrive\_CINEMA\cinema_2026\data\raw\zip\bilheteria_diaria\bilheteria-diaria-obras-por-exibidoras-csv.zip"
    unzipbilheteria = unzip_file.override(task_id="unzip_bilheteria")(
        zip_path = "/opt/airflow/cinema2026/data/raw/zip/bilheteria_diaria/bilheteria-diaria-obras-por-exibidoras-csv.zip",
        file_save="/opt/airflow/cinema2026/data/raw/unzip/bilheteria-diaria",
        anos = "{{ params.anos }}"
    )

    mover_zip_bilheteria = BashOperator(
    task_id='mover_zips',
    bash_command=f'mv /opt/airflow/cinema2026/data/raw/zip/bilheteria_diaria/bilheteria-diaria-obras-por-exibidoras-csv.zip /opt/airflow/cinema2026/data/raw/arquivo/zip/bilheteria_diaria/ '
    )

    etl_bilheteria =  read_bilheteria(caminho_arquivo = "/opt/airflow/cinema2026/data/raw/unzip/bilheteria-diaria/bilheteria-diaria-obras-por-exibidoras-csv",
                anos = "{{ params.anos }}",
                estados = "{{ params.estados }}",
                row= None,
                caminho_saida = "/opt/airflow/cinema2026/data/processados"
                ,database = "{{ params.database }}"
                ,host="{{ params.host }}"
                ,senha="{{ params.senha }}"
                )
    

    mover_bilheteria_csv = BashOperator(
    task_id='mover_read_bilheteria',
    bash_command=f'mv /opt/airflow/cinema2026/data/raw/unzip/bilheteria-diaria/bilheteria-diaria-obras-por-exibidoras-csv/*.csv /opt/airflow/cinema2026/data/raw/arquivo/unzip/bilheteria_diaria/'
    )
    
###-------------------------------------------------------------------------------------------------------------------------------------------------#####

    downloadcinemas = download_file.override(task_id="download_cinemas")(
        url = "https://dados.ancine.gov.br/dados-abertos/salas-de-exibicao-e-complexos.csv" ,
        path_save = "/opt/airflow/cinema2026/data/raw/unzip/salas-de-exibicao")
    
    etl_cinema = d_cinemas_salas(caminho= "/opt/airflow/cinema2026/data/raw/unzip/salas-de-exibicao/salas-de-exibicao-e-complexos.csv", 
                caminho_saida = "/opt/airflow/cinema2026/data/processados", 
                estados = "{{ params.estados }}"
                ,database = "{{ params.database }}"
                ,host="{{ params.host }}"
                ,senha="{{ params.senha }}"
    )
    
    mover_cinemas_csv = BashOperator(
    task_id='mover_salas_exibicao',
    bash_command=f'mv /opt/airflow/cinema2026/data/raw/unzip/salas-de-exibicao/*.csv /opt/airflow/cinema2026/data/raw/arquivo/unzip/salas-de-exibicao/ '
    )
    
###-------------------------------------------------------------------------------------------------------------------------------------------------#####


    downloadlancamentos = download_file.override(task_id="download_lancamentos")(
        url = "https://dados.ancine.gov.br/dados-abertos/lancamentos-comerciais-por-distribuidoras.csv" ,
        path_save = "/opt/airflow/cinema2026/data/raw/unzip/distribuidoras")

    elt_lancamentos = lancamentos("/opt/airflow/cinema2026/data/raw/unzip/distribuidoras/lancamentos-comerciais-por-distribuidoras.csv",
        "/opt/airflow/cinema2026/data/processados",
        database = "{{ params.database }}"
        ,host="{{ params.host }}"
        ,senha="{{ params.senha }}"
        )
    
    mover_distribuidoras_csv = BashOperator(
    task_id='mover_read_distribuidoras',
    bash_command=f'mv /opt/airflow/cinema2026/data/raw/unzip/distribuidoras/*.csv /opt/airflow/cinema2026/data/raw/arquivo/unzip/distribuidoras/'
    )
    
###-------------------------------------------------------------------------------------------------------------------------------------------------#####

    etl_filmes = d_filmes("/opt/airflow/cinema2026/data/processados/bilheteria_diaria_tratada.csv",
        #  ['CPB_ROE', 'TITULO_FILME', 'TITULO_ORIGINAL', 'TITULO_BRASIL', 'PAIS_OBRA'],
        ['cpb_roe', 'titulo_filme', 'titulo_original', 'titulo_brasil', 'pais_obra'],
         "/opt/airflow/cinema2026/data/processados",
         database = "{{ params.database }}"
        ,host="{{ params.host }}"
        ,senha="{{ params.senha }}"
         )
    

    dbt_cinema = DbtTaskGroup(
            group_id="camada_transformacao_dbt",
            project_config=ProjectConfig(DBT_PROJECT_PATH),
            profile_config=profile_config,
            render_config=RenderConfig(
                select=["path:models/cinema"]
            ),
            operator_args={"install_deps": True},
        )






    # upload = upload_to_postgres(caminho = "/opt/airflow/cinema2026/data/processados/bilheteria_diaria_tratada.csv", table_name = "{{ params.table_name }}", database = "{{ params.database }}", user = "{{ params.user }}", password = "{{ params.password }}")
    
    
    start = EmptyOperator(task_id = 'start')
    end = EmptyOperator(task_id = 'end')


    start >> downloadbilheteria >> unzipbilheteria >> etl_bilheteria >> etl_filmes >> mover_zip_bilheteria >> mover_bilheteria_csv >> dbt_cinema >> end
    start >> downloadcinemas >> etl_cinema >> mover_cinemas_csv >> end
    start >> downloadlancamentos >> elt_lancamentos >> mover_distribuidoras_csv >> end


cinema2026()