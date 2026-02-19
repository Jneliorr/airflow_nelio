import pandas as pd
import numpy as np
import os
import logging
import io

import pyarrow as pa
import pyarrow.parquet as pq
import pandas_gbq as bq
from google.oauth2 import service_account
from google.cloud import storage

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.models.variable import Variable

conn_id = "gcs_default"
#credentials = GCSHook(conn_id=conn_id).get_credentials()
project = Variable.get("project_id")

termos_01000 = [
    'REG',
    'DtgGeracao',
    'NumDas',
    'IdSistOrigem',
    'Cnpjmatriz',
    'Princ',
    'Multa',
    'Juros',
    'EncargoLegal',
    'Dtvenc',
    'Dtvalcalc',
    'Vdas',
    'CPF'
]
dicionario_01000 = {
    'colunas': termos_01000,

    'colunas_reais': [
        "Princ",
        "Multa",
        "Juros",
        "EncargoLegal",
        "Vdas"
    ],
    'colunas_datas': [
        'Dtvenc',
        'Dtvalcalc'
    ],
    'colunas_datetime':["DtgGeracao"],
    'colunas_inteiro': None
}


termos_01100= [
    "REG",
    "PA",
    "cod_rec_principal",
    "valor_principal",
    "cod_rec_multa",
    "valor_multa",
    "cod_rec_juros",
    "valor_juro",
    "uf",
    "cod_munic",
    "NumDas", 
    "IdSistOrigem"
]

dicionario_01100 = {
    'colunas': termos_01100,

    'colunas_reais': [
        "valor_principal",
        "valor_multa",
        "valor_juro"
    ],
    'colunas_datas': None,
    'colunas_datetime':None,
    'colunas_inteiro': ['PA']
}



termos_aaaaa = ['REG_ABERTURA', 'COD_VER', 'DT_INI', 'DT_FIN', 'ARQUIVO']
## Serviço
class ETLDasn:
    def __init__(self, bucket_name='k8s-dataita', destination_parquet=None):
        self.atributos_arquivo_aaaaa = []
        #self.contribuite_apuracao_00000 = []
        #self.info_sobre_processo_nao_optante_00001 = []
        self.info_valores_calculados_das_01000 = []
        self.info_perfil_das_01100 = []
        self.encerramento_99999 = []
        self.bucket_name = bucket_name
        self.destination_parquet = destination_parquet
        self.destinations = []

    def _read(self,prefix, bucket=None, credentials=None, cloud=False, encoding='utf8', mode='r'):
        
        
        if cloud:
            path_file = prefix
            client = storage.Client(credentials=credentials)
            bucket = client.bucket(bucket_name=bucket)
            blob = bucket.blob(path_file)
            #data = StringIO(blob.download_as_string())
            
            with blob.open(mode=mode, encoding=encoding) as file:
                rows = file.readlines()
                file.close()
            #print(rows[:10])
            return rows
            
        else:
            path_file = os.path.join(path_file, file)
            arq = open(path_file, mode, encoding='utf-8')
            rows = arq.readlines()
            return rows
    
    def read_dasn(self, prefix, bucket=None, credentials=None, cloud=False, encoding='utf8', mode='r'):

        #arq = open(os.path.join(path, file), 'r', encoding='utf-8')
        file=prefix.split('/')[-1]
        rows = self._read(prefix=prefix, bucket=bucket, credentials=credentials, cloud=cloud, encoding=encoding, mode=mode)
        len_rows = len(rows)
        for i in range(len_rows):
            linha = rows[i].strip('\n')  # .split('|')
           

            if linha[0:5] == 'AAAAA':
                self.atributos_arquivo_aaaaa.append(linha + '|' + file)
                if i % 100 == 0:
                    logging.info('Montando as listas de linhas do arquivo {}'.format(file))


            elif linha[0:5] == '01000':
               
                self.info_valores_calculados_das_01000.append(linha)
                linha = linha.split('|')
                num_das, id_sistema = linha[2], linha[3]
                if i % 100 == 0:
                    logging.info('Registro 01000 DASn id: {} finalizado no index {}'.format(num_das, i))
                #print(num_das, id_sistema)
                
                

            elif linha[0:5] == '01100':
                self.info_perfil_das_01100.append(linha + '|' + num_das + '|' + id_sistema)
                if i % 100 == 0:
                    logging.info('Registro 01100 DASn id: {} finalizado no index {}'.format(num_das, i))

    def clean(self, df: pd.DataFrame, metadados):
        ## datas
        logging.info('limpando os dataframes')
        
        if  metadados['colunas_datas'] != None:
            for col in metadados['colunas_datas']:
                df[col] = pd.to_datetime(df[col], format='%Y%m%d', errors='coerce').astype('datetime64[us]')
             
        ## Datetime
        if  metadados['colunas_datetime'] != None:
            for col in metadados['colunas_datetime']:
                df[col] = pd.to_datetime(df[col], format='%Y%m%d%H%M%S').astype('datetime64[us]')
        ## float
        if  metadados['colunas_reais'] != None:
            for col in metadados['colunas_reais']:
                df[col] = df[col].str.replace(',', '.').replace('', np.nan).astype(float)
        ## Inteiros
        if  metadados['colunas_inteiro'] != None:
            for col in metadados['colunas_inteiro']:
                df[col] = df[col].astype(int)

        return df


    def create_dataframe_aaaaa(self, prefix, dataset, credentials,mode='l', if_exists='append'):
        list_data = self.atributos_arquivo_aaaaa
        data = []
        file = prefix.split('/')[-1].replace('.txt','-dasn_file_aaaaa.parquet')

        for i in list_data:
            data.append(i.split('|'))

        df = pd.DataFrame(data, columns=termos_aaaaa)
        if mode=='l':
            return  df 
        elif mode == 'gcp':
            bq.to_gbq(df, destination_table=dataset + '.dasn_file_aaaaa',
                  project_id=project, if_exists=if_exists, credentials=credentials)
        elif mode == 's3':
            
            client = storage.Client(credentials=credentials)
            bucket = client.get_bucket(self.bucket_name)
        
            destination = self.destination_parquet + f'/dasn_file_aaaaa/{file}'
            self.destinations.append(destination)

            table = pa.Table.from_pandas(df, schema=None, preserve_index=False)
            buffer = io.BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)
                        
            logging.info(f'gravando {destination}')
            blob = bucket.blob(destination)
            blob.upload_from_string(
                    buffer.read()
                    #df \
                    #.to_parquet(engine='pyarrow')
                    ,'application/octet-stream'
                )
            

    def create_dataframe_01000(self, prefix, dataset, credentials, mode='l', if_exists='append'):
        list_data = self.info_valores_calculados_das_01000
        data = []
        file = prefix.split('/')[-1].replace('.txt','-dasn_apuracao_01000.parquet')

        for i in list_data:
            data.append(i.split('|'))

        df = pd.DataFrame(data, columns=termos_01000)
        df = self.clean(df, metadados=dicionario_01000)

        if mode == 'l':
            
            #print(df)
            return df
        
        elif mode == 'gcp':
            bq.to_gbq(df, destination_table=dataset + '.dasn_apuracao_01000',
                  project_id=project, if_exists=if_exists, credentials=credentials)
            
        elif mode == 's3':
            
            client = storage.Client(credentials=credentials)
            bucket = client.get_bucket(self.bucket_name)
        
            destination = self.destination_parquet + f'/dasn_apuracao_01000/{file}'
            self.destinations.append(destination)

            table = pa.Table.from_pandas(df, schema=None, preserve_index=False)
            buffer = io.BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)
                        
            logging.info(f'gravando {destination}')
            blob = bucket.blob(destination)
            blob.upload_from_string(
                    buffer.read()
                    #self.clean(df, metadados=dicionario_01000) \
                    #.to_parquet(engine='pyarrow')
                    ,'application/octet-stream'
                )

    def create_dataframe_01100(self, prefix, dataset, credentials, mode='l', if_exists='append'):
        list_data = self.info_perfil_das_01100
        data = []
        file = prefix.split('/')[-1].replace('.txt','-dasn_info_01100.parquet')
        logging.info(f'read file: {file}')

        for i in list_data:
            data.append(i.split('|'))

        df = pd.DataFrame(data, columns=termos_01100)
        df = self.clean(df, metadados=dicionario_01100)

        if mode == 'l':
            return df
        elif mode == 'gcp':

            bq.to_gbq(df, destination_table=dataset + '.dasn_info_01100',
                  project_id=project, if_exists=if_exists, credentials=credentials)
            
        elif mode == 's3':
            
            client = storage.Client(credentials=credentials)
            bucket = client.get_bucket(self.bucket_name)
                        
            destination = self.destination_parquet + f'/dasn_info_01100/{file}'
            self.destinations.append(destination)

            table = pa.Table.from_pandas(df, schema=None, preserve_index=False)

            buffer = io.BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)

            
            logging.info(f'gravando {destination}')
            blob = bucket.blob(destination)
            blob.upload_from_string(
                        buffer.read()
                        #self.clean(df, metadados=dicionario_01100) \
                        #.to_parquet(engine='pyarrow')
                    ,'application/octet-stream'
                )




## Funcao final de Carregamento das DASn
class DasnOperatorGBQ(BaseOperator):
    template_fields = ("bucket_name", "prefix", "dataset")
    @apply_defaults
    def __init__(self, prefix, bucket_name=None, credentials=None, dataset=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.prefix = prefix
        self.bucket_name = bucket_name
        self.credentials = credentials
        self.dataset = dataset

    def execute(self, context):
        datadas = ETLDasn()
        datadas.read_dasn(prefix=self.prefix, bucket=self.bucket_name, credentials=self.credentials, cloud=True)
        datadas.create_dataframe_aaaaa(dataset=self.dataset, mode='gcp', credentials=self.credentials)
        logging.info('Carregamento das DASn aaaaa')
        datadas.create_dataframe_01000(dataset=self.dataset, mode='gcp', credentials=self.credentials)
        logging.info('Carregamento das DASn 01000')
        datadas.create_dataframe_01100(dataset=self.dataset, mode='gcp', credentials=self.credentials)
        logging.info('Carregamento das DASn 01100')
        
        return f"End {self.prefix}"
    
    
## Funcao final de Carregamento das DASn
class DasnOperatorParquet(BaseOperator):
    template_fields = ("bucket_name", "prefix", "dataset", "destination_parquet")
    @apply_defaults
    def __init__(self, prefix, destination_parquet=None, bucket_name=None, credentials=None, dataset=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.prefix = prefix
        self.bucket_name = bucket_name
        self.credentials = credentials
        self.dataset = dataset
        self.destination_parquet = destination_parquet

    def execute(self, context):
        datadas = ETLDasn(bucket_name=self.bucket_name, destination_parquet=self.destination_parquet)
        datadas.read_dasn(prefix=self.prefix, bucket=self.bucket_name, credentials=self.credentials, cloud=True)
        datadas.create_dataframe_aaaaa(prefix=self.prefix, dataset=self.dataset, mode='s3', credentials=self.credentials)
        logging.info('Carregamento das DASn aaaaa')
        datadas.create_dataframe_01000(prefix=self.prefix, dataset=self.dataset, mode='s3', credentials=self.credentials)
        logging.info('Carregamento das DASn 01000')
        datadas.create_dataframe_01100(prefix=self.prefix, dataset=self.dataset, mode='s3', credentials=self.credentials)
        logging.info('Carregamento das DASn 01100')
        
        return datadas.destinations