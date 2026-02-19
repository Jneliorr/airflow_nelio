from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.hooks.pgdas_hook import ETLPgdasHook
import pandas as pd
from google.cloud import storage
import io
import logging
from pandas_gbq import to_gbq
import os
import pyarrow as pa
import pyarrow.parquet as pq

class PgdasToParquetOperator(BaseOperator):
    
    template_fields = ("destination_parquet", "bucket_name")
    @apply_defaults
    def __init__(self, file, destination_parquet=None, bucket_name=None, cloud=True, encoding='utf8', mode='r', project_id=None, credentials=None, if_exists='append', *args, **kwargs):
        super().__init__(*args, **kwargs)
        #self.path_file = prefix
        self.destination_parquet = destination_parquet
        self.file = file #prefix.split('/')[-1]
        self.bucket_name = bucket_name
        self.cloud = cloud
        self.encoding = encoding
        self.mode = mode
        self.project_id = project_id
        self.if_exists = if_exists
        self.credentials = credentials
        #self.dataset = dataset
        
        
    def execute(self, context):
        """
        Método execute é obrigatório e será chamado quando a tarefa for executada.
        """
        # Inicializa o Hook
        dados = ETLPgdasHook()
        path = os.path.dirname(self.file)
        file = self.file.split('/')[-1]
        # Lê o arquivo
        dados.read_pgdas(
            path_file=path,
            file=file,
            bucket=self.bucket_name,
            credentials=self.credentials,  # Passe as credenciais se necessário
            cloud=self.cloud,
            encoding=self.encoding,
            mode=self.mode
        )

        client = storage.Client(credentials=self.credentials)
        bucket = client.get_bucket(self.bucket_name)
        destinations = []
        # Cria o DataFrame e salva no formato Parquet
        destination = self.destination_parquet + f'/df_aaaaa/{file.replace(".txt","-aaaaa.parquet")}'
        logging.info(f'gravando {destination}')

        table = pa.Table.from_pandas(dados.create_dataframe_aaaaa(), schema=None, preserve_index=False)
        
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        blob = bucket.blob(destination)
        blob.upload_from_string(
                buffer.read()
                #.to_parquet(engine='pyarrow')
                ,'application/octet-stream'
            )
        destinations.append(destination)
        logging.info(f"Salvo no bucket {self.bucket_name} no caminho {destination}")
        
        # Cria o DataFrame e salva no formato Parquet
        destination = self.destination_parquet + f'/df_00000/{file.replace(".txt","-00000.parquet")}'
        logging.info(f'gravando {destination}')

        table = pa.Table.from_pandas(dados.create_dataframe_00000(), schema=None, preserve_index=False)

        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        blob = bucket.blob(destination)
        blob.upload_from_string(
                    buffer.read()
                    #dados.create_dataframe_00000() \
                    #.to_parquet(engine='pyarrow')
                ,'application/octet-stream'
            )
        destinations.append(destination)
        logging.info(f"Salvo no bucket {self.bucket_name} no caminho {destination}")
        
        
        # Cria o DataFrame e salva no formato Parquet
        destination = self.destination_parquet + f'/df_00001/{file.replace(".txt","-00001.parquet")}'
        logging.info(f'gravando {destination}')

        table = pa.Table.from_pandas(dados.create_dataframe_00001(), schema=None, preserve_index=False)

        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        blob = bucket.blob(destination)
        blob.upload_from_string(
                    buffer.read()
                    #dados.create_dataframe_00001() \
                    #.to_parquet(engine='pyarrow')
                ,'application/octet-stream'
            )
        destinations.append(destination)
        logging.info(f"Salvo no bucket {self.bucket_name} no caminho {destination}")

        # Cria o DataFrame e salva no formato Parquet
        destination = self.destination_parquet + f'/df_01000/{file.replace(".txt","-01000.parquet")}'
        logging.info(f'gravando {destination}')

        table = pa.Table.from_pandas(dados.create_dataframe_01000(), schema=None, preserve_index=False)

        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        blob = bucket.blob(destination)
        blob.upload_from_string(
                    buffer.read()
                    #dados.create_dataframe_01000() \
                    #.to_parquet(engine='pyarrow')
                ,'application/octet-stream'
            )
        destinations.append(destination)
        logging.info(f"Salvo no bucket {self.bucket_name} no caminho {destination}")
        
        # Cria o DataFrame e salva no formato Parquet
        destination = self.destination_parquet + f'/df_01100/{file.replace(".txt","-01100.parquet")}'
        logging.info(f'gravando {destination}')

        table = pa.Table.from_pandas(dados.create_dataframe_01100(), schema=None, preserve_index=False)

        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        blob = bucket.blob(destination)
        blob.upload_from_string(
                    buffer.read()
                    #dados.create_dataframe_01100() \
                    #.to_parquet(engine='pyarrow')
                ,'application/octet-stream'
            )
        destinations.append(destination)
        logging.info(f"Salvo no bucket {self.bucket_name} no caminho {destination}")
        

        # Cria o DataFrame e salva no formato Parquet
        destination = self.destination_parquet + f'/df_01500/{file.replace(".txt","-01500.parquet")}'
        logging.info(f'gravando {destination}')

        table = pa.Table.from_pandas(dados.create_dataframe_01500(), schema=None, preserve_index=False)
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        blob = bucket.blob(destination)
        blob.upload_from_string(
                    buffer.read()
                    #dados.create_dataframe_01500() \
                    #.to_parquet(engine='pyarrow')
                ,'application/octet-stream'
            )
        destinations.append(destination)
        logging.info(f"Salvo no bucket {self.bucket_name} no caminho {destination}")
        
        # Cria o DataFrame e salva no formato Parquet
        destination = self.destination_parquet + f'/df_01501/{file.replace(".txt","-01501.parquet")}'
        logging.info(f'gravando {destination}')
        table = pa.Table.from_pandas(dados.create_dataframe_01501(), schema=None, preserve_index=False)
        
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        blob = bucket.blob(destination)
        blob.upload_from_string(
                    buffer.read()
                    #dados.create_dataframe_01501() \
                    #.to_parquet(engine='pyarrow')
                ,'application/octet-stream'
            )
        destinations.append(destination)
        logging.info(f"Salvo no bucket {self.bucket_name} no caminho {destination}")
        
        # Cria o DataFrame e salva no formato Parquet
        destination = self.destination_parquet + f'/df_01502/{file.replace(".txt","-01502.parquet")}'
        logging.info(f'gravando {destination}')
        table = pa.Table.from_pandas(dados.create_dataframe_01502(), schema=None, preserve_index=False)
        
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        blob = bucket.blob(destination)
        blob.upload_from_string(
                    buffer.read()
                    #dados.create_dataframe_01502() \
                    #.to_parquet(engine='pyarrow')
                ,'application/octet-stream'
            )
        destinations.append(destination)
        logging.info(f"Salvo no bucket {self.bucket_name} no caminho {destination}")
        
        # Cria o DataFrame e salva no formato Parquet
        destination = self.destination_parquet + f'/df_02000/{file.replace(".txt","-02000.parquet")}'
        logging.info(f'gravando {destination}')
        table = pa.Table.from_pandas(dados.create_dataframe_02000(), schema=None, preserve_index=False)
        
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        blob = bucket.blob(destination)
        blob.upload_from_string(
                    buffer.read()
                    #dados.create_dataframe_02000() \
                    #.to_parquet(engine='pyarrow')
                ,'application/octet-stream'
            )
        destinations.append(destination)
        logging.info(f"Salvo no bucket {self.bucket_name} no caminho {destination}")
        
        # Cria o DataFrame e salva no formato Parquet
        destination = self.destination_parquet + f'/df_03000/{file.replace(".txt","-03000.parquet")}'
        logging.info(f'gravando {destination}')
        table = pa.Table.from_pandas(dados.create_dataframe_03000(), schema=None, preserve_index=False)
        
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        blob = bucket.blob(destination)
        blob.upload_from_string(
                    buffer.read()
                    #dados.create_dataframe_03000() \
                    #.to_parquet(engine='pyarrow')
                ,'application/octet-stream'
            )
        destinations.append(destination)
        logging.info(f"Salvo no bucket {self.bucket_name} no caminho {destination}")
        
        # Cria o DataFrame e salva no formato Parquet
        destination = self.destination_parquet + f'/df_03100/{file.replace(".txt","-03100.parquet")}'
        logging.info(f'gravando {destination}')
        table = pa.Table.from_pandas(dados.create_dataframe_03100(), schema=None, preserve_index=False)
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)
        
        blob = bucket.blob(destination)
        blob.upload_from_string(
                    buffer.read()
                    #dados.create_dataframe_03100() \
                    #.to_parquet(engine='pyarrow')
                ,'application/octet-stream'
            )
        destinations.append(destination)
        logging.info(f"Salvo no bucket {self.bucket_name} no caminho {destination}")
        
        # Cria o DataFrame e salva no formato Parquet
        destination = self.destination_parquet + f'/df_03110/{file.replace(".txt","-03110.parquet")}'
        logging.info(f'gravando {destination}')
        table = pa.Table.from_pandas(dados.create_dataframe_03110(), schema=None, preserve_index=False)

        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        blob = bucket.blob(destination)
        blob.upload_from_string(
                    buffer.read()
                    #dados.create_dataframe_03110() \
                    #.to_parquet(engine='pyarrow')
                ,'application/octet-stream'
            )
        destinations.append(destination)
        logging.info(f"Salvo no bucket {self.bucket_name} no caminho {destination}")
        
        
        # Cria o DataFrame e salva no formato Parquet
        destination = self.destination_parquet + f'/df_03120/{file.replace(".txt","-03120.parquet")}'
        logging.info(f'gravando {destination}')
        table = pa.Table.from_pandas(dados.create_dataframe_03120(), schema=None, preserve_index=False)

        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        blob = bucket.blob(destination)
        blob.upload_from_string(
                    buffer.read()
                    #dados.create_dataframe_03120() \
                    #.to_parquet(engine='pyarrow')
                ,'application/octet-stream'
            )
        destinations.append(destination)
        logging.info(f"Salvo no bucket {self.bucket_name} no caminho {destination}")
        
        # Cria o DataFrame e salva no formato Parquet
        destination = self.destination_parquet + f'/df_03130/{file.replace(".txt","-03130.parquet")}'
        logging.info(f'gravando {destination}')
        table = pa.Table.from_pandas(dados.create_dataframe_03130(), schema=None, preserve_index=False)
        
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        blob = bucket.blob(destination)
        blob.upload_from_string(
                    buffer.read()
                    #dados.create_dataframe_03130() \
                    #.to_parquet(engine='pyarrow')
                ,'application/octet-stream'
            )
        destinations.append(destination)
        logging.info(f"Salvo no bucket {self.bucket_name} no caminho {destination}")
        
        # Cria o DataFrame e salva no formato Parquet
        destination = self.destination_parquet + f'/df_03111/{file.replace(".txt","-03111.parquet")}'
        logging.info(f'gravando {destination}')
        table = pa.Table.from_pandas(dados.create_dataframe_03111(), schema=None, preserve_index=False)
        
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        blob = bucket.blob(destination)
        blob.upload_from_string(
                    buffer.read()
                    #dados.create_dataframe_03111() \
                    #.to_parquet(engine='pyarrow')
                ,'application/octet-stream'
            )
        destinations.append(destination)
        logging.info(f"Salvo no bucket {self.bucket_name} no caminho {destination}")

        # Cria o DataFrame e salva no formato Parquet
        destination = self.destination_parquet + f'/df_03112/{file.replace(".txt","-03112.parquet")}'
        logging.info(f'gravando {destination}')
        table = pa.Table.from_pandas(dados.create_dataframe_03112(), schema=None, preserve_index=False)

        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        blob = bucket.blob(destination)
        blob.upload_from_string(
                    buffer.read()
                    #dados.create_dataframe_03112() \
                    #.to_parquet(engine='pyarrow')
                ,'application/octet-stream'
            )
        destinations.append(destination)
        logging.info(f"Salvo no bucket {self.bucket_name} no caminho {destination}")
        
        # Cria o DataFrame e salva no formato Parquet
        destination = self.destination_parquet + f'/df_03500/{file.replace(".txt","-03500.parquet")}'
        logging.info(f'gravando {destination}')
        table = pa.Table.from_pandas(dados.create_dataframe_03500(), schema=None, preserve_index=False)

        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        blob = bucket.blob(destination)
        blob.upload_from_string(
                    buffer.read()
                    #dados.create_dataframe_03500() \
                    #.to_parquet(engine='pyarrow')
                ,'application/octet-stream'
            )
        destinations.append(destination)
        logging.info(f"Salvo no bucket {self.bucket_name} no caminho {destination}")
        
        # Cria o DataFrame e salva no formato Parquet
        destination = self.destination_parquet + f'/df_contribuintes/{file.replace(".txt","-contribuintes.parquet")}'
        logging.info(f'gravando {destination}')
        table = pa.Table.from_pandas(dados.create_dataframe_contribuintes(), schema=None, preserve_index=False)

        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        blob = bucket.blob(destination)
        blob.upload_from_string(
                    buffer.read()
                    #dados.create_dataframe_contribuintes() \
                    #.to_parquet(engine='pyarrow')
                ,'application/octet-stream'
            )
        destinations.append(destination)
        logging.info(f"Salvo no bucket {self.bucket_name} no caminho {destination}")
        logging.info(f"""=======================================================
                     
                     Lista de destinos: {destination} 
                     
                     ===========================================================
                     """)
        
        return destinations