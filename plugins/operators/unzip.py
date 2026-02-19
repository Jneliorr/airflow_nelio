from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.google.cloud.hooks.gcs import GCSHook    


import logging
from io import BytesIO
from zipfile import ZipFile


class UnzipGCS(BaseOperator):
    template_fields = ("bucket_name", "prefix") 
    @apply_defaults
    def __init__(self,bucket_name='k8s-dataita', prefix='raw/cnpj/2025-05/estabelecimentos',*args, **kwargs):   
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.prefix = prefix

        
    def execute(self, context):

        hook = GCSHook(gcp_conn_id="google_cloud_default")
        logging.info(f"Listando arquivos em: {self.prefix}")
        files =hook.list(bucket_name = self.bucket_name, prefix=self.prefix, delimiter=".zip" )
        logging.info(f'connect in bucket: {self.bucket_name}')
        logging.info(f'list of files: {files}')

         
        
        path_descompacted_files = f'tmp/{self.prefix}/'
        for i in range(len(files)):
            zip_data = BytesIO(hook.download(self.bucket_name, files[i]))
            logging.info(f'{zip_data}')
            with ZipFile(zip_data, 'r') as zip_ref:
                for file in zip_ref.namelist():
                    logging.info(f'{file}')
                    if file.endswith('.zip'):
                        continue
                    file_data = zip_ref.read(file)
                    logging.info(f'Uploading file: {path_descompacted_files + file}')
                    hook.upload(self.bucket_name,object_name= path_descompacted_files + file, data=file_data)


        return path_descompacted_files
    
    
    
class UnzipGCS_v2(BaseOperator):
    template_fields = ("bucket_name", "prefix") 
    @apply_defaults
    def __init__(self,bucket_name='k8s-dataita', prefix='raw/cnpj/2025-05/estabelecimentos', destination='tmp', *args, **kwargs):   
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.destination = destination

        
    def execute(self, context):

        hook = GCSHook(gcp_conn_id="google_cloud_default")
        logging.info(f"Listando arquivos em: {self.prefix}")
        files =hook.list(bucket_name = self.bucket_name, prefix=self.prefix, delimiter=".zip" )
        logging.info(f'connect in bucket: {self.bucket_name}')
        logging.info(f'list of files: {files}')

         
        
        path_descompacted_files = f'tmp/{self.prefix}/'
        for i in range(len(files)):
            zip_data = BytesIO(hook.download(self.bucket_name, files[i]))
            logging.info(f'{zip_data}')
            with ZipFile(zip_data, 'r') as zip_ref:
                for file in zip_ref.namelist():
                    logging.info(f'{file}')
                    if file.endswith('.zip'):
                        continue
                    file_data = zip_ref.read(file)
                    logging.info(f'Uploading file: {path_descompacted_files + file}')
                    hook.upload(self.bucket_name,object_name= path_descompacted_files + file, data=file_data)


        return path_descompacted_files