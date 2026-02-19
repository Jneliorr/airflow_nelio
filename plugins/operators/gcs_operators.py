
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.google.cloud.hooks.gcs import GCSHook    
from airflow.models.variable import Variable
import logging

from google.cloud import bigquery

credentials = GCSHook().get_credentials()
project_id = Variable.get('project_id')

class GCSListObjectsOperators(BaseOperator): 
    template_fields = ("prefix", "bucket_name")
    @apply_defaults 
    def __init__(self, prefix, bucket_name=None, gcp_conn_id='gcs_default', delimiter=None, **kwargs): 
        super().__init__(**kwargs)
        self.prefix = prefix
        self.bucket_name = bucket_name
        self.delimiter = delimiter
        self.gcp_conn_id = gcp_conn_id
        self.kwargs = kwargs

    def execute(self, context): 
        
        #params : ParamsDict = kwargs["params"]  

        hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
        ## Get all zips in folder e subfolders 
        files = hook.list(self.bucket_name, prefix=self.prefix, delimiter=self.delimiter  )
        list_name_files = [file.split('/')[-1] for file in files]
        logging.info(list_name_files)
               
        #metadata_decred = {'file_name': list_name_files, 'path_files': files}
  
        return files
    
    
    
class GCSParquetToGBQ(BaseOperator):
    
    template_fields = ("bucket_name", "table_id", "dataset")
    @apply_defaults
    def __init__(self,
                 prefix=None, 
                 bucket_name=None,
                 dataset = None,
                 table_id=None,
                 **kwargs):
        super().__init__(**kwargs)
        
        self.prefix = prefix
        self.bucket_name = bucket_name 
        self.table_id = table_id
        self.dataset = dataset
        self.kwargs = kwargs
        
    def execute(self, context):
        
        #logging.info(f'dataset dest {self.dataset}')
        logging.info(f'bucket src {self.bucket_name}')
        logging.info(f'prefix src {self.prefix}')
        bq_client = bigquery.Client(credentials=credentials)
        table_ref = f"{Variable.get('project_id')}.{self.dataset}.{self.table_id}"
        logging.info(f'table_ref: {table_ref}')
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
            )
        load_job = bq_client.load_table_from_uri(
            f"gs://{self.bucket_name}/{self.prefix}/*.parquet", table_ref, job_config=job_config
        )
        
        load_job.result()
        return "finalizado {self.prefix}"