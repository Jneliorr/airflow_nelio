from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import os
from airflow.providers.google.cloud.hooks.gcs import GCSHook

#######CONEXÕES E CREDENCIAIS###########
conn_id = "gcs_default"
hook = GCSHook(gcp_conn_id=conn_id)
credentials = hook.get_credentials()
def convert_date(df):
        df = df.copy()
        for col in df.columns:
            if col.upper().startswith('DATA'):
                df[col] = pd.to_datetime(df[col], format='%Y%m%d', errors='coerce')

        return df

def transforms(df):
    
    df = df.copy()

    # Convert String cnae to array
    #df['CNAE_SECUNDARIA'] = df['CNAE_SECUNDARIA'].str.split(',')
    df['CNPJ_COMPLETO'] = df['CNPJ_BASICO'] + df['CNPJ_ORDEM'] + df['CNPJ_DV']

    # convert dates
    df = convert_date(df)

    return df


def remove_table(project, dataset, table):

    table_id = f"""{project}.{dataset}.{table}"""
    
    client = bigquery.Client(credentials=credentials)

    # TODO(developer): Set table_id to the ID of the table to fetch.
    #table_id = 'infra-itaborai.teste.cnpj'

    # If the table does not exist, delete_table raises
    # google.api_core.exceptions.NotFound unless not_found_ok is True.
    client.delete_table(table_id, not_found_ok=True)  # Make an API request.
    print("Deleted table '{}'.".format(table_id))
    
### ELT Arquivos de Pagamento DASn
def get_path_files(path, bucket=None, cloud=True):
    if cloud:
        #print([bucket, path,cloud])
        client = storage.Client(credentials=credentials)
        bucket = client.bucket(bucket_name=bucket)
        blobs = ['gs://' + blob.id[:-(len(str(blob.generation)) + 1)] for blob in bucket.list_blobs(prefix=path) if not blob.name.endswith('/')]
        
        return blobs
    else:
        files = os.listdir(path)
        files = [os.path.join(path, file) for file in files]
        return files
    
    
        