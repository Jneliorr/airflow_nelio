from functools import reduce
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col
import pyspark.sql.functions as f
import os

from google.cloud import storage
#from config.auth import credentials

def get_path_files(path, bucket=None, cloud=True):
    if cloud:
        #print([bucket, path,cloud])
        client = storage.Client()
        bucket = client.bucket(bucket_name=bucket)
        blobs = ['gs://' + blob.id[:-(len(str(blob.generation)) + 1)] for blob in bucket.list_blobs(prefix=path) if not blob.name.endswith('/')]
        
        return blobs
    else:
        files = os.listdir(path)
        files = [os.path.join(path, file) for file in files]
        return files


def cnpj_estabeleciemntos(spark, path_root):

    ## Reading files

    colunas_estabelecimento_schema = StructType([
    StructField("CNPJ_BASICO", StringType(), True),
    StructField("CNPJ_ORDEM", StringType(), True),
    StructField("CNPJ_DV", StringType(), True),
    StructField("MATRIZ_FILIAL", StringType(), True),
    StructField("NOME_FANTASIA", StringType(), True),
    StructField("SITUACAO_CADASTRAL", StringType(), True),
    StructField("DATA_SITUACAO_CADASTRAL", StringType(), True),
    StructField("MOTIVO_SITUACAO", StringType(), True),
    StructField("NOME_CIDADE_EXT", StringType(), True),
    StructField("PAIS", StringType(), True),
    StructField("Data_Inicio_Atividade", StringType(), True),
    StructField("CNAE_PRINCIPAL", StringType(), True),
    StructField("CNAE_SECUNDARIA", StringType(), True),
    StructField("TIPO_LOGRADOURO", StringType(), True),
    StructField("LOGRADOURO", StringType(), True),
    StructField("NUM", StringType(), True),
    StructField("COMPLEMENTO", StringType(), True),
    StructField("BAIRRO", StringType(), True),
    StructField("CEP", StringType(), True),
    StructField("UF", StringType(), True),
    StructField("MUNICIPIO", StringType(), True),
    StructField("DDD1", StringType(), True),
    StructField("TEL1", StringType(), True),
    StructField("DDD2", StringType(), True),
    StructField("TEL2", StringType(), True),
    StructField("DDD_FAX", StringType(), True),
    StructField("TEL_FAX", StringType(), True),
    StructField("E_MAIL", StringType(), True),
    StructField("SITUACAO_ESPECIAL", StringType(), True),
    StructField("DATA_SIT_ESPECIAL", StringType(), True)
    ])
    
    bucket = os.environ.get("BUCKET", "k8s-dataita")

    list_df = []
    list_files =get_path_files(path=path_root, cloud=True, bucket=bucket)
    for file in list_files:
        print(file)
        list_df.append(spark.read.csv(file, sep=";", 
                        header=False, 
                        inferSchema=False, 
                        schema=colunas_estabelecimento_schema,
                        encoding="ISO-8859-1"))
        

    df = reduce(lambda df1, df2: df1.union(df2), list_df)

    ## Trasnforms Estabeleciemntos
    df = df.withColumn('CNPJ_COMPLETO', f.concat(col('CNPJ_BASICO'),  col('CNPJ_ORDEM'),  col('CNPJ_DV')))
    df = df.withColumn('DATA_SITUACAO_CADASTRAL', f.to_date("DATA_SITUACAO_CADASTRAL", "yyyyMMdd"))
    df = df.withColumn('Data_Inicio_Atividade', f.to_date("Data_Inicio_Atividade", "yyyyMMdd"))
    df = df.withColumn('DATA_SIT_ESPECIAL',f.to_date("DATA_SIT_ESPECIAL", "yyyyMMdd"))


    ### Load
    project_id = os.environ.get("PROJECT_ID", "infra-itaborai")
    dataset_id =  os.environ.get("DATASET", "mds_cnpj")
    table_id = "estabelecimentos"
    table_full_id = f"{project_id}.{dataset_id}.{table_id}"

    df.write.format("bigquery").option("table", table_full_id).option("temporaryGcsBucket","tmpbucket-e").mode("overwrite").save()

    return "Arquivo Carregado com sucesso!"


if __name__ == "__main__":


    spark = SparkSession.builder \
        .appName('cnpj') \
        .getOrCreate()
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    spark.conf.set("spark.bigquery.tempGcsBucket", "tmpbucket-e")	
    print(os.environ.get("PATH_ROOT"))
    print(os.environ.get("DATASET"))
    cnpj_estabeleciemntos(spark=spark, path_root=os.environ.get("PATH_ROOT"))
