from pyspark.sql import SparkSession
from google.cloud import storage
import os 

def get_pa_from_path_gcs(bucket_name: str, prefix: str):
    """
    Obtém uma lista de blobs em um bucket GCS com um prefixo específico.
    
    Args:
        bucket_name (str): Nome do bucket no GCS
        prefix (str): Prefixo para filtrar os blobs
    
    Returns:
        List[storage.Blob]: Lista de blobs encontrados
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    
    parts = [blob.name.split('/')[4] for blob in blobs]
    unique_parts = set(parts)
    return unique_parts



# Configuração da Sessão do Spark
# Dependencias do Delta + configuracoes de log33
spark = (
    SparkSession
    .builder
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "true")
    .config("spark.databricks.delta.checkpointInterval", "10")
    .config("spark.delta.logLevel", "DEBUG")
    .config("spark.sql.legacy.parquet.nanosAsLong", "true")
    .getOrCreate()
)



bucket_name = os.environ.get("BUCKET", "k8s-dataita")
#path =  'staged/simples_nacional/dasn/parquet/202201'          #os.environ.get("PATH_ROOT")
#destination = 'staged/simples_nacional/dasn/ready/'    #os.environ.get("DESTINATION")
#partition = '202507'      #os.environ.get("PA")



print("== Conferindo Variaveis de Ambiente")
print(f"BUCKET: {bucket_name}")

    # lista_ano_mes = [
    #     202201, 202202, 202203, 202204, 202205, 202206, 202207, 202208, 202209, 202210, 202211, 202212,
    #     202301, 202302, 202303, 202304, 202305, 202306, 202307, 202308, 202309, 202310, 202311, 202312,
    #     202401, 202402, 202403, 202404, 202405, 202406, 202407, 202408, 202409, 202410, 202411, 202412,
    #     202501, 202502, 202503, 202504, 202505, 202506, 202507
    # ]

partitions = get_pa_from_path_gcs(bucket_name, "staged/simples_nacional/dasn/waiting/")
print(f"PARTITIONS ENCONTRADAS: {partitions}")

for partition in partitions:
    print(f"PARTITION: {partition}")
    dasn_01100 = (
        spark.read.option("mergeSchema", "true") \
        .option("pathGlobFilter", "*.parquet") \
        .option("recursiveFileLookup", "true") \
        .parquet(f"gs://{bucket_name}/staged/simples_nacional/dasn/waiting/{partition}/dasn_info_01100/")
        .dropDuplicates()

        )

    dasn_01000 = (
            spark.read.option("mergeSchema", "true") \
            .option("pathGlobFilter", "*.parquet") \
            .option("recursiveFileLookup", "true") \
            .parquet(f"gs://{bucket_name}/staged/simples_nacional/dasn/waiting/{partition}/dasn_apuracao_01000/")
            .dropDuplicates()
            )

    dasn_aaaaa = (
            spark.read.option("mergeSchema", "true") \
            .option("pathGlobFilter", "*.parquet") \
            .option("recursiveFileLookup", "true") \
            .parquet(f"gs://{bucket_name}/staged/simples_nacional/dasn/waiting/{partition}/dasn_file_aaaaa/")
            .dropDuplicates()
            )
    
    dasn_01100.write.mode('overwrite').parquet(f"gs://{bucket_name}/staged/simples_nacional/dasn/ready/dasn_info_01100/df_01100_{partition}")
    dasn_01000.write.mode('overwrite').parquet(f"gs://{bucket_name}/staged/simples_nacional/dasn/ready/dasn_apuracao_01000/df_01000_{partition}")
    dasn_aaaaa.write.mode('overwrite').parquet(f"gs://{bucket_name}/staged/simples_nacional/dasn/ready/dasn_file_aaaaa/df_aaaaa_{partition}")
    
print(("======================== Job finalizado com sucesso! ========================="))