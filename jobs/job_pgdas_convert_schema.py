from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, DoubleType
from google.cloud import storage
from functools import reduce

import os 

# Configuração da Sessão do Spark
# Dependencias do Delta + configuracoes de log33
spark = (
    SparkSession
    .builder
    #.master("local[*]")
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "true")
    .config("spark.databricks.delta.checkpointInterval", "10")
    .config("spark.delta.logLevel", "DEBUG")
    .config("spark.sql.legacy.parquet.nanosAsLong", "true")
    .getOrCreate()
)



bucket = os.environ.get("BUCKET")
path = os.environ.get("PATH_ROOT")
destination = os.environ.get("DESTINATION")
partition = os.environ.get("PA")



print("== Conferindo Variaveis de Ambiente")
print(f"BUCKET: {bucket}")
print(f"PATH_ROOT: {path}")
print(f"DESTINATION: {destination}")
print(f"PARTITION: {partition}")

print(("==============================================="))


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
    
my_files = get_path_files(path, bucket, cloud=True)


print(f"== Encontrados {len(my_files)} arquivos no bucket {bucket} com o prefixo {path}")
print(f"== Encontrados {my_files[:5]} ")

#url_base = my_files[0].split('/')
#output_uri_elemetes = my_files[0].split("/")
#output_uri_base = '/'.join([output_uri_elemetes[3], output_uri_elemetes[4], output_uri_elemetes[5], 'converted'])

df_00000_list = []
df_00001_list = []
df_01000_list = []
df_01100_list = []
df_01500_list = []
df_01501_list = []
df_01502_list = []
df_02000_list = []
df_03000_list = []
df_03100_list = []
df_03110_list = []
df_03111_list = []
df_03112_list = []
df_03120_list = []
df_03130_list = []
df_03500_list = []
df_aaaaa_list = []
df_contribuintes_list = []

for file in my_files:
    if "df_00000" in file:
        df_00000_list.append(spark.read.parquet(file))
    elif "df_00001" in file:
        df_00001_list.append(spark.read.parquet(file))
    elif "df_01000" in file:
        df_01000_list.append(spark.read.parquet(file))        
    elif "df_01100" in file:
        df_01100_list.append(spark.read.parquet(file))        
    elif "df_01500" in file:        
        df_01500_list.append(spark.read.parquet(file))
    elif "df_01501" in file:
        df_01501_list.append(spark.read.parquet(file))
    elif "df_01502" in file:
        df_01502_list.append(spark.read.parquet(file))
    elif "df_02000" in file:
        df_02000_list.append(spark.read.parquet(file))
    elif "df_03000" in file:
        df_03000_list.append(spark.read.parquet(file))
    elif "df_03100" in file:
        df_03100_list.append(spark.read.parquet(file))
    elif "df_03110" in file: 
        df_03110_list.append(spark.read.parquet(file))
    elif "df_03111" in file:
        df_03111_list.append(spark.read.parquet(file))
    elif "df_03112" in file:
        df_03112_list.append(spark.read.parquet(file))
    elif "df_03120" in file:
        df_03120_list.append(spark.read.parquet(file))
    elif "df_03130" in file:
        df_03130_list.append(spark.read.parquet(file))
    elif "df_03500" in file:
        df_03500_list.append(spark.read.parquet(file))
    elif "df_aaaaa" in file:
        df_aaaaa_list.append(spark.read.parquet(file))
    elif "df_contribuintes" in file:
        df_contribuintes_list.append(spark.read.parquet(file))
    else:
        print(f"Arquivo {file} não corresponde a nenhum padrão conhecido.")

df_00000 = reduce(lambda df1, df2: df1.union(df2), df_00000_list)
df_00001 = reduce(lambda df1, df2: df1.union(df2), df_00001_list)
df_01000 = reduce(lambda df1, df2: df1.union(df2), df_01000_list)
df_01100 = reduce(lambda df1, df2: df1.union(df2), df_01100_list)
df_01500 = reduce(lambda df1, df2: df1.union(df2), df_01500_list)
df_01501 = reduce(lambda df1, df2: df1.union(df2), df_01501_list)
df_01502 = reduce(lambda df1, df2: df1.union(df2), df_01502_list)
df_02000 = reduce(lambda df1, df2: df1.union(df2), df_02000_list)
df_03000 = reduce(lambda df1, df2: df1.union(df2), df_03000_list)
df_03100 = reduce(lambda df1, df2: df1.union(df2), df_03100_list)   
df_03110 = reduce(lambda df1, df2: df1.union(df2), df_03110_list)
df_03111 = reduce(lambda df1, df2: df1.union(df2), df_03111_list)
df_03112 = reduce(lambda df1, df2: df1.union(df2), df_03112_list)
df_03120 = reduce(lambda df1, df2: df1.union(df2), df_03120_list)
df_03130 = reduce(lambda df1, df2: df1.union(df2), df_03130_list)
df_03500 = reduce(lambda df1, df2: df1.union(df2), df_03500_list)
df_aaaaa = reduce(lambda df1, df2: df1.union(df2), df_aaaaa_list)
df_contribuintes = reduce(lambda df1, df2: df1.union(df2), df_contribuintes_list)

tables_dict = {
    "df_00000": df_00000,
    "df_00001": df_00001,
    "df_01000": df_01000,
    "df_01100": df_01100,
    "df_01500": df_01500,
    "df_01501": df_01501,
    "df_01502": df_01502,
    "df_02000": df_02000,
    "df_03000": df_03000,
    "df_03100": df_03100,
    "df_03110": df_03110,
    "df_03111": df_03111,
    "df_03112": df_03112,
    "df_03120": df_03120,
    "df_03130": df_03130,
    "df_03500": df_03500,
    "df_aaaaa": df_aaaaa,
    "df_contribuintes": df_contribuintes
}


for df_name, df in tables_dict.items():
    for f in df.schema.fields:
        if isinstance(f.dataType, FloatType):
            df = df.withColumn(f.name, F.col(f.name).cast(DoubleType()))
    df.write.mode('overwrite').parquet(f'gs://{bucket}/{destination}/{df_name}/{df_name}_{partition}.parquet')
    print(f"== Arquivo {df_name}_{partition}.parquet convertido e salvo em {f'gs://{bucket}/{destination}/{df_name}/{df_name}_{partition}.parquet'}")

spark.stop()
print("== Spark session stopped.")