from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, isnan, isnull, from_unixtime
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, IntegerType, DoubleType
    )
from pyspark.sql.window import Window
from pyspark.sql.functions import col, to_timestamp, when, row_number, desc, max
from pyspark.sql import DataFrame
from functools import reduce
import os 


MUNICIPIO = os.environ.get('MUNICIPIO', '5837')
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'nelio_teste')
DEST_URL1 = f"gs://{BUCKET_NAME}/curated/{MUNICIPIO}/simples_nacional/dasn/dasn_guias"
DEST_URL2 = f"gs://{BUCKET_NAME}/curated/{MUNICIPIO}/simples_nacional/dasn/dasn_detalhes"


spark = SparkSession.builder \
        .appName('pgdas_fiscalizacao') \
        .config("spark.sql.legacy.parquet.nanosAsLong", "true") \
        .getOrCreate()
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.bigquery.tempGcsBucket", "tmpbucket-e")



def convert_nanos_to_timestamp_optimized(df: DataFrame, col_names: list) -> DataFrame:
    """
    Versão otimizada usando funções nativas do Spark
    Converte nanossegundos para timestamp em múltiplas colunas
    Args:
        df (DataFrame): DataFrame do PySpark
        col_names (list): Lista de nomes das colunas a serem convertidas
    Returns:
        DataFrame: DataFrame com as colunas convertidas
    """
    def convert_column(df, col_name):
        return df.withColumn(
            col_name,
            when(
                col(col_name).isNull() | 
                isnull(col(col_name)) | 
                (col(col_name) <= 0),
                None
            ).otherwise(
                from_unixtime(col(col_name) / 1_000_000_000)
            )
        )
    
    return reduce(convert_column, col_names, df)


def dasn_filter(spark=spark, url1=DEST_URL1,url2=DEST_URL2, MUNICIPIO= MUNICIPIO):
    
    dasn_01100 = (
        spark.read.option("mergeSchema", "true") \
        .option("pathGlobFilter", "*.parquet") \
        .option("recursiveFileLookup", "true") \
        .parquet("gs://k8s-dataita/staged/simples_nacional/dasn/parquet/*/dasn_info_01100/")
        )
    

    dasn_01000 = (spark.read \
        .option("pathGlobFilter", "*.parquet") \
        .option("recursiveFileLookup", "true") \
        .parquet('gs://k8s-dataita/staged/simples_nacional/dasn/parquet/*/dasn_apuracao_01000/')
    )

    # dasn_01000 = convert_nanos_to_timestamp_optimized(spark.read \
    #     .option("pathGlobFilter", "*.parquet") \
    #     .option("recursiveFileLookup", "true") \
    #     .parquet('gs://k8s-dataita/staged/simples_nacional/dasn/parquet/*/dasn_apuracao_01000/'),
    # ['DtgGeracao', 'Dtvenc', 'Dtvalcalc']
    # )


    dasn_01000.createOrReplaceTempView("tb_01000") 
    dasn_01100.createOrReplaceTempView("tb_01100")


    d_01100 = spark.sql(f"""
        
        SELECT * FROM tb_01100
        WHERE cod_munic = {MUNICIPIO}

        """)

    d_01100.createOrReplaceTempView("tb_numdas")


    d_01000 = spark.sql("""
    
        SELECT tb_01000.* FROM tb_01000
        INNER JOIN tb_numdas ON tb_01000.NumDas = tb_numdas.NumDas
        
        """).distinct()

    d_01000.write.mode('overwrite').parquet(url1)
    d_01100.write.mode('overwrite').parquet(url2) 
    

dasn_filter(spark=spark, MUNICIPIO=MUNICIPIO, url1=DEST_URL1,url2=DEST_URL2)

spark.stop()
print("=============== Job finalizado com sucesso! ===============")