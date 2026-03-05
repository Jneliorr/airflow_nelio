"""

Esse job recebe o arquivo tratado pelo job_dasn_filter.py e realiza a apuração das tabelas dasnpag e daspasg, para verificar
as dasn's que foram pagas e as dasn's que ainda nao foram pagas.

"""


from pyspark.sql import SparkSession
import pyspark.sql.functions as F
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

URL_DEST_RESULTADO = f"gs://{BUCKET_NAME}/curated/{MUNICIPIO}/simples_nacional/dasn/dasn_pagamentos"
URL_DASN_FILTERED = f"gs://{BUCKET_NAME}/curated/{MUNICIPIO}/simples_nacional/dasn/dasn_guias"
URL_DASN_DETALHES = f"gs://{BUCKET_NAME}/curated/{MUNICIPIO}/simples_nacional/dasn/dasn_detalhes"

spark = SparkSession.builder \
        .appName('pgdas_fiscalizacao') \
        .config("spark.sql.legacy.parquet.nanosAsLong", "true") \
        .getOrCreate()
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.bigquery.tempGcsBucket", "tmpbucket-e")

def convert_daspag_schema(df: DataFrame, create_view: bool = True) -> DataFrame:
    """
    Converts the columns of the DASNPAG DataFrame to the correct types and formats.

    :param df: The DataFrame to be converted.
    :param create_view: If True, the converted DataFrame is registered as a view named "tb_daspag".
    :return: The converted DataFrame if create_view is False.
    """
    df_daspag = (
        df
        # Convert the valor_total_das to float and divide it by 100 to get the correct value
        .withColumn('valor_total_das', col('valor_total_das').cast("float") / 100 )
        # Convert the data_arrecadacao to date type
        .withColumn('data_arrecadacao', F.to_date(col('data_arrecadacao'), format='yyyyMMdd'))
    )
    if create_view:
        df_daspag.createOrReplaceTempView("tb_daspag")
    else:
        return df_daspag

## Leitura e criação da view das dasn_detalhes
## Esse path recebe o arquivo tratado pelo job_dasn_filter.py
dasn_detalhes = spark.read.parquet(URL_DASN_DETALHES)
dasn_detalhes.createOrReplaceTempView("tb_01100")

## Leitura e criação da view das dasn_detalhes
## Esse path recebe o arquivo tratado pelo job_dasn_filter.py
dasn_geral = spark.read.parquet(URL_DASN_FILTERED)
dasn_geral.createOrReplaceTempView("tb_01000")
dasn_geral_com_pa = spark.sql("""
    SELECT t1.*, t2.PA FROM tb_01000 t1
    LEFT JOIN (SELECT DISTINCT PA, NumDas FROM  tb_01100) t2 ON t1.NumDas = t2.NumDas
    """)
dasn_geral_com_pa.createOrReplaceTempView('tb_dans_pa')


## Pagamentos da Dasn Geral (DASNPAG)
df_daspag = spark.read \
    .option("recursiveFileLookup", "true") \
    .option("mergeSchema", "true") \
    .option("pathGlobFilter", "*.parquet") \
    .parquet("gs://k8s-dataita/staged/simples_nacional/daspag/waiting/*/content/")

# Conversão dos esquemas de data e valores e criação da view tb_daspag
convert_daspag_schema(df_daspag)


spark.sql("""
    SELECT 
        A.*,
        B.data_arrecadacao,
        B.codigo_banco,
        B.valor_total_das as Vlpago,
        B.numero_daf607
    FROM tb_dans_pa A  
    LEFT JOIN tb_daspag B on A.NumDas = B.numero_das

    """).write.mode('overwrite').parquet(URL_DEST_RESULTADO)

spark.stop()
print("=============== Job finalizado com sucesso! ===============")