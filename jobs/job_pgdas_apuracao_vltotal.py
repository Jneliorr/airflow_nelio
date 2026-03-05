from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, IntegerType, DoubleType
    )
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, desc, max
from pyspark.sql.functions import col, to_timestamp, when
import os 


MUNICIPIO = os.environ.get('MUNICIPIO', '5837')
DEST_URL = os.environ.get('DEST_URL')



spark = SparkSession.builder \
        .appName('pgdas_filter_00000') \
        .getOrCreate()
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.bigquery.tempGcsBucket", "tmpbucket-e")



def vltotal(spark=spark):

    df = spark.sql(f""" 
WITH pgdas_validas AS (
--SELECT NO RESULTADO DA FUNÇÃO PARA PEGAR O ID PGDAS MAIS AUTAL PARA O MUNICIPIO SELECIONADO
SELECT Pgdasd_ID_Declaracao FROM tb_idpgdas
),

rec_estabelecimento as(
--VALOR TOTAL DA RECEITA PARA CADA ATIVIDADE DA EMPRESA DO SN QUE VAI DE 01 A 43
SELECT * FROM tb_03100 

)

SELECT rec_estabelecimento.* FROM rec_estabelecimento
INNER JOIN pgdas_validas ON pgdas_validas.Pgdasd_ID_Declaracao = rec_estabelecimento.id_pgdas
"""
        )
    return df
        

def execute_pgdas_vltotal(spark=spark, url=DEST_URL, MUNICIPIO= MUNICIPIO):
    
 
    df_03100 = spark.read.parquet('gs://k8s-dataita/staged/simples_nacional/pgdas/ready/df_03100/df_03100_202502.parquet')
    df_idPgdas = spark.read.parquet('gs://nelio_teste/curated/simples_nacional/pgdas/apuracao/*.parquet')






    df_03100.createOrReplaceTempView("tb_03100")
    df_idPgdas.createOrReplaceTempView("tb_idpgdas")
    
    # utilma_valida(df=df_03000, MUNICIPIO=MUNICIPIO, if_create_view=True)
    df = vltotal(spark=spark)
    

    df.write.mode('overwrite').parquet(url)
    return f'Save parquet file in {url}'


execute_pgdas_vltotal(spark=spark, MUNICIPIO=MUNICIPIO, url = DEST_URL)