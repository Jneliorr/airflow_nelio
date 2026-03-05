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
BUKET_NAME = os.environ.get('BUCKET_NAME')


url_00000 = f"gs://{BUKET_NAME}/curated/{MUNICIPIO}/simples_nacional/pgdas/filtered/contribuinte_apuracao_00000"
url_02000 = f"gs://{BUKET_NAME}/curated/{MUNICIPIO}/simples_nacional/pgdas/filtered/rbt12_02000"
url_03000 = f"gs://{BUKET_NAME}/curated/{MUNICIPIO}/simples_nacional/pgdas/filtered/estabelecimentos_03000"
url_03100 = f"gs://{BUKET_NAME}/curated/{MUNICIPIO}/simples_nacional/pgdas/filtered/receita_atividade_03100"
url_03110 = f"gs://{BUKET_NAME}/curated/{MUNICIPIO}/simples_nacional/pgdas/filtered/receita_atividade_fa_03110"
url_03120 = f"gs://{BUKET_NAME}/curated/{MUNICIPIO}/simples_nacional/pgdas/filtered/receita_atividade_fb_03120"
url_03130 = f"gs://{BUKET_NAME}/curated/{MUNICIPIO}/simples_nacional/pgdas/filtered/receita_atividade_fc_03130"
url_03111 = f"gs://{BUKET_NAME}/curated/{MUNICIPIO}/simples_nacional/pgdas/filtered/receita_isencao_03111"
url_03112 = f"gs://{BUKET_NAME}/curated/{MUNICIPIO}/simples_nacional/pgdas/filtered/receita_reducao_03112"



spark = SparkSession.builder \
            .appName('pgdas_filter_00000') \
        .getOrCreate()
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.bigquery.tempGcsBucket", "tmpbucket-e")

def utilma_valida_municipio(df, cod_tom='5837', if_create_view=True):
    # Definindo a janela para calcular o maior sequencial por cnpj_raiz e periodo_apuracao

    janela = Window.partitionBy("CNPJ", "PA").orderBy(desc("id_pgdas"))
    df_resultado  =(
            df.withColumn("rank", row_number().over(janela))
            .filter(col("Cod_TOM") == cod_tom) # Atribuindo ranking por grupo
            .filter(col("rank") == 1)  # Pegando apenas o maior sequencial
            .drop("rank")  # Removendo a coluna auxiliar
        )
    if if_create_view:
        df_resultado.createOrReplaceTempView('tb_estabelecimento')
        print('crianda view tb_estabelecimento')
    else:
        return df_resultado

df_00000 = spark.read.parquet(f'gs://{BUKET_NAME}/staged/simples_nacional/pgdas/ready/df_00000/*.parquet')
df_02000 = spark.read.parquet(f'gs://{BUKET_NAME}/staged/simples_nacional/pgdas/ready/df_02000/*.parquet')
df_03000 = spark.read.parquet(f'gs://{BUKET_NAME}/staged/simples_nacional/pgdas/ready/df_03000/*.parquet')
df_03100 = spark.read.parquet(f'gs://{BUKET_NAME}/staged/simples_nacional/pgdas/ready/df_03100/*.parquet')
df_03110 = spark.read.parquet(f'gs://{BUKET_NAME}/staged/simples_nacional/pgdas/ready/df_03110/*.parquet')
df_03120 = spark.read.parquet(f'gs://{BUKET_NAME}/staged/simples_nacional/pgdas/ready/df_03120/*.parquet')
df_03130 = spark.read.parquet(f'gs://{BUKET_NAME}/staged/simples_nacional/pgdas/ready/df_03130/*.parquet')
df_03111 = spark.read.parquet(f'gs://{BUKET_NAME}/staged/simples_nacional/pgdas/ready/df_03111/*.parquet')
df_03112 = spark.read.parquet(f'gs://{BUKET_NAME}/staged/simples_nacional/pgdas/ready/df_03112/*.parquet')

utilma_valida_municipio(df_03000)

df_00000.createOrReplaceTempView("tb_00000")
df_02000.createOrReplaceTempView("tb_02000")

df_03100.createOrReplaceTempView("tb_03100")
df_03110.createOrReplaceTempView("tb_03110")
df_03120.createOrReplaceTempView("tb_03120")
df_03130.createOrReplaceTempView("tb_03130")
df_03111.createOrReplaceTempView("tb_03111")
df_03112.createOrReplaceTempView("tb_03112")

df_00000_filter = spark.sql("""
    SELECT A.* FROM tb_00000 A
    INNER JOIN tb_estabelecimento B ON A.Pgdasd_ID_Declaracao = B.id_pgdas

""")

df_02000_filter = spark.sql("""
    SELECT A.* FROM tb_02000 A
    INNER JOIN tb_estabelecimento B ON A.id_pgdas = B.id_pgdas

""")

df_03000_filter = spark.sql("""
    SELECT * FROM tb_estabelecimento 

""")

df_03100_filter = spark.sql("""
    SELECT A.* FROM tb_03100 A
    inner join tb_estabelecimento B on a.id_pgdas = B.id_pgdas AND a.CNPJ = B.CNPJ

""")

df_03110_filter = spark.sql("""
    SELECT A.* FROM tb_03110 A
    INNER JOIN tb_estabelecimento B ON A.id_pgdas = B.id_pgdas AND a.CNPJ = B.CNPJ

""")

df_03120_filter = spark.sql("""
    SELECT A.* FROM tb_03120 A
    INNER JOIN tb_estabelecimento B ON A.id_pgdas = B.id_pgdas AND a.CNPJ = B.CNPJ

""")

df_03130_filter = spark.sql("""
    SELECT A.* FROM tb_03130 A
    INNER JOIN tb_estabelecimento B ON A.id_pgdas = B.id_pgdas AND a.CNPJ = B.CNPJ

""")

df_03111_filter = spark.sql("""
    SELECT A.* FROM tb_03111 A
    INNER JOIN tb_estabelecimento B ON A.id_pgdas = B.id_pgdas AND a.CNPJ = B.CNPJ

""")

df_03112_filter = spark.sql("""
    SELECT A.* FROM tb_03112 A
    INNER JOIN tb_estabelecimento B ON A.id_pgdas = B.id_pgdas AND a.CNPJ = B.CNPJ

""")


df_00000_filter.write.mode('overwrite').parquet(url_00000)
df_02000_filter.write.mode('overwrite').parquet(url_02000)
df_03000_filter.write.mode('overwrite').parquet(url_03000)
df_03100_filter.write.mode('overwrite').parquet(url_03100)
df_03110_filter.write.mode('overwrite').parquet(url_03110)
df_03120_filter.write.mode('overwrite').parquet(url_03120)
df_03130_filter.write.mode('overwrite').parquet(url_03130)
df_03111_filter.write.mode('overwrite').parquet(url_03111)
df_03112_filter.write.mode('overwrite').parquet(url_03112)
