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



def apuracao(spark=spark):

    df = spark.sql(f""" 
        WITH pgdas_app AS (
        -- Seleciona os dados da tabela de apuração, extraindo o CNPJ_BASICO 
        SELECT 
            Pgdasd_ID_Declaracao,
            Cnpjmatriz,
            Cnpjmatriz,
            PA,
            Rpa,
            Operacao,
            Regime,
            RpaC,
            Pgdasd_Dt_Transmissao,
            LEFT(Cnpjmatriz, 8) AS CNPJ_BASICO
        FROM tb_00000
        ),
        pgdas_filtro as (
        SELECT id_pgdas FROM tb_estabelecimento
        )

        -- Seleciona os dados das declarações
        SELECT 
            pgdas_app.Pgdasd_ID_Declaracao,
            pgdas_app.Pgdasd_Dt_Transmissao,
            pgdas_app.Cnpjmatriz,
            pgdas_app.PA,
            pgdas_app.Rpa,
            pgdas_app.Operacao,
            pgdas_app.Regime,
            pgdas_app.RpaC

        -- Faz a junção das tabelas
        FROM pgdas_app
        INNER JOIN
        pgdas_filtro ON pgdas_filtro.id_pgdas = pgdas_app.Pgdasd_ID_Declaracao;
        """
        )
    return df
        
def utilma_valida(df, MUNICIPIO= MUNICIPIO, if_create_view=True):
    # Definindo a janela para calcular o maior sequencial por cnpj_raiz e periodo_apuracao

    janela = Window.partitionBy("CNPJ", "PA").orderBy(desc("id_pgdas"))
    df_resultado  =(
            df.withColumn("rank", row_number().over(janela))
            .filter(col("Cod_TOM") == MUNICIPIO) # Atribuindo ranking por grupo
            .filter(col("rank") == 1)  # Pegando apenas o maior sequencial
            .drop("rank")  # Removendo a coluna auxiliar
        )
    if if_create_view:
        df_resultado.createOrReplaceTempView('tb_estabelecimento')
        print('crianda view tb_estabelecimento')
    else:
        return df_resultado
    return df_resultado

def execute_pgdas_fisc(spark=spark, url=DEST_URL, MUNICIPIO= MUNICIPIO):
    
 
    df_00000 = spark.read.parquet('gs://k8s-dataita/staged/simples_nacional/pgdas/ready/df_00000/df_00000_202503_ajustado.parquet')
    df_03000 = (
        spark.read.format("parquet")
        .load("gs://k8s-dataita/staged/simples_nacional/pgdas/ready/df_03000/df_03000_202503.parquet")
    )

    df_00000.createOrReplaceTempView("tb_00000")
    df_03000.createOrReplaceTempView("tb_03000")
    
    utilma_valida(df=df_03000, MUNICIPIO=MUNICIPIO, if_create_view=True)
    df = apuracao(spark=spark)
    

    df.write.mode('overwrite').parquet(url)
    return f'Save parquet file in {url}'


execute_pgdas_fisc(spark=spark, MUNICIPIO=MUNICIPIO, url = DEST_URL)