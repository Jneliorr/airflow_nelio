from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, IntegerType, DoubleType
    )
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, desc, max

import os 

COD_STATUS = os.environ.get('COD_STATUS')
MUNICIPIO = os.environ.get('MUNICIPIO', '5837')
BUKET_NAME = os.environ.get('BUCKET_NAME')
TABLE_NAME = os.environ.get('TABLE_NAME')
URL_DEST = f"gs://{BUKET_NAME}/curated/{MUNICIPIO}/simples_nacional/pgdas/{TABLE_NAME}"

spark = SparkSession.builder \
        .appName('pgdas_fiscalizacao') \
        .getOrCreate()
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.bigquery.tempGcsBucket", "tmpbucket-e")


def find_iss_pgdas(spark=spark, COD_STATUS=None, MUNICIPIO='5837', PA_INIT=None, PA_END=None):
    """
    spark: SparkSession object
    cod_status: str, status code for ISS (e.g.,  
    '11' for retencao/substituicao tributaria,  
    '45' for isenção/redução,  
    '03' for lançamento de oficio,  
    '02' for exigibilidade suspensa,  
    '01' for imunidade,  
    '00' nao informado)  
    municipio: str, municipality code (e.g., '5837' for Itaboraí)
    PA_INIT: str, initial period for analysis (optional)
    PA_END: str, end period for analysis (optional)
    
    """ 
    if COD_STATUS == '45':
        return spark.sql(
        f"""
        WITH fa_03110 AS (
            SELECT 
                CNPJ,
                id_pgdas,
                tipo_ativ,
                UF AS UF_DEST,
                COALESCE(Cod_TOM, 'PAGO ORIGEM') AS Cod_TOM_DEST,
                Valor AS Valor_atividade,  
                COFINS,
                CSLL,
                ICMS,
                INSS,
                IPI,
                IRPJ,
                ISS,
                PIS,
                Vlimposto,
                Valor_apurado_de_COFINS,
                Valor_apurado_de_CSLL,
                Valor_apurado_de_ICMS,
                Valor_apurado_de_INSS,
                Valor_apurado_de_IPI,
                Valor_apurado_de_IRPJ,
                Aliquota_apurada_de_ISS,
                Valor_apurado_de_ISS,
                Valor_apurado_de_PIS,
                PA
            FROM tb_03110
        ),
        
        tf_03000 AS (
            SELECT 
            id_pgdas,
            Uf, 
            Cod_TOM
            FROM tb_03000),
        tf_03111 as (
            SELECT
            *
            FROM tb_03111
        ),
        
        tf_03112 as (
            SELECT
            *
            FROM tb_03112
        )
        SELECT 
           fa_03110.CNPJ,
           fa_03110.id_pgdas,
           fa_03110.tipo_ativ,
           fa_03110.Valor_atividade as Valor_Atividade,
           fa_03110.ISS,
           fa_03110.Aliquota_apurada_de_ISS,
           fa_03110.Valor_apurado_de_ISS,
           fa_03110.PA,
           tf_03000.Uf, 
           tf_03000.Cod_TOM,
           tf_03112.Valor as Valor_Red,
           tf_03112.Red as Perc_Red,
           tf_03111.Valor as Valor_Ex
        
        FROM fa_03110
        LEFT JOIN tf_03000  ON  fa_03110.id_pgdas = tf_03000.id_pgdas
        LEFT JOIN tf_03111 ON fa_03110.id_pgdas = tf_03111.id_pgdas and fa_03110.tipo_ativ = tf_03111.tipo_ativ
        LEFT JOIN tf_03112 ON fa_03110.id_pgdas = tf_03112.id_pgdas and fa_03110.tipo_ativ = tf_03112.tipo_ativ
        
        WHERE ISS = {COD_STATUS} and Cod_TOM = {MUNICIPIO} and Cod_TOM_DEST = '' 
                
        """
        )
        
    else:
        return spark.sql(
        f"""
        WITH fa_03110 AS (
            SELECT 
                CNPJ,
                id_pgdas,
                tipo_ativ,
                UF AS UF_DEST,
                COALESCE(Cod_TOM, 'PAGO ORIGEM') AS Cod_TOM_DEST,
                Valor AS Valor_atividade,  
                COFINS,
                CSLL,
                ICMS,
                INSS,
                IPI,
                IRPJ,
                ISS,
                PIS,
                Vlimposto,
                Valor_apurado_de_COFINS,
                Valor_apurado_de_CSLL,
                Valor_apurado_de_ICMS,
                Valor_apurado_de_INSS,
                Valor_apurado_de_IPI,
                Valor_apurado_de_IRPJ,
                Aliquota_apurada_de_ISS,
                Valor_apurado_de_ISS,
                Valor_apurado_de_PIS,
                PA
            FROM tb_03110
        ),
        
        tf_03000 AS (
            SELECT 
                id_pgdas,
                Uf, 
                Cod_TOM
            FROM tb_03000)

                
        SELECT 
           fa_03110.CNPJ,
           fa_03110.id_pgdas,
           fa_03110.tipo_ativ,
           fa_03110.Valor_atividade as Valor_Atividade,
           fa_03110.ISS,
           fa_03110.Aliquota_apurada_de_ISS,
           fa_03110.Valor_apurado_de_ISS,
           fa_03110.PA,
           tf_03000.Uf, 
           tf_03000.Cod_TOM
        
        FROM fa_03110
        LEFT JOIN tf_03000  ON  fa_03110.id_pgdas = tf_03000.id_pgdas
        WHERE ISS = {COD_STATUS} and Cod_TOM = {MUNICIPIO} and Cod_TOM_DEST = '' 
                
        """)
        
def utilma_valida(df):
    # Definindo a janela para calcular o maior sequencial por cnpj_raiz e periodo_apuracao
    janela = Window.partitionBy("CNPJ", "PA").orderBy(desc("id_pgdas"))
    df_resultado  =(
            df.withColumn("rank", row_number().over(janela))  # Atribuindo ranking por grupo
            .filter(col("rank") == 1)  # Pegando apenas o maior sequencial
            .drop("rank")  # Removendo a coluna auxiliar
        )
    return df_resultado

def execute_pgdas_fisc(spark=spark, cod_servico=None, url=URL_DEST, MUNICIPIO='5837'):
    

    """ spark: SparkSession object
    cod_status: str, status code for ISS (e.g.,  
    '11' for retencao/substituicao tributaria,  
    '45' for isenção/redução,  
    '03' for lançamento de oficio,  
    '02' for exigibilidade suspensa,  
    '01' for imunidade,  
    '00' nao informado)  
    municipio: str, municipality code (e.g., '5837' for Itaboraí)
    """

    
    df_03110 = spark.read.parquet('gs://k8s-dataita/staged/simples_nacional/pgdas/ready/df_03110/*.parquet')
    df_03120 = spark.read.parquet('gs://k8s-dataita/staged/simples_nacional/pgdas/ready/df_03120/*.parquet')
    df_03130 = spark.read.parquet('gs://k8s-dataita/staged/simples_nacional/pgdas/ready/df_03130/*.parquet')
    df_03000 = (
        spark.read.format("parquet")
        .load("gs://k8s-dataita/staged/simples_nacional/pgdas/ready/df_03000/*.parquet")
    )
    df_03111 = spark.read.parquet('gs://k8s-dataita/staged/simples_nacional/pgdas/ready/df_03111/*.parquet')
    df_03112 = spark.read.parquet('gs://k8s-dataita/staged/simples_nacional/pgdas/ready/df_03112/*.parquet')
    
    df_03000.createOrReplaceTempView("tb_03000")
    df_03120.createOrReplaceTempView("tb_03120")
    df_03110.createOrReplaceTempView("tb_03110")
    df_03130.createOrReplaceTempView("tb_03130")
    df_03111.createOrReplaceTempView("tb_03111")
    df_03112.createOrReplaceTempView("tb_03112")
    
    df = find_iss_pgdas(spark=spark,COD_STATUS=cod_servico, MUNICIPIO=MUNICIPIO)
    df = utilma_valida(df=df)

    df.write.mode('overwrite').parquet(url)
    return f'Save parquet file in {url}'


execute_pgdas_fisc(spark=spark, cod_servico = COD_STATUS, MUNICIPIO=MUNICIPIO, url = URL_DEST)