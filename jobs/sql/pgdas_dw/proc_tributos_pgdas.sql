BEGIN


--DECLARE municipio STRING;
--CALL pgdas_dw.proc_tributos_pgdas("5837");


WITH fa_03110 AS (
  SELECT 
    CNPJ,
    id_pgdas,
    tipo_ativ,
    COFINS,
    CSLL,
    ICMS,
    INSS,
    IPI,
    IRPJ,
    ISS,
    PIS,
    Valor_apurado_de_COFINS,
    Valor_apurado_de_CSLL,
    Valor_apurado_de_ICMS,
    Valor_apurado_de_INSS,
    Valor_apurado_de_IPI,
    Valor_apurado_de_IRPJ,
    Valor_apurado_de_ISS,
    Valor_apurado_de_PIS,
    PA
  FROM `infra-itaborai.pgdas_dw.valor_receita_atividade_fa_03110`  
)

, fb_03120 AS (
  SELECT 
    CNPJ,
    id_pgdas,
    tipo_ativ,
    '88' as COFINS,
    '88' as CSLL,
    '88' as ICMS,
    '88' as INSS,
    '88' as IPI,
    '88' as IRPJ,
    '88' as ISS,
    '88' as PIS,
    Valor_apurado_de_COFINS,
    Valor_apurado_de_CSLL,
    Valor_apurado_de_ICMS,
    Valor_apurado_de_INSS,
    Valor_apurado_de_IPI,
    Valor_apurado_de_IRPJ,
    Valor_apurado_de_ISS,
    Valor_apurado_de_PIS,
    PA
  FROM `infra-itaborai.pgdas_dw.valor_receita_atividade_fb_03120` 
), 

 fc_03130 AS (
  SELECT 
    CNPJ,
    id_pgdas,
    tipo_ativ,
    '77' as COFINS,
    '77' as CSLL,
    '77' as ICMS,
    '77' as INSS,
    '77' as IPI,
    '77' as IRPJ,
    '77' as ISS,
    '77' as PIS,
    Valor_apurado_de_COFINS,
    Valor_apurado_de_CSLL,
    Valor_apurado_de_ICMS,
    Valor_apurado_de_INSS,
    Valor_apurado_de_IPI,
    Valor_apurado_de_IRPJ,
    Valor_apurado_de_ISS,
    Valor_apurado_de_PIS,
    PA
  FROM `infra-itaborai.pgdas_dw.valor_receita_atividade_fc_03130` 
), 
pgdas_filtro AS (
  SELECT 
    LEFT(Cnpjmatriz, 8) AS CNPJ_BASICO,
    PA,
    MAX(Pgdasd_ID_Declaracao) AS Pgdasd_ID_Declaracao
  FROM pgdas_dw.contribuinte_apuracao_00000
  GROUP BY 
    CNPJ_BASICO,
    PA
),
meus_cnpjs AS (
  SELECT DISTINCT id_pgdas FROM pgdas_dw.estabelecimentos_filial_03000
  WHERE Cod_TOM = municipio
), 
intermed AS (
  SELECT * FROM fa_03110
  UNION ALL
  SELECT * FROM fb_03120
  UNION ALL
  SELECT * FROM fc_03130
)
SELECT intermed.* 
FROM intermed
INNER JOIN meus_cnpjs ON meus_cnpjs.id_pgdas = intermed.id_pgdas
INNER JOIN pgdas_filtro ON pgdas_filtro.Pgdasd_ID_Declaracao = intermed.id_pgdas;

END