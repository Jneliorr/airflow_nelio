BEGIN
/*
COMANDO PARA CHAMAR PROCEDURE
DECLARE municipio STRING;
CALL pgdas_dw.proc_apuracao_contribuinte('5837');
*/
WITH meus_cnpjs AS (
  SELECT distinct id_pgdas FROM pgdas_dw.estabelecimentos_filial_03000
  WHERE Cod_TOM = municipio),

pgdas_app AS (
  SELECT *, LEFT(Cnpjmatriz, 8) AS CNPJ_BASICO FROM `pgdas_dw.contribuinte_apuracao_00000`
),

pgdas_filtro AS (
  SELECT 
    LEFT(Cnpjmatriz, 8) AS CNPJ_BASICO,
    PA,
    MAX(Pgdasd_ID_Declaracao) AS Pgdasd_ID_Declaracao
  FROM `pgdas_dw.contribuinte_apuracao_00000`
  GROUP BY 
    CNPJ_BASICO,
    PA

)


SELECT 
  --cnpj_ita.CNPJ_COMPLETO AS cnpj_mun,
  pgdas_app.Pgdasd_ID_Declaracao,
  --pgdas_app.Pgdasd_Num_Recibo,
  --pgdas_app.Pgdasd_Num_Autenticacao,
  Pgdasd_Dt_Transmissao,
  pgdas_app.Cnpjmatriz,
  pgdas_app.PA,
  pgdas_app.Rpa,
  pgdas_app.Operacao,
  pgdas_app.Regime,
  pgdas_app.RpaC
FROM 
  pgdas_app
INNER JOIN 
  meus_cnpjs ON meus_cnpjs.id_pgdas = pgdas_app.Pgdasd_ID_Declaracao
INNER JOIN
  pgdas_filtro ON pgdas_filtro.Pgdasd_ID_Declaracao = pgdas_app.Pgdasd_ID_Declaracao;

END