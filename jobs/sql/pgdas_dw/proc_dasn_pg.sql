BEGIN
/*
COMANDO PARA CHAMAR PROCEDURE
DECLARE municipio STRING;
CALL pgdas_dw.proc_dasn_pg("5837");
*/
WITH pgdas_filtro AS (
  SELECT 
    LEFT(Cnpjmatriz, 8) as CNPJ_BASICO,
    PA,
    MAX(Pgdasd_ID_Declaracao) AS Pgdasd_ID_Declaracao
  FROM pgdas_dw.contribuinte_apuracao_00000
  group by 
  CNPJ_BASICO,
  PA
  ),

  perfil as(

    SELECT * FROM pgdas_dw.perfil_das_01100
  ),

   meus_cnpjs as (
    SELECT id_pgdas FROM pgdas_dw.estabelecimentos_filial_03000
    WHERE Cod_TOM = municipio
  ),

  guias_pagas as (SELECT 
      numero_das, 
      MIN(data_arrecadacao) AS DT_PRIMEIRO_PAG, 
      MAX(data_arrecadacao) AS DT_ULTIMO_PAG,
      COUNT(data_arrecadacao) AS QNT_PAG,
      'S' AS PAGO, 
      SUM(CAST(valor_total_das AS FLOAT64)) as valor_total_pago
    FROM `dasn.dasn_pag`
    GROUP BY numero_das
  ),

  new_perfil as (
    SELECT distinct perfil.num_guia as num_guia  FROM perfil
    INNER JOIN meus_cnpjs ON meus_cnpjs.id_pgdas = perfil.id_pgdas
    INNER JOIN pgdas_filtro ON perfil.id_pgdas = pgdas_filtro.Pgdasd_ID_Declaracao
    )

  SELECT guias_pagas.* FROM guias_pagas
  INNER JOIN new_perfil ON new_perfil.num_guia = guias_pagas.numero_das;

END