BEGIN
/*
COMANDO PARA CHAMAR PROCEDURE
DECLARE municipio STRING;
CALL pgdas_dw.proc_perfil_dasn("5837");
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

   meus_cnnpjs as (
    SELECT distinct id_pgdas FROM pgdas_dw.estabelecimentos_filial_03000
    WHERE Cod_TOM = municipio
  )

  SELECT perfil.* FROM perfil
  INNER JOIN meus_cnnpjs ON meus_cnnpjs.id_pgdas = perfil.id_pgdas
  INNER JOIN pgdas_filtro ON perfil.id_pgdas = pgdas_filtro.Pgdasd_ID_Declaracao;

END