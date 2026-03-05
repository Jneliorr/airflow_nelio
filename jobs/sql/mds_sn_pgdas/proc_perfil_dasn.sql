BEGIN
/*
COMANDO PARA CHAMAR PROCEDURE
DECLARE municipio STRING;
CALL mds_sn_pgdas.proc_perfil_dasn("5837");
*/
WITH pgdas_filtro AS (
  SELECT 
    LEFT(Cnpjmatriz, 8) as CNPJ_BASICO,
    PA,
    MAX(Pgdasd_ID_Declaracao) AS Pgdasd_ID_Declaracao
  FROM mds_sn_pgdas.contribuinte_apuracao_00000
  group by 
  CNPJ_BASICO,
  PA
  ),

  perfil as(

    SELECT * FROM mds_sn_pgdas.perfil_das_01100
  ),

   meus_cnnpjs as (
    SELECT distinct id_pgdas FROM mds_sn_pgdas.estabelecimentos_filial_03000
    WHERE Cod_TOM = municipio
  )

  SELECT perfil.* FROM perfil
  INNER JOIN meus_cnnpjs ON meus_cnnpjs.id_pgdas = perfil.id_pgdas
  INNER JOIN pgdas_filtro ON perfil.id_pgdas = pgdas_filtro.Pgdasd_ID_Declaracao;

END