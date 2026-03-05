BEGIN
/*
COMANDO PARA CHAMAR PROCEDURE
DECLARE municipio STRING;
CALL mds_sn_pgdas.proc_receita_estabelecimento("5837");
*/
WITH pgdas_validas AS (
  SELECT 
    LEFT(Cnpjmatriz, 8) as CNPJ_BASICO,
    PA,
    MAX(Pgdasd_ID_Declaracao) AS id_pgdas
  FROM mds_sn_pgdas.contribuinte_apuracao_00000
  group by 
  CNPJ_BASICO,
  PA
  ),

  meus_cnpjs as (
    SELECT id_pgdas FROM mds_sn_pgdas.estabelecimentos_filial_03000
    WHERE Cod_TOM = municipio
  ),

  rec_estabelecimento as(

    SELECT * FROM `infra-itaborai.mds_sn_pgdas.atividade_estabelecimento_03100` 
    
    )

  SELECT rec_estabelecimento.* FROM rec_estabelecimento
  INNER JOIN meus_cnpjs ON rec_estabelecimento.id_pgdas = meus_cnpjs.id_pgdas
  INNER JOIN pgdas_validas ON pgdas_validas.id_pgdas = rec_estabelecimento.id_pgdas;

END