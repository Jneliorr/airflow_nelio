BEGIN

WITH 

  meus_cnpjs AS (
    -- Seleciona os CNPJs do município
  SELECT distinct id_pgdas FROM pgdas_dw.estabelecimentos_filial_03000
  WHERE Cod_TOM = municipio

  ),

  pgdas_app AS (
    -- Seleciona os dados da tabela de apuração, extraindo o CNPJ_BASICO 
    SELECT 
      *, 
      LEFT(Cnpjmatriz, 8) AS CNPJ_BASICO 
    FROM `pgdas_dw.contribuinte_apuracao_00000`
  ),

  pgdas_filtro AS (
    -- Agrupa por CNPJ_BASICO e PA, selecionando a declaração mais recente
    SELECT 
      LEFT(Cnpjmatriz, 8) AS CNPJ_BASICO,
      PA,
      MAX(Pgdasd_ID_Declaracao) AS Pgdasd_ID_Declaracao
    FROM `pgdas_dw.contribuinte_apuracao_00000`
    GROUP BY 
      CNPJ_BASICO,
      PA
  )

-- Seleciona os dados das declarações
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

-- Faz a junção das tabelas
FROM 
  pgdas_app
INNER JOIN 
  meus_cnpjs ON meus_cnpjs.id_pgdas = pgdas_app.Pgdasd_ID_Declaracao
INNER JOIN
  pgdas_filtro ON pgdas_filtro.Pgdasd_ID_Declaracao = pgdas_app.Pgdasd_ID_Declaracao;

-- Ordena os resultados
--ORDER BY cnpj_ita.CNPJ_BASICO, PA DESC;

END