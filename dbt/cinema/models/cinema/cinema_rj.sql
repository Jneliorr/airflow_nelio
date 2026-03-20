{{ config(materialized='view',full_refresh=true, alias='cinema_rj') }}

WITH estado AS (

    SELECT * FROM public.bilheteria
    WHERE uf_sala_complexo = 'RJ'

)
SELECT 
    *
FROM estado

