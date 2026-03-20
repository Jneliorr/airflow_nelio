{{ config(materialized='view',full_refresh=true, alias='cinema_rr') }}

WITH estado AS (

    SELECT * FROM public.bilheteria
    WHERE uf_sala_complexo = 'RR'

)
SELECT 
    *
FROM estado






