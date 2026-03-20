WITH estado AS (

    SELECT * FROM public.bilheteria
    WHERE uf_sala_complexo = 'RJ'

)
SELECT 
    *
FROM estado

