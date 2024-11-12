{{ config(
    materialized='incremental',
    name='dim_huecos'
) }}

-- with mae as(
--     select TAMANO_HUECO,
--         DESCRIPCION_TAMANO_HUECO,
--         {{ dbt_utils.generate_surrogate_key(['TAMANO_HUECO']) }} AS surrogate_key_mae
--     from {{ source('STG-SCH_LD', 'mae') }}
-- ),

with huecos_raw as(
    select *,
    ROW_NUMBER() OVER (PARTITION BY COD_TERMINAL, COD_HUECO ORDER BY FEC_COMMIT DESC,
        CASE OP 
            WHEN 'U' THEN 1
            WHEN 'I' THEN 2
            WHEN 'D' THEN 3
        END) AS RN
    from {{ ref('huecos_raw') }}
)

select FEC_CARGA,
OP,
FEC_COMMIT,
COD_TERMINAL,
COD_HUECO,
COD_TAMANO,
{{ dbt_utils.generate_surrogate_key(['COD_TAMANO']) }} AS surrogate_key_mae,
from huecos_raw
where RN =1 


