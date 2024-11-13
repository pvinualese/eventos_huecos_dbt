{{ config(
    materialized='incremental',
    name='final'
) }}

with mae as(
    select TAMANO_HUECO,
        DESCRIPCION_TAMANO_HUECO
        --{{ dbt_utils.generate_surrogate_key(['TAMANO_HUECO']) }} AS surrogate_key_mae
    from {{ source('STG-SCH_LD', 'mae') }}
),

dim_huecos as(
    select *
    from {{ ref('dim_huecos') }}
)

select 
    OP,
    FEC_COMMIT,
    COD_TERMINAL,
    COD_HUECO,
    surrogate_key_mae,
    DESCRIPCION_TAMANO_HUECO 
from dim_huecos 
left join mae 
    on mae.TAMANO_HUECO = COD_TAMANO 
where OP != 'D'


