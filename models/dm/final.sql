{{ config(
    materialized='incremental'
) }}

with mae as(
    select *
    from {{ ref('mae') }}
),

dim_huecos as(
    select *
    from {{ ref('dim_huecos') }}
)

select 
    d.OP,
    d.FEC_COMMIT,
    d.COD_TERMINAL,
    d.COD_HUECO,
    mae.DESCRIPCION_TAMANO_HUECO 
from dim_huecos d
left join mae 
    on mae.surrogate_key_mae = d.surrogate_key_mae 
where OP != 'D'
{% if is_incremental() %}
    and surrogate_key_dim not in (select surrogate_key_dim from {{ this }})
{% endif %}


