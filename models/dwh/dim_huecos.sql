{{ config(materialized="incremental", unique_key="surrogate_key_dim") }}


with
    huecos_raw as (
        select
            *,
            row_number() over (
                partition by cod_terminal, cod_hueco
                order by
                    fec_commit desc,
                    case op when 'U' then 1 when 'I' then 2 when 'D' then 3 end
            ) as rn
        from {{ ref("huecos_raw") }}
        {% if is_incremental() %}
            where fec_carga > (select max(fec_carga) from {{ this }}) 
        {% endif %}

    )

select
    fec_carga,
    op,
    fec_commit,
    cod_terminal,
    cod_hueco,
    {{ dbt_utils.generate_surrogate_key(["COD_TERMINAL", "COD_HUECO"]) }}
    as surrogate_key_dim,
    {{ dbt_utils.generate_surrogate_key(["COD_TAMANO"]) }} as surrogate_key_mae
from huecos_raw
where rn = 1
