{{ config(materialized="view") }}
    
select *,
        {{ dbt_utils.generate_surrogate_key(['TAMANO_HUECO']) }} AS surrogate_key_mae
from {{ source('STG-SCH_LD', 'mae') }}
