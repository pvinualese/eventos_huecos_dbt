{{ config(
    materialized='table',
    name='huecos_raw'
) }}

select distinct *
from {{ source('STG-SCH_LD', 'huecos') }}