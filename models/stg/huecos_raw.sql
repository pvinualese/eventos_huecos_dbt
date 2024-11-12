

select distinct *
from {{ source('STG-SCH_LD', 'huecos') }}