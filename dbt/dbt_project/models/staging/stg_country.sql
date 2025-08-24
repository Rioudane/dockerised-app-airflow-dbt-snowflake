select
    "country_id" as country_id,
    "country_name" as country_name
from {{ source('snowflake', 'raw_country') }}
