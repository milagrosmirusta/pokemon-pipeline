select
    id,
    name,
    base_experience,
    height,
    weight,
    types
from {{ source('raw', 'pokemon_raw') }}
where id is not null