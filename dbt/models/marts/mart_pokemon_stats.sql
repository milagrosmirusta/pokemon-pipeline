SELECT 
    types,
    COUNT(*) as pokemon_count,
    AVG(height) as avg_height,
    AVG(weight) as avg_weight,
    AVG(base_experience) as avg_experience
FROM {{ ref('stg_pokemon') }}
GROUP BY types