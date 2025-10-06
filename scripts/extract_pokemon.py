import requests
import json
import os
import psycopg2
from psycopg2.extras import Json

def extract_pokemon_data(limit=50):
    # Extrae de la API
    url = f"https://pokeapi.co/api/v2/pokemon?limit={limit}"
    response = requests.get(url)
    response.raise_for_status()
    results = response.json()["results"]

    pokemon_data = []
    for p in results:
        details = requests.get(p["url"]).json()
        pokemon_data.append({
            "id": details["id"],
            "name": details["name"],
            "base_experience": details["base_experience"],
            "height": details["height"],
            "weight": details["weight"],
            "types": details["types"][0]["type"]["name"]  # cambiado a "types"
        })

    # GUARDA en archivo (para backup)
    os.makedirs("/opt/airflow/data/raw", exist_ok=True)
    with open("/opt/airflow/data/raw/pokemon_raw.json", "w", encoding="utf-8") as f:
        json.dump(pokemon_data, f, indent=4)
    
    # GUARDA en PostgreSQL
    conn = psycopg2.connect(
        host='airflow_postgres',
        database='airflow',
        user='airflow',
        password='airflow'
    )
    cursor = conn.cursor()
    
    # Crear tabla
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pokemon_raw (
            id INTEGER PRIMARY KEY,
            name VARCHAR(100),
            height INTEGER,
            weight INTEGER,
            base_experience INTEGER,
            types VARCHAR(50)
        )
    """)
    
    # Insertar datos
    for pokemon in pokemon_data:
        cursor.execute("""
            INSERT INTO pokemon_raw (id, name, height, weight, base_experience, types)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                height = EXCLUDED.height,
                weight = EXCLUDED.weight,
                base_experience = EXCLUDED.base_experience,
                types = EXCLUDED.types
        """, (
            pokemon['id'],
            pokemon['name'],
            pokemon['height'],
            pokemon['weight'],
            pokemon['base_experience'],
            pokemon['types']
        ))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"{len(pokemon_data)} Pokémon guardados en PostgreSQL y archivo JSON")
    return pokemon_data

if __name__ == "__main__":
    extract_pokemon_data()
