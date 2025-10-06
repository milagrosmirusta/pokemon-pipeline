import requests
import json
import os
import psycopg2
from psycopg2.extras import Json

def extract_pokemon_data(limit=50):
    try:
        # Extrae de la API
        url = f"https://pokeapi.co/api/v2/pokemon?limit={limit}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        results = response.json()["results"]

        pokemon_data = []
        for p in results:
            details = requests.get(p["url"], timeout=10).json()
            
            # Validaciones de tipos de datos
            assert isinstance(details["id"], int), f"ID debe ser int, recibido: {type(details['id'])}"
            assert isinstance(details["name"], str), f"Name debe ser str, recibido: {type(details['name'])}"
            assert isinstance(details["height"], int), f"Height debe ser int, recibido: {type(details['height'])}"
            assert isinstance(details["weight"], int), f"Weight debe ser int, recibido: {type(details['weight'])}"
            assert details["base_experience"] is None or isinstance(details["base_experience"], int), \
                f"Base experience debe ser int o None"
            assert len(details["types"]) > 0, f"Pokémon {details['name']} sin tipos"
            
            pokemon_data.append({
                "id": details["id"],
                "name": details["name"],
                "base_experience": details["base_experience"],
                "height": details["height"],
                "weight": details["weight"],
                "types": details["types"][0]["type"]["name"]
            })

        # Guarda en archivo (para backup)
        os.makedirs("/opt/airflow/data/raw", exist_ok=True)
        with open("/opt/airflow/data/raw/pokemon_raw.json", "w", encoding="utf-8") as f:
            json.dump(pokemon_data, f, indent=4)
        
        # Guarda en PostgreSQL usando context manager
        with psycopg2.connect(
            host='airflow_postgres',
            database='airflow',
            user='airflow',
            password='airflow'
        ) as conn:
            with conn.cursor() as cursor:
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
        
        print(f"✅ {len(pokemon_data)} Pokémon guardados en PostgreSQL y archivo JSON")
        return pokemon_data
    
    except requests.RequestException as e:
        print(f"❌ Error al consultar la API: {e}")
        raise
    except psycopg2.Error as e:
        print(f"❌ Error en la base de datos: {e}")
        raise
    except AssertionError as e:
        print(f"❌ Error de validación: {e}")
        raise
    except Exception as e:
        print(f"❌ Error inesperado: {e}")
        raise

if __name__ == "__main__":
    extract_pokemon_data()
