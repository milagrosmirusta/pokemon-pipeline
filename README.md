# Pokémon Data Pipeline 
Pipeline de datos end-to-end que extrae información de la PokéAPI, la transforma con dbt, y genera métricas agregadas sobre características de Pokémon. Mini proyecto como Analytics Engineering.

## Stack Tecnológico

- **Orquestación**: Apache Airflow 2.9.3
- **Extracción**: Python + requests (PokéAPI)
- **Transformación**: dbt Core + dbt-postgres
- **Base de datos**: PostgreSQL 13
- **Infraestructura**: Docker + Docker Compose
- **Análisis**: Jupyter Notebook + pandas + matplotlib

## Arquitectura
PokéAPI → Python Script → PostgreSQL → dbt → Tablas Analytics → Visualizaciones


**Flujo de datos:**
1. `extract_pokemon_data`: Extrae 50 pokémon de la API y los carga en `pokemon_raw`
2. `dbt run`: Ejecuta modelos de staging y marts
   - `stg_pokemon`: Limpieza y estandarización
   - `mart_pokemon_stats`: Agregaciones por tipo (count, promedios)

## Estructura del Proyecto
- `airflow/` - Orquestación con Airflow
  - `dags/etl_pokemon_dag.py` - DAG principal
  - `docker-compose.yml` - Configuración Docker
- `dbt/` - Modelos de transformación
  - `models/staging/` - Limpieza de datos
  - `models/marts/` - Métricas agregadas
- `scripts/` - Scripts de extracción
- `data/raw/` - Datos descargados
- `analysis/` - Notebooks de visualización

## Instalación

### Prerrequisitos
- Docker y Docker Compose
- Python 3.8+ (para notebooks locales)

### Setup

1. Clonar el repositorio
```bash
git clone https://github.com/milagrosmirusta/pokemon-pipeline
cd pokemon-pipeline