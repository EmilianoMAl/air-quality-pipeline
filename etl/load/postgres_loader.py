import logging
import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


def get_engine():
    """
    Crea engine de conexión a PostgreSQL.
    Usa variables de entorno — nunca credenciales hardcodeadas.
    """
    host     = os.getenv("DB_HOST", "localhost")
    port     = os.getenv("DB_PORT", "5432")
    db       = os.getenv("DB_NAME", "air_quality")
    user     = os.getenv("DB_USER", "pipeline_user")
    password = os.getenv("DB_PASSWORD", "pipeline123")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"

    engine = create_engine(
        url,
        pool_pre_ping=True,   # Verifica conexión antes de usarla
        pool_size=5,          # Máximo 5 conexiones simultáneas
    )
    return engine


def test_connection() -> bool:
    """Verifica que la conexión a PostgreSQL funciona."""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            version = result.scalar()
            logger.info(f"Conexión exitosa | PostgreSQL {version[:40]}")
            return True
    except SQLAlchemyError as e:
        logger.error(f"Error de conexión: {e}")
        return False


def upsert_locations(df: pd.DataFrame) -> int:
    """
    Inserta o actualiza estaciones en la tabla locations.
    
    Usa UPSERT (INSERT ... ON CONFLICT) — patrón estándar en pipelines
    para evitar duplicados sin borrar datos existentes.

    Args:
        df: DataFrame limpio de parse_locations()

    Returns:
        Número de registros procesados
    """
    if df.empty:
        logger.warning("DataFrame vacío — nada que cargar")
        return 0

    engine = get_engine()

    records = df.to_dict(orient="records")
    processed = 0

    upsert_sql = text("""
        INSERT INTO locations (
            location_id, name, locality, country,
            latitude, longitude, is_mobile, is_monitor,
            sensors_count, extracted_at, updated_at
        )
        VALUES (
            :location_id, :name, :locality, :country,
            :latitude, :longitude, :is_mobile, :is_monitor,
            :sensors_count, :extracted_at, NOW()
        )
        ON CONFLICT (location_id) DO UPDATE SET
            name          = EXCLUDED.name,
            locality      = EXCLUDED.locality,
            sensors_count = EXCLUDED.sensors_count,
            updated_at    = NOW()
    """)

    try:
        with engine.begin() as conn:
            for record in records:
                conn.execute(upsert_sql, record)
                processed += 1

        logger.info(f"Locations cargadas exitosamente: {processed} registros")
        return processed

    except SQLAlchemyError as e:
        logger.error(f"Error al cargar locations: {e}")
        raise


def query_locations(country: str = "MX") -> pd.DataFrame:
    """
    Consulta estaciones guardadas en la base de datos.
    Útil para verificar que los datos llegaron correctamente.
    """
    engine = get_engine()
    query = text("""
        SELECT location_id, name, locality, country,
               latitude, longitude, sensors_count, updated_at
        FROM locations
        WHERE country = :country
        ORDER BY name
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"country": country})

    return df


if __name__ == "__main__":
    import sys
    sys.path.append(".")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
    )

    from pathlib import Path
    from etl.transform.cleaner import load_raw_json, parse_locations, validate_dataframe

    # 1. Probar conexión
    if not test_connection():
        logger.error("No se pudo conectar a PostgreSQL — verifica Docker")
        exit(1)

    # 2. Cargar y transformar datos
    raw_files = sorted(Path("data/raw").glob("locations_mx_*.json"))
    if not raw_files:
        logger.error("No hay archivos en data/raw/")
        exit(1)

    raw_data = load_raw_json(raw_files[-1])
    df = parse_locations(raw_data)

    if not validate_dataframe(df):
        exit(1)

    # 3. Cargar a PostgreSQL
    upsert_locations(df)

    # 4. Verificar que llegaron los datos
    result = query_locations("MX")
    logger.info(f"\nDatos en PostgreSQL:\n{result.to_string()}")