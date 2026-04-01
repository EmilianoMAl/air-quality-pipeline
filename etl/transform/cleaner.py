import pandas as pd
import json
import logging
from pathlib import Path
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def load_raw_json(filepath: str | Path) -> dict:
    """
    Carga un archivo JSON crudo desde data/raw/.

    Args:
        filepath: ruta al archivo JSON

    Returns:
        dict con el contenido del archivo
    """
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def parse_locations(raw_data: dict) -> pd.DataFrame:
    """
    Convierte la respuesta cruda de /locations en un DataFrame limpio.

    Reglas de limpieza:
    - Elimina registros sin coordenadas
    - Normaliza nombres a minúsculas sin espacios extra
    - Filtra solo estaciones activas
    - Extrae latitud y longitud de estructura anidada

    Args:
        raw_data: dict crudo de la API

    Returns:
        DataFrame limpio con columnas estandarizadas
    """
    results = raw_data.get("results", [])

    if not results:
        logger.warning("No se encontraron resultados en el JSON")
        return pd.DataFrame()

    rows = []
    for loc in results:
        # Extraer coordenadas de estructura anidada
        coordinates = loc.get("coordinates") or {}
        lat = coordinates.get("latitude")
        lon = coordinates.get("longitude")

        rows.append({
            "location_id":   loc.get("id"),
            "name":          loc.get("name"),
            "locality":      loc.get("locality"),
            "country":       loc.get("country", {}).get("code") if loc.get("country") else None,
            "latitude":      lat,
            "longitude":     lon,
            "is_mobile":     loc.get("isMobile", False),
            "is_monitor":    loc.get("isMonitor", False),
            "sensors_count": len(loc.get("sensors", [])),
            "extracted_at":  datetime.now(timezone.utc),
        })

    df = pd.DataFrame(rows)

    logger.info(f"Registros antes de limpieza: {len(df)}")

    # --- Reglas de limpieza ---

    # 1. Eliminar registros sin ID
    df = df.dropna(subset=["location_id"])

    # 2. Eliminar registros sin coordenadas (no sirven para análisis geográfico)
    before = len(df)
    df = df.dropna(subset=["latitude", "longitude"])
    dropped = before - len(df)
    if dropped > 0:
        logger.warning(f"Eliminados {dropped} registros sin coordenadas")

    # 3. Limpiar nombres: strip de espacios, título case
    df["name"] = df["name"].str.strip().str.title()
    df["locality"] = df["locality"].str.strip().str.title(
    ) if df["locality"].notna().any() else df["locality"]

    # 4. Eliminar duplicados por location_id
    before = len(df)
    df = df.drop_duplicates(subset=["location_id"])
    dropped = before - len(df)
    if dropped > 0:
        logger.warning(f"Eliminados {dropped} duplicados")

    # 5. Validar rangos de coordenadas (coordenadas imposibles = dato corrupto)
    valid_coords = (
        df["latitude"].between(-90, 90) &
        df["longitude"].between(-180, 180)
    )
    before = len(df)
    df = df[valid_coords]
    dropped = before - len(df)
    if dropped > 0:
        logger.warning(
            f"Eliminados {dropped} registros con coordenadas inválidas")

    # 6. Reset index limpio
    df = df.reset_index(drop=True)

    logger.info(f"Registros después de limpieza: {len(df)}")
    return df


def validate_dataframe(df: pd.DataFrame) -> bool:
    """
    Validaciones mínimas antes de cargar a base de datos.
    En producción usaríamos Great Expectations aquí.

    Returns:
        True si pasa validaciones, False si hay problemas críticos
    """
    if df.empty:
        logger.error("DataFrame vacío — no hay datos para cargar")
        return False

    required_columns = ["location_id", "name", "latitude", "longitude"]
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        logger.error(f"Columnas requeridas faltantes: {missing}")
        return False

    null_ids = df["location_id"].isna().sum()
    if null_ids > 0:
        logger.error(f"Hay {null_ids} location_id nulos — datos corruptos")
        return False

    logger.info("✓ Validaciones pasadas correctamente")
    return True


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
    )

    # Buscar el JSON más reciente en data/raw/
    raw_files = sorted(Path("data/raw").glob("locations_mx_*.json"))

    if not raw_files:
        logger.error(
            "No hay archivos en data/raw/ — corre primero el extractor")
        exit(1)

    latest_file = raw_files[-1]
    logger.info(f"Procesando: {latest_file}")

    raw_data = load_raw_json(latest_file)
    df = parse_locations(raw_data)

    if validate_dataframe(df):
        logger.info(
            f"\n{df[['location_id', 'name', 'locality', 'latitude', 'longitude']].to_string()}")
        logger.info(f"\nDtipos de datos:\n{df.dtypes}")
