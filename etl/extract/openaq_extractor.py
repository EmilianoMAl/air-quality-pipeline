import requests
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

# Configuración de logging profesional (nada de prints)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)


API_BASE_URL = "https://api.openaq.org/v3"
API_KEY = os.getenv("OPENAQ_API_KEY")
RAW_DATA_PATH = Path("data/raw")


def get_headers() -> dict:
    """Retorna headers necesarios para autenticación."""
    return {
        "X-API-Key": API_KEY,
        "Accept": "application/json"
    }


def fetch_locations_cdmx(limit: int = 50) -> dict:
    """
    Extrae estaciones de monitoreo activas en Ciudad de México.
    
    Args:
        limit: número máximo de estaciones a traer
    
    Returns:
        dict con la respuesta cruda de la API
    
    Raises:
        requests.HTTPError: si la API responde con error
        requests.Timeout: si la API no responde en tiempo
    """
    if not API_KEY:
        raise ValueError("OPENAQ_API_KEY no está definida en el .env")

    params = {
        "iso": "MX",
        "limit": limit,
    }

    logger.info(f"Consultando ubicaciones en México | limit={limit}")

    try:
        response = requests.get(
            f"{API_BASE_URL}/locations",
            headers=get_headers(),
            params=params,
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
        logger.info(f"Ubicaciones recibidas: {data.get('meta', {}).get('found', 0)}")
        return data

    except requests.Timeout:
        logger.error("Timeout al conectar con OpenAQ API")
        raise
    except requests.HTTPError as e:
        logger.error(f"Error HTTP {e.response.status_code}: {e.response.text}")
        raise


def fetch_measurements_by_location(location_id: int, limit: int = 1000) -> dict:
    """
    Extrae mediciones recientes de una estación específica.

    Args:
        location_id: ID de la estación en OpenAQ
        limit: número máximo de mediciones

    Returns:
        dict con mediciones crudas
    """
    params = {
        "limit": limit,
    }

    logger.info(f"Consultando mediciones | location_id={location_id}")

    try:
        response = requests.get(
            f"{API_BASE_URL}/locations/{location_id}/sensors",
            headers=get_headers(),
            params=params,
            timeout=30
        )
        response.raise_for_status()
        return response.json()

    except requests.Timeout:
        logger.error(f"Timeout al obtener mediciones de location_id={location_id}")
        raise
    except requests.HTTPError as e:
        logger.error(f"Error HTTP {e.response.status_code} en location_id={location_id}")
        raise


def save_raw_data(data: dict, filename: str) -> Path:
    """
    Guarda respuesta cruda de la API como JSON con timestamp.
    Nunca transformamos datos en esta capa.

    Args:
        data: dict con la respuesta de la API
        filename: nombre base del archivo

    Returns:
        Path donde se guardó el archivo
    """
    RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filepath = RAW_DATA_PATH / f"{filename}_{timestamp}.json"

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    logger.info(f"Datos guardados en: {filepath}")
    return filepath


if __name__ == "__main__":
    # Prueba rápida del extractor
    logger.info("=== Iniciando extracción de prueba ===")
    
    # 1. Traer ubicaciones en México
    locations_data = fetch_locations_cdmx(limit=10)
    save_raw_data(locations_data, "locations_mx")
    
    # 2. Mostrar las primeras ubicaciones encontradas
    results = locations_data.get("results", [])
    logger.info(f"Primeras ubicaciones encontradas:")
    for loc in results[:5]:
        logger.info(f"  ID: {loc.get('id')} | {loc.get('name')} | {loc.get('locality')}")