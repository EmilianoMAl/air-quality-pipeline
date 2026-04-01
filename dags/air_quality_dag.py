from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import logging
import sys
import os

sys.path.insert(0, "/opt/airflow")

logger = logging.getLogger(__name__)

# --- Funciones que ejecuta cada tarea ---

def task_extract(**context):
    """Extrae datos de OpenAQ y los guarda en data/raw/"""
    from etl.extract.openaq_extractor import (
        fetch_locations_cdmx,
        save_raw_data
    )
    data = fetch_locations_cdmx(limit=50)
    filepath = save_raw_data(data, "locations_mx")
    
    # Pasamos la ruta al siguiente task via XCom
    context["ti"].xcom_push(key="raw_filepath", value=str(filepath))
    logger.info(f"Extracción completada: {filepath}")


def task_transform(**context):
    """Limpia y valida los datos crudos"""
    from etl.transform.cleaner import (
        load_raw_json,
        parse_locations,
        validate_dataframe
    )
    import json

    filepath = context["ti"].xcom_pull(
        key="raw_filepath",
        task_ids="extract_openaq"
    )

    raw_data = load_raw_json(filepath)
    df = parse_locations(raw_data)

    if not validate_dataframe(df):
        raise ValueError("Validación fallida — pipeline detenido")

    # Serializamos el DataFrame para pasarlo al siguiente task
    context["ti"].xcom_push(
        key="clean_data",
        value=df.to_json(orient="records", date_format="iso")
    )
    logger.info(f"Transformación completada: {len(df)} registros limpios")


def task_load(**context):
    """Carga los datos limpios a PostgreSQL"""
    import pandas as pd
    from etl.load.postgres_loader import upsert_locations

    clean_json = context["ti"].xcom_pull(
        key="clean_data",
        task_ids="transform_clean"
    )

    df = pd.read_json(clean_json, orient="records")
    df["extracted_at"] = pd.to_datetime(df["extracted_at"], utc=True)

    count = upsert_locations(df)
    logger.info(f"Carga completada: {count} registros en PostgreSQL")


# --- Definición del DAG ---

default_args = {
    "owner":            "emiliano",
    "depends_on_past":  False,
    "retries":          3,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="air_quality_pipeline",
    description="Pipeline ETL de calidad del aire desde OpenAQ API",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule="@hourly",
    catchup=False,
    tags=["air-quality", "etl", "openaq"],
) as dag:

    start = EmptyOperator(task_id="start")

    extract = PythonOperator(
        task_id="extract_openaq",
        python_callable=task_extract,
    )

    transform = PythonOperator(
        task_id="transform_clean",
        python_callable=task_transform,
    )

    load = PythonOperator(
        task_id="load_postgres",
        python_callable=task_load,
    )

    end = EmptyOperator(task_id="end")

    # Define el orden de ejecución
    start >> extract >> transform >> load >> end