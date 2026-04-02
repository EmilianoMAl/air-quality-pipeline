import os
import streamlit as st

def get_db_config():
    """
    Retorna configuración de DB.
    En local usa .env, en Streamlit Cloud usa st.secrets.
    """
    # Streamlit Cloud
    if hasattr(st, "secrets") and "postgres" in st.secrets:
        return {
            "host":     st.secrets["postgres"]["host"],
            "port":     st.secrets["postgres"]["port"],
            "db":       st.secrets["postgres"]["db"],
            "user":     st.secrets["postgres"]["user"],
            "password": st.secrets["postgres"]["password"],
        }
    # Local — usa .env
    return {
        "host":     os.getenv("DB_HOST", "localhost"),
        "port":     os.getenv("DB_PORT", "5432"),
        "db":       os.getenv("DB_NAME", "air_quality"),
        "user":     os.getenv("DB_USER", "pipeline_user"),
        "password": os.getenv("DB_PASSWORD", "pipeline123"),
    }