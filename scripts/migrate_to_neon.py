import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# --- Conexión local ---
local_engine = create_engine(
    f"postgresql+psycopg2://pipeline_user:pipeline123@localhost:5432/air_quality"
)

# --- Conexión Neon ---
NEON_URL = os.getenv("NEON_DATABASE_URL")
neon_engine = create_engine(NEON_URL)

def migrate_locations():
    print("Migrando locations...")
    with local_engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM locations", conn)
    
    print(f"  Registros encontrados: {len(df)}")
    
    upsert_sql = text("""
        INSERT INTO locations (
            location_id, name, locality, country,
            latitude, longitude, is_mobile, is_monitor,
            sensors_count, extracted_at, updated_at
        )
        VALUES (
            :location_id, :name, :locality, :country,
            :latitude, :longitude, :is_mobile, :is_monitor,
            :sensors_count, :extracted_at, :updated_at
        )
        ON CONFLICT (location_id) DO UPDATE SET
            name          = EXCLUDED.name,
            sensors_count = EXCLUDED.sensors_count,
            updated_at    = EXCLUDED.updated_at
    """)
    
    with neon_engine.begin() as conn:
        for record in df.to_dict(orient="records"):
            conn.execute(upsert_sql, record)
    
    print(f"  ✅ {len(df)} registros migrados a Neon")

if __name__ == "__main__":
    migrate_locations()