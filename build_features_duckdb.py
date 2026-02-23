"""
build_features_duckdb.py

Reads v_daily_model_input FROM Postgres, computes rolling/lag features in DuckDB,
and writes features_daily back to Postgres.

Requirements:
  pip install duckdb psycopg2-binary sqlalchemy pandas sqlalchemy[postgresql]  # or just sqlalchemy + psycopg2
"""

import duckdb
import pandas as pd
from sqlalchemy import create_engine
import psycopg2

# -------------------------
# Config
# -------------------------
DB = {
    "host": "localhost",
    "port": 5435,
    "dbname": "smartgrid_db",
    "user": "smart",
    "password": "smart1",
}
CONN_STR = f"postgresql://{DB['user']}:{DB['password']}@{DB['host']}:{DB['port']}/{DB['dbname']}"

# SQL to pull the base view from Postgres
SELECT_BASE = "SELECT * FROM v_daily_model_input ORDER BY date;"

# DuckDB SQL to compute features (single-entity daily series)
DUCKDB_FEATURE_SQL = """
WITH base AS (
    SELECT
        date::DATE AS date,
        consumption::DOUBLE AS consumption,
        cloud_cover, sunshine, global_radiation,
        max_temp, mean_temp, min_temp,
        precipitation, pressure, snow_depth,
        is_weekend, is_holiday,
        dow, doy
    FROM base_df  -- base_df is a registered pandas DataFrame in DuckDB
    ORDER BY date
)
SELECT
    date,
    consumption,
    -- target (next day) for supervised training
    LEAD(consumption, 1) OVER (ORDER BY date) AS y_next,
    
    -- lags
    LAG(consumption, 1) OVER (ORDER BY date) AS lag_1,
    LAG(consumption, 2) OVER (ORDER BY date) AS lag_2,
    LAG(consumption, 7) OVER (ORDER BY date) AS lag_7,
    
    -- rolling means (7, 14, 30 days)
    AVG(consumption) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)  AS rm_7,
    AVG(consumption) OVER (ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS rm_14,
    AVG(consumption) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS rm_30,
    
    -- rolling std
    STDDEV_SAMP(consumption) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)  AS rs_7,
    STDDEV_SAMP(consumption) OVER (ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS rs_14,
    
    -- deltas / momentum
    consumption - LAG(consumption, 1) OVER (ORDER BY date) AS delta_1,
    
    -- bring through weather/calendar
    cloud_cover, sunshine, global_radiation,
    max_temp, mean_temp, min_temp,
    precipitation, pressure, snow_depth,
    
    -- convert day of week and day of year to ML-friendly features
    sin(2 * pi() * dow / 7.0)    AS dow_sin,
    cos(2 * pi() * dow / 7.0)    AS dow_cos,
    sin(2 * pi() * doy / 365.25) AS doy_sin,
    cos(2 * pi() * doy / 365.25) AS doy_cos
FROM base;
"""

# -------------------------
# Helpers
# -------------------------
def fetch_base_from_postgres(conn_str: str) -> pd.DataFrame:
    """Read v_daily_model_input from Postgres into pandas."""
    engine = create_engine(conn_str)
    with engine.connect() as conn:
        df = pd.read_sql(SELECT_BASE, conn)
    # Ensure date typed correctly
    df['date'] = pd.to_datetime(df['date'])
    return df


def write_features_to_postgres(df_features: pd.DataFrame, conn_str: str, table_name: str = "features_daily"):
    """Write features DataFrame back to Postgres using sqlalchemy (replace)."""
    engine = create_engine(conn_str)
    # Replace the table atomically:
    df_features.to_sql(table_name, engine, if_exists="replace", index=False, method='multi', chunksize=1000)
    # Add primary key and index via psycopg2 (because to_sql doesn't add constraints)
    with psycopg2.connect(host=DB['host'], port=DB['port'], dbname=DB['dbname'], user=DB['user'], password=DB['password']) as conn:
        with conn.cursor() as cur:
            # create primary key on date if not exists: replace table so safe to alter
            try:
                cur.execute(f"ALTER TABLE {table_name} ADD PRIMARY KEY (date);")
            except Exception:
                # probably already has PK
                conn.rollback()
            try:
                cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_date ON {table_name}(date);")
            except Exception:
                conn.rollback()
        conn.commit()


# -------------------------
# Main
# -------------------------
def main():
    print("Fetching base view from Postgres...")
    base_df = fetch_base_from_postgres(CONN_STR)
    print(f"Fetched {len(base_df)} rows.")

    print("Running DuckDB feature SQL...")
    con = duckdb.connect(database=':memory:')
    # register pandas df as base_df in duckdb
    con.register("base_df", base_df)

    features = con.execute(DUCKDB_FEATURE_SQL).df()
    print(f"Produced {len(features)} feature rows and {len(features.columns)} columns.")

    # Drop rows where y_next is null (last row) and drop rows with any NULLs (first N rows due to windows)
    features = features.dropna().reset_index(drop=True)
    print(f"After dropna: {len(features)} rows remain.")

    print("Writing features_daily back to Postgres...")
    write_features_to_postgres(features, CONN_STR, table_name="features_daily")
    print("Done.")


if __name__ == "__main__":
    main()