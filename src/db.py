# ============================================================
# src/db.py
# Módulo de base de datos.
#
# Responsabilidades:
#   - Crear la conexión a PostgreSQL
#   - Garantizar que las tablas existen (Bronze y Gold)
#   - Insertar datos en Bronze (JSON crudo)
#   - Hacer upsert en las tablas Gold (datos procesados)
#   - Refrescar las tablas de agregados periódicamente
#
# IMPORTANTE: No existe ningún archivo .sql externo.
# Todo el schema se gestiona aquí, desde Python.
# ============================================================

import psycopg2
from psycopg2.extras import Json
import logging

# Importamos la configuración centralizada
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))
import src.config as config

logger = logging.getLogger(__name__)


def get_connection() -> psycopg2.extensions.connection:
    """
    Crea y retorna una nueva conexión a PostgreSQL.
    Cada operación abre y cierra su propia conexión para
    mantener el código simple y evitar conexiones colgadas.
    """
    return psycopg2.connect(
        host=config.DB_HOST,
        port=config.DB_PORT,
        dbname=config.DB_NAME,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        connect_timeout=10,
    )


def ensure_tables_exist():
    """
    Crea las tablas Bronze y Gold si no existen todavía.
    Es seguro llamar a esta función múltiples veces (idempotente).
    Se ejecuta al arrancar el subscriber.

    Arquitectura Medallón simplificada:
      · Bronze: JSON crudo sin tocar, tal como llega de MQTT.
      · Gold:   Tablas normalizadas y limpias para el dashboard.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:

            # --------------------------------------------------
            # CAPA BRONZE — Datos crudos
            # Una fila por cada mensaje MQTT recibido.
            # La columna 'payload' es JSONB: permite indexar y
            # hacer queries sobre el JSON directamente en SQL.
            # --------------------------------------------------
            cur.execute("""
                CREATE TABLE IF NOT EXISTS bronze_raw (
                    id          SERIAL PRIMARY KEY,
                    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    payload     JSONB       NOT NULL
                );
            """)

            # Índice GIN sobre el JSONB para búsquedas rápidas en la capa cruda
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_bronze_payload
                ON bronze_raw USING GIN (payload);
            """)
            logger.info("Tabla bronze_raw ✓")

            # --------------------------------------------------
            # CAPA GOLD — Tabla principal de pistas procesadas
            # Una fila por track_id (UPSERT).
            # Diseñada para alimentar directamente el dashboard.
            # --------------------------------------------------
            cur.execute("""
                CREATE TABLE IF NOT EXISTS gold_tracks (
                    track_id            TEXT        PRIMARY KEY,
                    track_name          TEXT,
                    artists             TEXT,
                    album_name          TEXT,
                    track_genre         TEXT,
                    release_year        SMALLINT,         -- extraído del nombre del álbum si está disponible
                    popularity          SMALLINT,         -- 0–100
                    danceability        NUMERIC(4,3),     -- 0.0–1.0
                    energy              NUMERIC(4,3),     -- 0.0–1.0
                    valence             NUMERIC(4,3),     -- 0.0–1.0 (positividad musical)
                    tempo               NUMERIC(6,2),     -- BPM
                    acousticness        NUMERIC(4,3),
                    instrumentalness    NUMERIC(4,3),
                    speechiness         NUMERIC(4,3),
                    duration_min        NUMERIC(5,2),     -- duración en minutos (calculada)
                    explicit            BOOLEAN,
                    fiesta_index        NUMERIC(4,3),     -- métrica compuesta calculada
                    data_source         TEXT NOT NULL DEFAULT 'csv_kaggle',
                    processed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)

            # Índices para acelerar los filtros más frecuentes del dashboard
            cur.execute("CREATE INDEX IF NOT EXISTS idx_gold_genre    ON gold_tracks (track_genre);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_gold_year     ON gold_tracks (release_year);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_gold_popular  ON gold_tracks (popularity DESC);")
            logger.info("Tabla gold_tracks ✓")

            # --------------------------------------------------
            # CAPA GOLD — Estadísticas por género
            # Agregado pre-calculado: una fila por género.
            # Ideal para gráficas de barras y análisis de nichos.
            # --------------------------------------------------
            cur.execute("""
                CREATE TABLE IF NOT EXISTS gold_genre_stats (
                    track_genre         TEXT        PRIMARY KEY,
                    track_count         INTEGER,
                    avg_popularity      NUMERIC(5,2),
                    avg_danceability    NUMERIC(4,3),
                    avg_energy          NUMERIC(4,3),
                    avg_valence         NUMERIC(4,3),
                    avg_tempo           NUMERIC(6,2),
                    avg_fiesta_index    NUMERIC(4,3),
                    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            logger.info("Tabla gold_genre_stats ✓")

            # --------------------------------------------------
            # CAPA GOLD — Tendencias temporales por año
            # Muestra la evolución de los estilos musicales.
            # Ideal para gráficas de líneas temporales.
            # --------------------------------------------------
            cur.execute("""
                CREATE TABLE IF NOT EXISTS gold_temporal_trends (
                    release_year        SMALLINT    PRIMARY KEY,
                    track_count         INTEGER,
                    avg_danceability    NUMERIC(4,3),
                    avg_energy          NUMERIC(4,3),
                    avg_valence         NUMERIC(4,3),
                    avg_acousticness    NUMERIC(4,3),
                    avg_fiesta_index    NUMERIC(4,3),
                    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            logger.info("Tabla gold_temporal_trends ✓")

        conn.commit()
        logger.info("Schema de la base de datos verificado y listo.")

    except Exception as e:
        conn.rollback()
        logger.error(f"Error creando tablas: {e}")
        raise
    finally:
        conn.close()


def insert_bronze(payload_dict: dict):
    """
    Inserta un mensaje crudo en la capa Bronze.
    El diccionario se serializa como JSONB en PostgreSQL.

    Args:
        payload_dict: El payload tal como llegó del mensaje MQTT.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO bronze_raw (payload) VALUES (%s);",
                (Json(payload_dict),)
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Error insertando en bronze_raw: {e}")
        raise
    finally:
        conn.close()


def upsert_gold_track(track: dict):
    """
    Inserta o actualiza un registro en gold_tracks (UPSERT).
    Si el track_id ya existe (porque el CSV tiene duplicados o se re-escanea),
    solo actualiza los campos que pueden cambiar.

    Args:
        track: Diccionario con los campos ya transformados y validados.
    """
    conn = get_connection()
    sql = """
        INSERT INTO gold_tracks (
            track_id, track_name, artists, album_name, track_genre,
            release_year, popularity, danceability, energy, valence,
            tempo, acousticness, instrumentalness, speechiness,
            duration_min, explicit, fiesta_index, data_source, processed_at
        ) VALUES (
            %(track_id)s, %(track_name)s, %(artists)s, %(album_name)s, %(track_genre)s,
            %(release_year)s, %(popularity)s, %(danceability)s, %(energy)s, %(valence)s,
            %(tempo)s, %(acousticness)s, %(instrumentalness)s, %(speechiness)s,
            %(duration_min)s, %(explicit)s, %(fiesta_index)s, %(data_source)s, NOW()
        )
        ON CONFLICT (track_id) DO UPDATE SET
            popularity    = EXCLUDED.popularity,
            fiesta_index  = EXCLUDED.fiesta_index,
            data_source   = EXCLUDED.data_source, -- <-- ¡AÑADIR ESTA LÍNEA TAMBIÉN!
            processed_at  = NOW();
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, track)
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Error en upsert gold_tracks (track_id={track.get('track_id')}): {e}")
        raise
    finally:
        conn.close()


def refresh_genre_stats():
    """
    Recalcula y sobreescribe las estadísticas agregadas por género.
    Se llama cada N mensajes desde el subscriber para mantener
    la tabla gold_genre_stats actualizada mientras llegan los datos.
    """
    conn = get_connection()
    sql = """
        INSERT INTO gold_genre_stats (
            track_genre, track_count,
            avg_popularity, avg_danceability, avg_energy,
            avg_valence, avg_tempo, avg_fiesta_index, updated_at
        )
        SELECT
            track_genre,
            COUNT(*)                        AS track_count,
            ROUND(AVG(popularity), 2)       AS avg_popularity,
            ROUND(AVG(danceability), 3)     AS avg_danceability,
            ROUND(AVG(energy), 3)           AS avg_energy,
            ROUND(AVG(valence), 3)          AS avg_valence,
            ROUND(AVG(tempo), 2)            AS avg_tempo,
            ROUND(AVG(fiesta_index), 3)     AS avg_fiesta_index,
            NOW()
        FROM gold_tracks
        WHERE track_genre IS NOT NULL
          AND track_genre != 'Unknown'
        GROUP BY track_genre
        ON CONFLICT (track_genre) DO UPDATE SET
            track_count      = EXCLUDED.track_count,
            avg_popularity   = EXCLUDED.avg_popularity,
            avg_danceability = EXCLUDED.avg_danceability,
            avg_energy       = EXCLUDED.avg_energy,
            avg_valence      = EXCLUDED.avg_valence,
            avg_tempo        = EXCLUDED.avg_tempo,
            avg_fiesta_index = EXCLUDED.avg_fiesta_index,
            updated_at       = NOW();
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        logger.info("gold_genre_stats actualizada.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error refrescando genre_stats: {e}")
        raise
    finally:
        conn.close()


def refresh_temporal_trends():
    """
    Recalcula y sobreescribe las tendencias temporales por año.
    Solo procesa filas con release_year válido (1950–año actual).
    """
    conn = get_connection()
    sql = """
        INSERT INTO gold_temporal_trends (
            release_year, track_count,
            avg_danceability, avg_energy, avg_valence,
            avg_acousticness, avg_fiesta_index, updated_at
        )
        SELECT
            release_year,
            COUNT(*)                        AS track_count,
            ROUND(AVG(danceability), 3)     AS avg_danceability,
            ROUND(AVG(energy), 3)           AS avg_energy,
            ROUND(AVG(valence), 3)          AS avg_valence,
            ROUND(AVG(acousticness), 3)     AS avg_acousticness,
            ROUND(AVG(fiesta_index), 3)     AS avg_fiesta_index,
            NOW()
        FROM gold_tracks
        WHERE release_year IS NOT NULL
          AND release_year BETWEEN 1950 AND EXTRACT(YEAR FROM NOW())::SMALLINT
        GROUP BY release_year
        ON CONFLICT (release_year) DO UPDATE SET
            track_count      = EXCLUDED.track_count,
            avg_danceability = EXCLUDED.avg_danceability,
            avg_energy       = EXCLUDED.avg_energy,
            avg_valence      = EXCLUDED.avg_valence,
            avg_acousticness = EXCLUDED.avg_acousticness,
            avg_fiesta_index = EXCLUDED.avg_fiesta_index,
            updated_at       = NOW();
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        logger.info("gold_temporal_trends actualizada.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error refrescando temporal_trends: {e}")
        raise
    finally:
        conn.close()
