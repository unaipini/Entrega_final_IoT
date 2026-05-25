"""
src/consumer/main.py
Servicio consumidor MQTT del pipeline IoT de Spotify.

Arquitectura Medallón completa:
  MQTT msg → Bronze (raw JSONB) → transform y validacion → Gold (normalizado)

Mejoras respecto a versiones anteriores:
  - Capa Bronze: cada mensaje se guarda crudo antes de validar.
    Permite auditar qué llegó al sistema vs qué pasó la validación.
  - Transformer riguroso: valida rangos con safe_float(min, max),
    normaliza explicit desde múltiples formatos, extrae release_year.
  - party_index ponderado (0.40/0.35/0.25) en lugar de media simple.
  - Agregados pre-calculados (genre_stats, temporal_trends) cada 500 msgs.
  - Credenciales desde variables de entorno.
  - Reintentos automáticos de conexión a PostgreSQL y MQTT.
"""

import json
import logging
import os
import re
import time
from typing import Optional

import paho.mqtt.client as mqtt
import psycopg2
from psycopg2.extras import Json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("consumer")

MQTT_BROKER = os.environ.get("MQTT_BROKER", "mosquitto")
MQTT_PORT   = int(os.environ.get("MQTT_PORT", 1883))
MQTT_TOPIC  = os.environ.get("MQTT_TOPIC", "spotify/tracks")

PG_HOST     = os.environ.get("POSTGRES_HOST", "postgres")
PG_PORT     = int(os.environ.get("POSTGRES_PORT", 5432))
PG_DB       = os.environ.get("POSTGRES_DB", "spotify_db")
PG_USER     = os.environ.get("POSTGRES_USER", "spotify_user")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "")

REFRESH_EVERY_N = 500

_message_counter   = 0
_discarded_counter = 0

# ===========================================================================
# Conexión a PostgreSQL
# ===========================================================================

def _get_pg_connection():
    while True:
        try:
            conn = psycopg2.connect(
                host=PG_HOST, port=PG_PORT,
                dbname=PG_DB, user=PG_USER, password=PG_PASSWORD,
            )
            logger.info("Conexion a PostgreSQL establecida.")
            return conn
        except psycopg2.OperationalError as exc:
            logger.warning("PostgreSQL no disponible (%s). Reintentando en 5 s...", exc)
            time.sleep(5)

# ===========================================================================
# Transformer — validación y limpieza rigurosa (tomado de Unai, mejorado)
# ===========================================================================

def _safe_float(value, min_val: float = 0.0, max_val: float = 1.0) -> Optional[float]:
    try:
        f = float(value)
        return f if min_val <= f <= max_val else None
    except (ValueError, TypeError):
        return None

def _safe_int(value, min_val=None, max_val=None) -> Optional[int]:
    try:
        i = int(float(value))
        if min_val is not None and i < min_val: return None
        if max_val is not None and i > max_val: return None
        return i
    except (ValueError, TypeError):
        return None

def _extract_year(text: str) -> Optional[int]:
    """Extrae un año de 4 dígitos (1950-2025) del nombre del álbum."""
    if not isinstance(text, str):
        return None
    matches = re.findall(r'\b(19[5-9]\d|20[0-2]\d)\b', text)
    return int(matches[0]) if matches else None

def _normalize_explicit(value) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes")
    return bool(value) if value is not None else False

def _transform(raw: dict) -> Optional[dict]:
    """
    Transforma y valida un payload crudo.
    Devuelve None si el registro no supera la validación mínima.
    """
    track_id = raw.get("track_id")
    if not track_id or not str(track_id).strip():
        logger.warning("Registro descartado: track_id ausente.")
        return None

    track_name  = str(raw.get("track_name",  "Unknown")).strip()[:255]
    artist_id   = str(raw.get("artist_id",   "")).strip()[:22]
    artist_name = str(raw.get("artist_name", "Unknown")).strip()[:255]
    album_id    = str(raw.get("album_id",    "")).strip()[:22]
    album_name  = str(raw.get("album_name",  "")).strip()[:255]
    track_genre = str(raw.get("track_genre", "Unknown")).strip()[:100]
    genres      = raw.get("genres", [])
    if not isinstance(genres, list):
        genres = []

    popularity       = _safe_int(raw.get("popularity"),       0,   100)
    danceability     = _safe_float(raw.get("danceability"),   0.0, 1.0)
    energy           = _safe_float(raw.get("energy"),         0.0, 1.0)
    valence          = _safe_float(raw.get("valence"),        0.0, 1.0)
    tempo            = _safe_float(raw.get("tempo"),          0.0, 300.0)
    loudness         = _safe_float(raw.get("loudness"),      -60.0, 5.0)
    acousticness     = _safe_float(raw.get("acousticness"),   0.0, 1.0)
    instrumentalness = _safe_float(raw.get("instrumentalness"), 0.0, 1.0)
    speechiness      = _safe_float(raw.get("speechiness"),   0.0, 1.0)
    liveness         = _safe_float(raw.get("liveness"),       0.0, 1.0)
    key              = _safe_int(raw.get("key"),  0, 11)
    mode             = _safe_int(raw.get("mode"), 0, 1)
    time_sig         = _safe_int(raw.get("time_signature"), 1, 7)

    duration_ms  = _safe_int(raw.get("duration_ms"), 0)
    release_year = _extract_year(album_name)
    explicit     = _normalize_explicit(raw.get("explicit", False))
    source       = raw.get("data_source", "csv")
    if source not in ("csv", "api"):
        source = "csv"

    # Fecha de lanzamiento desde la API (formato ISO)
    release_date = raw.get("release_date")

    return {
        "track_id":         str(track_id).strip()[:22],
        "track_name":       track_name,
        "artist_id":        artist_id or None,
        "artist_name":      artist_name,
        "album_id":         album_id or None,
        "album_name":       album_name,
        "track_genre":      track_genre,
        "genres":           genres,
        "release_date":     release_date,
        "release_year":     release_year,
        "popularity":       popularity if popularity is not None else 0,
        "danceability":     danceability,
        "energy":           energy,
        "valence":          valence,
        "tempo":            tempo,
        "loudness":         loudness,
        "acousticness":     acousticness,
        "instrumentalness": instrumentalness,
        "speechiness":      speechiness,
        "liveness":         liveness,
        "key":              key,
        "mode":             mode,
        "time_signature":   time_sig,
        "duration_ms":      duration_ms,
        "explicit":         explicit,
        "source":           source,
    }

# ===========================================================================
# Operaciones de base de datos
# ===========================================================================

def _insert_bronze(conn, payload_dict: dict) -> None:
    with conn.cursor() as cur:
        cur.execute("INSERT INTO bronze_raw (payload) VALUES (%s);", (Json(payload_dict),))
    conn.commit()

def _upsert_artist(cur, data: dict) -> None:
    if not data.get("artist_id") or not data.get("artist_name"):
        return
    cur.execute(
        """
        INSERT INTO artists (id, name, genres)
        VALUES (%s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET genres = EXCLUDED.genres
        """,
        (data["artist_id"], data["artist_name"], data["genres"]),
    )

def _upsert_album(cur, data: dict) -> None:
    if not data.get("album_id") or not data.get("album_name"):
        return
    release_date = data.get("release_date")
    if not release_date and data.get("release_year"):
        release_date = f"{data['release_year']}-01-01"
    cur.execute(
        """
        INSERT INTO albums (id, name, artist_id, release_date)
        VALUES (%s, %s, %s, %s::date)
        ON CONFLICT (id) DO NOTHING
        """,
        (data["album_id"], data["album_name"][:255],
         data.get("artist_id"), release_date),
    )

def _upsert_track(cur, data: dict) -> bool:
    cur.execute(
        """
        INSERT INTO tracks (id, name, artist_id, album_id, duration_ms, explicit, popularity, source)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            popularity = EXCLUDED.popularity,
            source     = EXCLUDED.source
        """,
        (
            data["track_id"], data["track_name"],
            data["artist_id"] or None,
            data["album_id"]  or None,
            data["duration_ms"], data["explicit"],
            data["popularity"], data["source"],
        ),
    )
    return cur.rowcount > 0

def _upsert_audio_features(cur, data: dict) -> None:
    if all(data.get(k) is None for k in ("danceability", "energy", "valence")):
        return
    cur.execute(
        """
        INSERT INTO audio_features (
            track_id, danceability, energy, valence,
            tempo, loudness, speechiness, acousticness,
            instrumentalness, liveness, key, mode, time_signature
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (track_id) DO NOTHING
        """,
        (
            data["track_id"],
            data["danceability"], data["energy"], data["valence"],
            data["tempo"], data["loudness"], data["speechiness"],
            data["acousticness"], data["instrumentalness"], data["liveness"],
            data["key"], data["mode"], data["time_signature"],
        ),
    )

def _refresh_genre_stats(conn) -> None:
    sql = """
        INSERT INTO gold_genre_stats (
            track_genre, track_count,
            avg_popularity, avg_danceability, avg_energy,
            avg_valence, avg_tempo, avg_party_index, updated_at
        )
        SELECT
            COALESCE(t.name, 'Unknown') AS track_genre,
            COUNT(*)                    AS track_count,
            ROUND(AVG(t.popularity), 2),
            ROUND(AVG(af.danceability), 3),
            ROUND(AVG(af.energy), 3),
            ROUND(AVG(af.valence), 3),
            ROUND(AVG(af.tempo), 2),
            ROUND(AVG(af.party_index), 3),
            NOW()
        FROM tracks t
        JOIN audio_features af ON t.id = af.track_id
        GROUP BY 1
        HAVING COUNT(*) > 0
        ON CONFLICT (track_genre) DO UPDATE SET
            track_count      = EXCLUDED.track_count,
            avg_popularity   = EXCLUDED.avg_popularity,
            avg_danceability = EXCLUDED.avg_danceability,
            avg_energy       = EXCLUDED.avg_energy,
            avg_valence      = EXCLUDED.avg_valence,
            avg_tempo        = EXCLUDED.avg_tempo,
            avg_party_index  = EXCLUDED.avg_party_index,
            updated_at       = NOW();
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    logger.info("gold_genre_stats actualizada.")

def _refresh_temporal_trends(conn) -> None:
    sql = """
        INSERT INTO gold_temporal_trends (
            release_year, track_count,
            avg_danceability, avg_energy, avg_valence,
            avg_acousticness, avg_party_index, updated_at
        )
        SELECT
            al.release_year,
            COUNT(*)                        AS track_count,
            ROUND(AVG(af.danceability), 3),
            ROUND(AVG(af.energy), 3),
            ROUND(AVG(af.valence), 3),
            ROUND(AVG(af.acousticness), 3),
            ROUND(AVG(af.party_index), 3),
            NOW()
        FROM tracks t
        JOIN audio_features af ON t.id     = af.track_id
        JOIN albums         al ON t.album_id = al.id
        WHERE al.release_year IS NOT NULL
          AND al.release_year BETWEEN 1950 AND EXTRACT(YEAR FROM NOW())::SMALLINT
        GROUP BY al.release_year
        ON CONFLICT (release_year) DO UPDATE SET
            track_count      = EXCLUDED.track_count,
            avg_danceability = EXCLUDED.avg_danceability,
            avg_energy       = EXCLUDED.avg_energy,
            avg_valence      = EXCLUDED.avg_valence,
            avg_acousticness = EXCLUDED.avg_acousticness,
            avg_party_index  = EXCLUDED.avg_party_index,
            updated_at       = NOW();
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    logger.info("gold_temporal_trends actualizada.")

# ===========================================================================
# Procesamiento de mensajes
# ===========================================================================

def _process_message(conn, raw_payload: dict) -> None:
    global _message_counter, _discarded_counter

    # PASO 1: Bronze — guardar siempre, incluso si falla la validación
    try:
        _insert_bronze(conn, raw_payload)
    except Exception as exc:
        logger.error("Error insertando en bronze_raw: %s", exc)
        conn = _get_pg_connection()
        return

    # PASO 2: Transformar y validar
    data = _transform(raw_payload)
    if data is None:
        _discarded_counter += 1
        return

    # PASO 3: Gold — insertar en esquema normalizado
    cur = conn.cursor()
    try:
        _upsert_artist(cur, data)
        _upsert_album(cur, data)
        _upsert_track(cur, data)
        _upsert_audio_features(cur, data)
        conn.commit()
        _message_counter += 1
    except Exception as exc:
        conn.rollback()
        logger.error("Error persistiendo track %s: %s", data.get("track_id"), exc)
        _discarded_counter += 1
    finally:
        cur.close()

    # PASO 4: Refrescar agregados cada REFRESH_EVERY_N mensajes
    if _message_counter > 0 and _message_counter % REFRESH_EVERY_N == 0:
        logger.info("[%d procesados | %d descartados] Refrescando agregados Gold...",
                    _message_counter, _discarded_counter)
        try:
            _refresh_genre_stats(conn)
            _refresh_temporal_trends(conn)
        except Exception as exc:
            logger.error("Error refrescando agregados: %s", exc)

# ===========================================================================
# Punto de entrada
# ===========================================================================

def main() -> None:
    conn = _get_pg_connection()

    def on_connect(client, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            logger.info("Conectado al broker MQTT. Suscribiendose a %s", MQTT_TOPIC)
            client.subscribe(MQTT_TOPIC, qos=1)
        else:
            logger.error("Fallo de conexion MQTT. Codigo: %s", reason_code)

    def on_message(client, userdata, msg):
        nonlocal conn
        try:
            data = json.loads(msg.payload.decode("utf-8"))
            _process_message(conn, data)
        except json.JSONDecodeError as exc:
            logger.error("Mensaje MQTT no es JSON valido: %s", exc)
        except Exception as exc:
            logger.error("Error inesperado: %s. Reconectando a PostgreSQL...", exc)
            conn = _get_pg_connection()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="spotify_consumer")
    client.on_connect = on_connect
    client.on_message = on_message

    while True:
        try:
            client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
            break
        except OSError:
            logger.warning("Broker MQTT no disponible, reintentando en 5 s...")
            time.sleep(5)

    logger.info("Consumidor MQTT iniciado. Esperando mensajes...")
    client.loop_forever()


if __name__ == "__main__":
    main()
