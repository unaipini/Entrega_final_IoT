"""
src/consumer/main.py
Servicio consumidor MQTT del pipeline IoT de Spotify.

Responsabilidades:
  1. Suscribirse al topic MQTT donde el productor publica las pistas.
  2. Por cada mensaje recibido, deserializar el JSON.
  3. Calcular party_index = (danceability + energy + valence) / 3.
  4. Persistir la pista y sus audio_features en PostgreSQL.

El consumidor es tolerante a fallos: si PostgreSQL no está disponible
al arrancar, reintenta la conexión cada 5 segundos.
"""

import json
import logging
import os
import time

import paho.mqtt.client as mqtt
import psycopg2
from psycopg2.extras import execute_values

# ---------------------------------------------------------------------------
# Configuración del logger
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("consumer")

# ---------------------------------------------------------------------------
# Variables de entorno
# ---------------------------------------------------------------------------
MQTT_BROKER = os.environ.get("MQTT_BROKER", "mosquitto")
MQTT_PORT = int(os.environ.get("MQTT_PORT", 1883))
MQTT_TOPIC = os.environ.get("MQTT_TOPIC", "spotify/tracks")

PG_HOST = os.environ.get("POSTGRES_HOST", "postgres")
PG_PORT = int(os.environ.get("POSTGRES_PORT", 5432))
PG_DB = os.environ.get("POSTGRES_DB", "spotify_db")
PG_USER = os.environ.get("POSTGRES_USER", "spotify_user")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "")


def _get_pg_connection():
    """Devuelve una conexión activa a PostgreSQL. Reintenta si falla."""
    while True:
        try:
            conn = psycopg2.connect(
                host=PG_HOST,
                port=PG_PORT,
                dbname=PG_DB,
                user=PG_USER,
                password=PG_PASSWORD,
            )
            logger.info("Conexión a PostgreSQL establecida.")
            return conn
        except psycopg2.OperationalError as exc:
            logger.warning("PostgreSQL no disponible (%s). Reintentando en 5 s...", exc)
            time.sleep(5)


def _safe_float(value, default=None):
    """Convierte a float de forma segura; devuelve default si el valor es None o vacío."""
    try:
        return float(value) if value not in (None, "", "None") else default
    except (ValueError, TypeError):
        return default


def _safe_int(value, default=None):
    """Convierte a int de forma segura."""
    try:
        return int(value) if value not in (None, "", "None") else default
    except (ValueError, TypeError):
        return default


def _upsert_artist(cur, data: dict) -> None:
    """Inserta o actualiza el artista en la tabla artists."""
    artist_id = data.get("artist_id")
    artist_name = data.get("artist_name")
    if not artist_id or not artist_name:
        return

    cur.execute(
        """
        INSERT INTO artists (id, name)
        VALUES (%s, %s)
        ON CONFLICT (id) DO NOTHING
        """,
        (artist_id[:22], artist_name[:255]),
    )


def _upsert_track(cur, data: dict) -> bool:
    """
    Inserta la pista en la tabla tracks.
    Devuelve True si se insertó un registro nuevo, False si ya existía.
    """
    cur.execute(
        """
        INSERT INTO tracks (id, name, artist_id, album_id, duration_ms, explicit, popularity, source)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING
        """,
        (
            data["track_id"][:22],
            data.get("track_name", "")[:255],
            data.get("artist_id", "")[:22] if data.get("artist_id") else None,
            data.get("album_id", "")[:22] if data.get("album_id") else None,
            _safe_int(data.get("duration_ms")),
            bool(data.get("explicit", False)),
            _safe_int(data.get("popularity")),
            data.get("source", "csv"),
        ),
    )
    return cur.rowcount > 0


def _upsert_audio_features(cur, data: dict) -> None:
    """
    Inserta las características de audio en audio_features.

    El campo party_index está definido como columna GENERATED en PostgreSQL,
    por lo que no se inserta explícitamente: la BD lo calcula sola.
    """
    danceability = _safe_float(data.get("danceability"))
    energy = _safe_float(data.get("energy"))
    valence = _safe_float(data.get("valence"))

    # Solo insertamos si tenemos al menos uno de los tres campos clave
    if all(v is None for v in (danceability, energy, valence)):
        return

    cur.execute(
        """
        INSERT INTO audio_features (
            track_id, danceability, energy, valence,
            tempo, loudness, speechiness, acousticness,
            instrumentalness, liveness
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (track_id) DO NOTHING
        """,
        (
            data["track_id"][:22],
            danceability,
            energy,
            valence,
            _safe_float(data.get("tempo")),
            _safe_float(data.get("loudness")),
            _safe_float(data.get("speechiness")),
            _safe_float(data.get("acousticness")),
            _safe_float(data.get("instrumentalness")),
            _safe_float(data.get("liveness")),
        ),
    )


def _process_message(conn, data: dict) -> None:
    """
    Persiste en PostgreSQL los datos de una pista recibida por MQTT.

    Usa una única transacción por mensaje para garantizar atomicidad:
    si falla la inserción de audio_features, también se revierte la pista.
    """
    track_id = data.get("track_id")
    if not track_id:
        logger.warning("Mensaje recibido sin track_id — ignorado.")
        return

    cur = conn.cursor()
    try:
        _upsert_artist(cur, data)
        is_new = _upsert_track(cur, data)
        if is_new:
            _upsert_audio_features(cur, data)
        conn.commit()
        if is_new:
            logger.info("Pista persistida: %s (%s)", data.get("track_name"), track_id)
    except Exception as exc:
        conn.rollback()
        logger.error("Error al persistir la pista %s: %s", track_id, exc)
    finally:
        cur.close()


def main() -> None:
    """Punto de entrada del consumidor MQTT."""

    # Conexión inicial a PostgreSQL (con reintentos)
    conn = _get_pg_connection()

    def on_connect(client, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            logger.info("Conectado al broker MQTT. Suscribiéndose a %s", MQTT_TOPIC)
            client.subscribe(MQTT_TOPIC, qos=1)
        else:
            logger.error("Fallo de conexión MQTT. Código: %s", reason_code)

    def on_message(client, userdata, msg):
        """Callback invocado por cada mensaje MQTT recibido."""
        try:
            data = json.loads(msg.payload.decode("utf-8"))
            _process_message(conn, data)
        except json.JSONDecodeError as exc:
            logger.error("Mensaje MQTT no es JSON válido: %s", exc)
        except Exception as exc:
            # Reconectar a PostgreSQL si la conexión se perdió
            logger.error("Error inesperado: %s. Reconectando a PostgreSQL...", exc)
            nonlocal conn
            conn = _get_pg_connection()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="spotify_consumer")
    client.on_connect = on_connect
    client.on_message = on_message

    # Reintentar conexión al broker si no está disponible
    while True:
        try:
            client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
            break
        except OSError:
            logger.warning("Broker MQTT no disponible, reintentando en 5 s...")
            time.sleep(5)

    logger.info("Consumidor MQTT iniciado. Esperando mensajes...")
    # loop_forever() bloquea el hilo principal y gestiona reconexiones automáticas
    client.loop_forever()


if __name__ == "__main__":
    main()
