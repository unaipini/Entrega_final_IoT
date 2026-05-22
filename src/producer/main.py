"""
src/producer/main.py
Servicio productor MQTT del pipeline IoT de Spotify.

Responsabilidades:
  1. Leer filas del CSV histórico y publicarlas en el topic MQTT (source=csv).
  2. Llamar a la API de Spotify y publicar nuevos lanzamientos (source=api).

Cada mensaje publicado es un JSON con los campos de una pista.
El consumidor MQTT (src/consumer/main.py) recibe esos mensajes y los persiste
en PostgreSQL.
"""

import csv
import json
import logging
import os
import time

import paho.mqtt.client as mqtt

from spotify_client import stream_new_releases

# ---------------------------------------------------------------------------
# Configuración del logger
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("producer")

# ---------------------------------------------------------------------------
# Variables de entorno
# ---------------------------------------------------------------------------
MQTT_BROKER = os.environ.get("MQTT_BROKER", "mosquitto")
MQTT_PORT = int(os.environ.get("MQTT_PORT", 1883))
MQTT_TOPIC = os.environ.get("MQTT_TOPIC", "spotify/tracks")
CSV_PATH = "/app/data/spotify_tracks.csv"
CSV_INTERVAL = float(os.environ.get("CSV_PUBLISH_INTERVAL", 5))

# Intervalo en segundos entre los ciclos de la API de Spotify
API_CYCLE_INTERVAL = 3600  # 1 hora


def _on_connect(client: mqtt.Client, userdata, flags, reason_code, properties=None):
    """Callback invocado cuando el cliente se conecta al broker."""
    if reason_code == 0:
        logger.info("Conectado al broker MQTT en %s:%d", MQTT_BROKER, MQTT_PORT)
    else:
        logger.error("Fallo de conexión al broker MQTT. Código: %s", reason_code)


def publish(client: mqtt.Client, payload: dict) -> None:
    """Serializa el payload a JSON y lo publica en el topic MQTT."""
    message = json.dumps(payload, ensure_ascii=False)
    result = client.publish(MQTT_TOPIC, message, qos=1)
    result.wait_for_publish(timeout=5)
    logger.debug("Publicado en %s: %s", MQTT_TOPIC, payload.get("track_name"))


def publish_csv(client: mqtt.Client) -> None:
    """
    Lee el CSV fila a fila y publica cada pista en MQTT con source='csv'.

    El intervalo entre publicaciones está controlado por CSV_INTERVAL para
    simular el comportamiento de un dispositivo IoT que envía datos
    de forma continua a una tasa controlada.
    """
    if not os.path.exists(CSV_PATH):
        logger.warning("CSV no encontrado en %s — omitiendo.", CSV_PATH)
        return

    logger.info("Iniciando publicación del CSV desde %s", CSV_PATH)
    with open(CSV_PATH, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            payload = {
                "track_id": row.get("id", ""),
                "track_name": row.get("name", ""),
                "artist_id": row.get("artists", ""),
                "artist_name": row.get("artists", ""),
                "album_id": row.get("album_id", ""),
                "album_name": row.get("album", ""),
                "duration_ms": int(row["duration_ms"]) if row.get("duration_ms") else None,
                "explicit": row.get("explicit", "False").lower() == "true",
                "popularity": int(row["popularity"]) if row.get("popularity") else None,
                "danceability": float(row["danceability"]) if row.get("danceability") else None,
                "energy": float(row["energy"]) if row.get("energy") else None,
                "valence": float(row["valence"]) if row.get("valence") else None,
                "tempo": float(row["tempo"]) if row.get("tempo") else None,
                "loudness": float(row["loudness"]) if row.get("loudness") else None,
                "source": "csv",
            }
            publish(client, payload)
            time.sleep(CSV_INTERVAL)


def publish_api(client: mqtt.Client) -> None:
    """
    Obtiene nuevos lanzamientos de Spotify y los publica en MQTT (source='api').

    Se invoca una vez por ciclo (definido por API_CYCLE_INTERVAL).
    """
    logger.info("Iniciando publicación de nuevos lanzamientos desde la API de Spotify...")
    try:
        for track in stream_new_releases(limit=20):
            publish(client, track)
            time.sleep(1)  # Pequeña pausa para no saturar el broker
    except Exception as exc:
        logger.error("Error al obtener datos de Spotify: %s", exc)


def main() -> None:
    """
    Punto de entrada del productor.

    Bucle principal:
      1. Publica todas las filas del CSV una vez al arrancar.
      2. Cada hora llama a la API de Spotify y publica los nuevos lanzamientos.
    """
    # Configurar cliente MQTT
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="spotify_producer")
    client.on_connect = _on_connect

    # Reintentar la conexión al broker hasta que esté disponible
    connected = False
    while not connected:
        try:
            client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
            connected = True
        except OSError:
            logger.warning("Broker no disponible, reintentando en 5 s...")
            time.sleep(5)

    client.loop_start()

    # --- Publicar CSV al arrancar ---
    publish_csv(client)

    # --- Publicar API en bucle horario ---
    last_api_run = 0.0
    while True:
        now = time.time()
        if now - last_api_run >= API_CYCLE_INTERVAL:
            publish_api(client)
            last_api_run = time.time()
        time.sleep(60)


if __name__ == "__main__":
    main()
