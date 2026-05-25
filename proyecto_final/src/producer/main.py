"""
src/producer/main.py
Servicio productor MQTT del pipeline IoT de Spotify.

Responsabilidades:
  1. Leer el CSV histórico y publicar cada fila en MQTT (data_source='csv').
  2. Llamar a la API de Spotify y publicar nuevos lanzamientos (data_source='api').

Mejoras:
  - Credenciales desde variables de entorno (sin hardcoding).
  - Campo data_source en cada payload para trazabilidad completa.
  - Reintentos automáticos de conexión al broker MQTT.
  - Velocidad configurable via CSV_PUBLISH_INTERVAL.
"""

import csv
import json
import logging
import os
import time

import paho.mqtt.client as mqtt

from spotify_client import stream_new_releases

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("producer")

MQTT_BROKER  = os.environ.get("MQTT_BROKER", "mosquitto")
MQTT_PORT    = int(os.environ.get("MQTT_PORT", 1883))
MQTT_TOPIC   = os.environ.get("MQTT_TOPIC", "spotify/tracks")
CSV_PATH     = "/app/data/spotify_tracks.csv"
CSV_INTERVAL = float(os.environ.get("CSV_PUBLISH_INTERVAL", 0.05))

API_CYCLE_INTERVAL = 3600


def _on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        logger.info("Conectado al broker MQTT en %s:%d", MQTT_BROKER, MQTT_PORT)
    else:
        logger.error("Fallo de conexion al broker MQTT. Codigo: %s", reason_code)


def publish(client: mqtt.Client, payload: dict) -> None:
    message = json.dumps(payload, ensure_ascii=False, default=str)
    result  = client.publish(MQTT_TOPIC, message, qos=1)
    result.wait_for_publish(timeout=5)
    logger.debug("Publicado: %s [%s]", payload.get("track_name"), payload.get("data_source"))


def publish_csv(client: mqtt.Client) -> None:
    if not os.path.exists(CSV_PATH):
        logger.warning("CSV no encontrado en %s — omitiendo.", CSV_PATH)
        return

    logger.info("Iniciando publicacion del CSV desde %s", CSV_PATH)
    count = 0
    with open(CSV_PATH, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            def _f(k): return float(row[k]) if row.get(k) else None
            def _i(k): return int(row[k])   if row.get(k) else None
            payload = {
                "track_id":         row.get("track_id") or row.get("id", ""),
                "track_name":       row.get("track_name") or row.get("name", ""),
                "artist_id":        row.get("artists", ""),
                "artist_name":      row.get("artists", ""),
                "album_id":         row.get("album_id", ""),
                "album_name":       row.get("album_name") or row.get("album", ""),
                "track_genre":      row.get("track_genre", "Unknown"),
                "duration_ms":      _i("duration_ms"),
                "explicit":         row.get("explicit", "False").lower() == "true",
                "popularity":       _i("popularity"),
                "danceability":     _f("danceability"),
                "energy":           _f("energy"),
                "valence":          _f("valence"),
                "tempo":            _f("tempo"),
                "acousticness":     _f("acousticness"),
                "instrumentalness": _f("instrumentalness"),
                "speechiness":      _f("speechiness"),
                "loudness":         _f("loudness"),
                "data_source":      "csv",
            }
            publish(client, payload)
            count += 1
            if count % 500 == 0:
                logger.info("CSV: %d tracks publicados...", count)
            time.sleep(CSV_INTERVAL)

    logger.info("CSV completado: %d tracks publicados.", count)


def publish_api(client: mqtt.Client) -> None:
    logger.info("Iniciando publicacion de nuevos lanzamientos desde la API de Spotify...")
    try:
        for track in stream_new_releases(limit=20):
            track["data_source"] = "api"
            publish(client, track)
            time.sleep(1)
    except Exception as exc:
        logger.error("Error al obtener datos de Spotify: %s", exc)


def main() -> None:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="spotify_producer")
    client.on_connect = _on_connect

    connected = False
    while not connected:
        try:
            client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
            connected = True
        except OSError:
            logger.warning("Broker no disponible, reintentando en 5 s...")
            time.sleep(5)

    client.loop_start()
    publish_csv(client)

    last_api_run = 0.0
    while True:
        now = time.time()
        if now - last_api_run >= API_CYCLE_INTERVAL:
            publish_api(client)
            last_api_run = time.time()
        time.sleep(60)


if __name__ == "__main__":
    main()