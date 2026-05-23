# ============================================================
# src/publisher.py
# Publicador MQTT — Simulación de streaming de datos
#
# Lee el CSV de Spotify y envía cada fila como un mensaje
# JSON a través del broker Mosquitto, simulando un flujo
# de datos en tiempo real (como si vinieran de una API
# o un sistema de captura de eventos).
#
# Uso:
#   cd src/
#   python publisher.py
#
# Prerequisitos:
#   - Docker Compose levantado (broker Mosquitto activo)
#   - CSV descargado en datos/dataset.csv
# ============================================================

import json
import time
import logging
import sys
import os

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import paho.mqtt.client as mqtt

# Añadimos el directorio src/ al path para los imports locales
sys.path.insert(0, os.path.dirname(__file__))
import src.config as config
from src.transformer import load_csv_as_records

# --- Configuración de logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PUBLISHER] %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ============================================================
# Callbacks del cliente MQTT
# ============================================================

def on_connect(client, userdata, flags, rc):
    """Se ejecuta cuando el cliente conecta al broker."""
    connection_codes = {
        0: "Conexión exitosa",
        1: "Protocolo incorrecto",
        2: "Client ID rechazado",
        3: "Broker no disponible",
        4: "Credenciales incorrectas",
        5: "No autorizado",
    }
    message = connection_codes.get(rc, f"Error desconocido (código {rc})")
    if rc == 0:
        logger.info(f"Conectado al broker MQTT — {message}")
    else:
        logger.error(f"Error al conectar: {message}")
        sys.exit(1)


def on_publish(client, userdata, mid):
    """
    Se ejecuta cuando el broker confirma la recepción de un mensaje.
    Con QoS 1, esto garantiza entrega al menos una vez.
    Descomentar el log para depuración detallada.
    """
    # logger.debug(f"Mensaje mid={mid} confirmado por el broker.")
    pass


# ============================================================
# Lógica de publicación
# ============================================================

def create_mqtt_client() -> mqtt.Client:
    """
    Inicializa y configura el cliente MQTT del publicador.
    El client_id debe ser único para evitar desconexiones.
    """
    client = mqtt.Client(client_id=config.MQTT_CLIENT_ID_PUB)
    client.on_connect = on_connect
    client.on_publish = on_publish
    return client


def publish_tracks(client: mqtt.Client, records: list):
    """
    Itera sobre los registros del CSV y publica cada uno
    como un mensaje JSON en el topic configurado.

    Con QoS 1, cada mensaje espera la confirmación PUBACK
    del broker antes de considerar la entrega exitosa.

    Args:
        client:  Cliente MQTT ya conectado al broker.
        records: Lista de dicts con los datos de los tracks.
    """
    total = len(records)
    logger.info(f"Iniciando streaming de {total} tracks hacia '{config.MQTT_TOPIC}'...")
    logger.info(f"Velocidad configurada: {1/config.PUBLISH_DELAY_SECONDS:.0f} tracks/seg "
                f"(delay={config.PUBLISH_DELAY_SECONDS}s)")

    start_time = time.time()

    for i, record in enumerate(records, start=1):
        # Serializar a JSON (default=str maneja tipos como NaN o Timestamps)
        payload_str = json.dumps(record, default=str)

        # Publicar con QoS 1 (entrega garantizada)
        result = client.publish(
            topic=config.MQTT_TOPIC,
            payload=payload_str,
            qos=config.MQTT_QOS,
        )

        # Esperar confirmación del broker (PUBACK)
        result.wait_for_publish()

        # Log de progreso cada 500 tracks o al terminar
        if i % 500 == 0 or i == total:
            elapsed = time.time() - start_time
            rate = i / elapsed if elapsed > 0 else 0
            logger.info(f"Progreso: {i:>6}/{total} tracks | "
                        f"{rate:.0f} tracks/seg | "
                        f"{elapsed:.1f}s transcurridos")

        # Pausa para simular streaming real (configurable en config.py)
        time.sleep(config.PUBLISH_DELAY_SECONDS)

    elapsed_total = time.time() - start_time
    logger.info(f"✅ Streaming completado: {total} tracks en {elapsed_total:.1f}s")

# ============================================================
# FUENTE 2: API de Spotify — Nuevos lanzamientos en tiempo real
# ============================================================

def create_spotify_client() -> spotipy.Spotify:
    """
    Inicializa el cliente de la API de Spotify usando
    Client Credentials Flow (sin login de usuario).
    Solo permite acceder a datos públicos, que es todo
    lo que necesitamos: nuevos lanzamientos y audio features.

    Returns:
        Instancia autenticada de spotipy.Spotify.

    Raises:
        spotipy.SpotifyException: Si las credenciales son incorrectas.
    """
    auth_manager = SpotifyClientCredentials(
        client_id=config.SPOTIFY_CLIENT_ID,
        client_secret=config.SPOTIFY_CLIENT_SECRET,
    )
    return spotipy.Spotify(auth_manager=auth_manager)


def fetch_spotify_new_releases(sp: spotipy.Spotify) -> list:
    """
    Consulta la API de Spotify para obtener los álbumes más recientes
    a nivel mundial, extrae sus tracks y los enriquece con
    audio features y géneros del artista.

    Flujo de llamadas a la API:
        1. new_releases()         → lista de álbumes nuevos
        2. album_tracks()         → tracks de cada álbum
        3. tracks()               → detalles (popularity, explicit…)  [batch]
        4. audio_features()       → danceability, energy, valence…   [batch]
        5. artists()              → géneros del artista principal     [batch]

    Las llamadas en batch (3, 4, 5) minimizan el número de requests.
    La API de Spotify permite hasta 50 items por llamada en batch.

    Args:
        sp: Cliente Spotify autenticado.

    Returns:
        Lista de dicts con exactamente las mismas claves que usa
        el CSV, listos para publicar en MQTT.
    """
    logger.info("Consultando nuevos lanzamientos en la API de Spotify...")

    # --- PASO 1: obtener álbumes de nuevos lanzamientos ---
    response = sp.new_releases(limit=config.SPOTIFY_NEW_RELEASES_LIMIT)
    albums   = response.get("albums", {}).get("items", [])
    logger.info(f"Álbumes nuevos obtenidos: {len(albums)}")

    # --- PASO 2: extraer todos los track_ids de esos álbumes ---
    # Usamos los tracks del objeto álbum (simplificados) para obtener IDs
    raw_track_items = []  # (track_simple, album_obj)
    for album in albums:
        try:
            album_tracks = sp.album_tracks(album["id"], limit=50)
            for track in album_tracks.get("items", []):
                raw_track_items.append((track, album))
                if len(raw_track_items) >= config.SPOTIFY_MAX_TRACKS:
                    break
        except Exception as e:
            logger.warning(f"Error obteniendo tracks del álbum '{album.get('name')}': {e}")
        if len(raw_track_items) >= config.SPOTIFY_MAX_TRACKS:
            break

    if not raw_track_items:
        logger.warning("No se obtuvieron tracks de la API.")
        return []

    track_ids = [t["id"] for t, _ in raw_track_items if t.get("id")]
    logger.info(f"Tracks a procesar: {len(track_ids)}")

    # --- PASO 3: obtener detalles completos (popularity, explicit) ---
    # La API acepta hasta 50 IDs por llamada → hacemos batches
    full_tracks_map = {}  # track_id → track_object
    for i in range(0, len(track_ids), 50):
        batch = track_ids[i:i+50]
        try:
            result = sp.tracks(batch)
            for t in result.get("tracks", []):
                if t:
                    full_tracks_map[t["id"]] = t
        except Exception as e:
            logger.warning(f"Error en batch de tracks (offset {i}): {e}")

    # --- PASO 4: obtener audio features (danceability, energy, valence…) ---
    audio_features_map = {}  # track_id → features_dict
    for i in range(0, len(track_ids), 50):
        batch = track_ids[i:i+50]
        try:
            features_list = sp.audio_features(batch)
            for f in (features_list or []):
                if f:
                    audio_features_map[f["id"]] = f
        except Exception as e:
            logger.warning(f"Error en batch de audio features (offset {i}): {e}")

    # --- PASO 5: obtener géneros del artista principal ---
    # Recopilamos los artist_ids únicos primero para minimizar llamadas
    artist_ids_unique = list({
        t["artists"][0]["id"]
        for t, _ in raw_track_items
        if t.get("artists")
    })
    artists_genre_map = {}  # artist_id → género (str)
    for i in range(0, len(artist_ids_unique), 50):
        batch = artist_ids_unique[i:i+50]
        try:
            result = sp.artists(batch)
            for artist in result.get("artists", []):
                if artist:
                    genres = artist.get("genres", [])
                    # Tomamos el primer género o "new_release" si no hay
                    artists_genre_map[artist["id"]] = genres[0] if genres else "new_release"
        except Exception as e:
            logger.warning(f"Error en batch de artistas (offset {i}): {e}")

    # --- PASO 6: ensamblar los payloads finales ---
    payloads = []
    for track_simple, album in raw_track_items:
        tid = track_simple.get("id")
        if not tid:
            continue

        full   = full_tracks_map.get(tid, {})
        feats  = audio_features_map.get(tid, {})
        artist_id = (track_simple.get("artists") or [{}])[0].get("id", "")
        genre  = artists_genre_map.get(artist_id, "new_release")

        payload = _map_to_common_schema(
            track_id    = tid,
            track_name  = track_simple.get("name", "Unknown"),
            artists     = ", ".join(a["name"] for a in track_simple.get("artists", [])),
            album_name  = album.get("name", ""),
            track_genre = genre,
            popularity  = full.get("popularity", 0),
            explicit    = full.get("explicit", False),
            duration_ms = track_simple.get("duration_ms", 0),
            features    = feats,
        )
        payloads.append(payload)

    logger.info(f"Payloads de API listos para publicar: {len(payloads)}")
    return payloads


def _map_to_common_schema(
    track_id, track_name, artists, album_name, track_genre,
    popularity, explicit, duration_ms, features: dict
) -> dict:
    """
    Mapea los datos de la API de Spotify al mismo esquema de campos
    que usa el CSV de Kaggle. Garantiza que ambas fuentes producen
    payloads con exactamente las mismas claves → el subscriber y
    el transformer los procesan de forma idéntica, sin saber de
    qué fuente vienen.

    Args:
        features: Diccionario de audio features devuelto por la API
                  (puede ser {} si la API no devolvió datos para ese track).

    Returns:
        Dict con las mismas claves que un registro del CSV.
    """
    return {
        "track_id":         track_id,
        "track_name":       track_name,
        "artists":          artists,
        "album_name":       album_name,
        "track_genre":      track_genre,
        "popularity":       popularity,
        "danceability":     features.get("danceability"),
        "energy":           features.get("energy"),
        "valence":          features.get("valence"),
        "tempo":            features.get("tempo"),
        "acousticness":     features.get("acousticness"),
        "instrumentalness": features.get("instrumentalness"),
        "speechiness":      features.get("speechiness"),
        "duration_ms":      duration_ms,
        "explicit":         explicit,
        # Campo extra para trazabilidad: el subscriber lo guardará
        # en Bronze, así podéis filtrar por fuente en el dashboard.
        "data_source":      "spotify_api",
    }

# ============================================================
# Punto de entrada
# ============================================================

def main():
    """
    Orquesta las dos fuentes de datos independientes:

        Fuente 1 (CSV histórico):   ~114k tracks del dataset de Kaggle
        Fuente 2 (API en tiempo real): nuevos lanzamientos de Spotify

    Ambas publican al mismo topic MQTT con el mismo esquema de campos.
    El subscriber y el transformer los procesan de forma idéntica.
    El campo 'data_source' en el payload permite distinguirlos en BD.
    """

    # ── FUENTE 1: CSV histórico ────────────────────────────────────────
    logger.info("=" * 55)
    logger.info("FUENTE 1: CSV histórico de Kaggle")
    logger.info("=" * 55)

    csv_records = []
    try:
        csv_records = load_csv_as_records(config.CSV_PATH, config.SELECTED_COLUMNS)
        # Añadimos la etiqueta de fuente para trazabilidad
        for r in csv_records:
            r["data_source"] = "csv_kaggle"
    except FileNotFoundError:
        logger.warning(
            f"CSV no encontrado en '{config.CSV_PATH}'. "
            "Se omite la fuente histórica y se continúa con la API."
        )

    # ── FUENTE 2: API de Spotify (nuevos lanzamientos) ─────────────────
    logger.info("=" * 55)
    logger.info("FUENTE 2: API de Spotify — Nuevos lanzamientos")
    logger.info("=" * 55)

    api_records = []
    if config.SPOTIFY_CLIENT_ID == "TU_CLIENT_ID_AQUI":
        logger.warning(
            "Credenciales de Spotify no configuradas en config.py. "
            "Se omite la fuente de API."
        )
    else:
        try:
            sp = create_spotify_client()
            api_records = fetch_spotify_new_releases(sp)
        except Exception as e:
            logger.error(f"Error conectando a la API de Spotify: {e}")

    # ── VALIDACIÓN: al menos una fuente debe tener datos ───────────────
    total_records = len(csv_records) + len(api_records)
    if total_records == 0:
        logger.error("Sin datos de ninguna fuente. Abortando.")
        return

    logger.info("=" * 55)
    logger.info(f"Resumen de fuentes:")
    logger.info(f"  CSV Kaggle:   {len(csv_records):>6} tracks")
    logger.info(f"  Spotify API:  {len(api_records):>6} tracks")
    logger.info(f"  TOTAL:        {total_records:>6} tracks")
    logger.info("=" * 55)

    # ── CONEXIÓN AL BROKER MQTT ────────────────────────────────────────
    client = create_mqtt_client()
    try:
        client.connect(config.MQTT_BROKER_HOST, config.MQTT_BROKER_PORT, keepalive=60)
    except ConnectionRefusedError:
        logger.error(
            f"No se pudo conectar al broker en "
            f"{config.MQTT_BROKER_HOST}:{config.MQTT_BROKER_PORT}.\n"
            "¿Está el Docker Compose levantado? Prueba: docker-compose up -d"
        )
        return

    client.loop_start()

    try:
        # Publicamos primero los tracks de la API (son pocos y son "nuevos")
        # y luego el CSV histórico (muchos más, tarda más tiempo).
        if api_records:
            logger.info("Publicando tracks de la API de Spotify...")
            publish_tracks(client, api_records)

        if csv_records:
            logger.info("Publicando tracks del CSV histórico...")
            publish_tracks(client, csv_records)

    except KeyboardInterrupt:
        logger.info("Publicación interrumpida por el usuario (Ctrl+C).")
    finally:
        client.loop_stop()
        client.disconnect()
        logger.info("Desconectado del broker MQTT.")


if __name__ == "__main__":
    main()
