"""
src/consumer/main.py

Servicio consumidor MQTT del pipeline IoT de Spotify.

Implementa una arquitectura Medallon completa:
1. Capa Bronze: Almacenamiento del payload original en formato JSONB.
   Permite trazar y auditar el total de mensajes entrantes antes de cualquier alteracion.
2. Transformacion y Validacion: Normalizacion de tipos, verificacion de rangos y calculo
   de campos derivados (como party_index y release_year).
3. Capa Gold: Insercion de los datos limpios en un esquema relacional normalizado y
   actualizacion periodica de vistas materializadas o tablas de agregacion para
   su visualizacion en dashboards.
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

# Configuracion del sistema de trazabilidad (Logging)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("consumer")

# Variables de entorno para la conexion MQTT
MQTT_BROKER = os.environ.get("MQTT_BROKER", "mosquitto")
MQTT_PORT = int(os.environ.get("MQTT_PORT", 1883))
MQTT_TOPIC = os.environ.get("MQTT_TOPIC", "spotify/tracks")

# Variables de entorno para la base de datos PostgreSQL
PG_HOST = os.environ.get("POSTGRES_HOST", "postgres")
PG_PORT = int(os.environ.get("POSTGRES_PORT", 5432))
PG_DB = os.environ.get("POSTGRES_DB", "spotify_db")
PG_USER = os.environ.get("POSTGRES_USER", "spotify_user")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "")

# Umbral para refrescar las tablas agregadas de la capa Gold
REFRESH_EVERY_N = 500

# Contadores globales de telemetria interna
_message_counter = 0
_discarded_counter = 0


# Funciones de gestion de conexion a base de datos


def _get_pg_connection():
    """
    Establece y retorna una conexion persistente con la base de datos PostgreSQL.

    Implementa un patron de reintento infinito (con esperas de 5 segundos) para asegurar
    la resiliencia del servicio en caso de que el motor de base de datos no este
    listo o sufra interrupciones.

    Returns:
        psycopg2.extensions.connection: Objeto de conexion activa.
    """
    while True:
        try:
            conn = psycopg2.connect(
                host=PG_HOST,
                port=PG_PORT,
                dbname=PG_DB,
                user=PG_USER,
                password=PG_PASSWORD,
            )
            logger.info("Conexion a PostgreSQL establecida correctamente.")
            return conn
        except psycopg2.OperationalError as exc:
            logger.warning(
                "Servicio PostgreSQL no disponible (%s). Reintentando conexion en 5 segundos...",
                exc,
            )
            time.sleep(5)


# Funciones de transformacion, validacion y limpieza de datos (Transformer)


def _safe_float(value, min_val: float = 0.0, max_val: float = 1.0) -> Optional[float]:
    """
    Convierte un valor a flotante y verifica que se encuentre dentro de un rango permitido.

    Args:
        value: Valor de entrada a convertir.
        min_val (float): Limite inferior permitido (inclusivo).
        max_val (float): Limite superior permitido (inclusivo).

    Returns:
        float o None: El valor convertido si es valido y esta en rango; None en caso contrario.
    """
    try:
        f = float(value)
        return f if min_val <= f <= max_val else None
    except (ValueError, TypeError):
        return None


def _safe_int(value, min_val=None, max_val=None) -> Optional[int]:
    """
    Convierte un valor a numero entero de forma segura, respetando limites opcionales.

    Args:
        value: Valor de entrada a convertir.
        min_val (int, opcional): Limite inferior.
        max_val (int, opcional): Limite superior.

    Returns:
        int o None: El valor convertido y validado, o None si falla.
    """
    try:
        i = int(float(value))
        if min_val is not None and i < min_val:
            return None
        if max_val is not None and i > max_val:
            return None
        return i
    except (ValueError, TypeError):
        return None


def _extract_year(text: str) -> Optional[int]:
    """
    Extrae un ano valido (formato de 4 digitos entre 1950 y 2029) contenido dentro de
    una cadena de texto (usualmente el nombre de un album).

    Args:
        text (str): La cadena de texto a analizar.

    Returns:
        int o None: El ano extraido o None si no se encuentra coincidencia.
    """
    if not isinstance(text, str):
        return None
    matches = re.findall(r"\b(19[5-9]\d|20[0-2]\d)\b", text)
    return int(matches[0]) if matches else None


def _normalize_explicit(value) -> bool:
    """
    Normaliza el indicador de contenido explicito a un booleano estricto.

    Interpreta multiples formatos comunes (strings como "true", "1", o valores nulos).

    Args:
        value: El valor original del dataset.

    Returns:
        bool: True si el contenido es explicito, False en cualquier otro caso.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes")
    return bool(value) if value is not None else False


def _transform(raw: dict) -> Optional[dict]:
    """
    Filtro principal de transformacion de cada mensaje entrante.

    Recibe el payload en bruto, descarta aquellos que carezcan de un identificador unico,
    aplica reglas de truncamiento en strings para evitar desbordamientos en base de datos,
    y utiliza las funciones seguras para castear numericos.

    Args:
        raw (dict): Payload crudo extraido del mensaje MQTT.

    Returns:
        dict o None: Un diccionario estandarizado listo para insercion, o None si el
                     mensaje debe ser descartado.
    """
    track_id = raw.get("track_id")
    if not track_id or not str(track_id).strip():
        logger.warning("Registro descartado por carecer de track_id principal.")
        return None

    track_name = str(raw.get("track_name", "Unknown")).strip()[:255]
    artist_id = str(raw.get("artist_id", "")).strip()[:22]
    artist_name = str(raw.get("artist_name", "Unknown")).strip()[:255]
    album_id = str(raw.get("album_id", "")).strip()[:22]
    album_name = str(raw.get("album_name", "")).strip()[:255]
    track_genre = str(raw.get("track_genre", "Unknown")).strip()[:100]
    genres = raw.get("genres", [])
    if not isinstance(genres, list):
        genres = []

    popularity = _safe_int(raw.get("popularity"), 0, 100)
    danceability = _safe_float(raw.get("danceability"), 0.0, 1.0)
    energy = _safe_float(raw.get("energy"), 0.0, 1.0)
    valence = _safe_float(raw.get("valence"), 0.0, 1.0)
    tempo = _safe_float(raw.get("tempo"), 0.0, 300.0)
    loudness = _safe_float(raw.get("loudness"), -60.0, 5.0)
    acousticness = _safe_float(raw.get("acousticness"), 0.0, 1.0)
    instrumentalness = _safe_float(raw.get("instrumentalness"), 0.0, 1.0)
    speechiness = _safe_float(raw.get("speechiness"), 0.0, 1.0)
    liveness = _safe_float(raw.get("liveness"), 0.0, 1.0)
    key = _safe_int(raw.get("key"), 0, 11)
    mode = _safe_int(raw.get("mode"), 0, 1)
    time_sig = _safe_int(raw.get("time_signature"), 1, 7)

    duration_ms = _safe_int(raw.get("duration_ms"), 0)
    release_year = _extract_year(album_name)
    explicit = _normalize_explicit(raw.get("explicit", False))
    source = raw.get("data_source", "csv")
    if source not in ("csv", "api"):
        source = "csv"

    # Fecha oficial proporcionada unicamente por la API de Spotify
    release_date = raw.get("release_date")

    return {
        "track_id": str(track_id).strip()[:22],
        "track_name": track_name,
        "artist_id": artist_id or None,
        "artist_name": artist_name,
        "album_id": album_id or None,
        "album_name": album_name,
        "track_genre": track_genre,
        "genres": genres,
        "release_date": release_date,
        "release_year": release_year,
        "popularity": popularity if popularity is not None else 0,
        "danceability": danceability,
        "energy": energy,
        "valence": valence,
        "tempo": tempo,
        "loudness": loudness,
        "acousticness": acousticness,
        "instrumentalness": instrumentalness,
        "speechiness": speechiness,
        "liveness": liveness,
        "key": key,
        "mode": mode,
        "time_signature": time_sig,
        "duration_ms": duration_ms,
        "explicit": explicit,
        "source": source,
    }


# Operaciones de persistencia en Base de Datos (Capa Bronze y Gold)


def _insert_bronze(conn, payload_dict: dict) -> None:
    """
    Inserta el payload en su estado original (Bronze) en la base de datos utilizando el
    tipo JSONB nativo de PostgreSQL, lo cual garantiza integridad estructural inicial.

    Args:
        conn: Conexion a PostgreSQL.
        payload_dict (dict): Diccionario sin procesar.
    """
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO bronze_raw (payload) VALUES (%s);", (Json(payload_dict),)
        )
    conn.commit()


def _upsert_artist(cur, data: dict) -> None:
    """
    Inserta o actualiza un registro en la tabla 'artists'. Si existe, actualiza su genero.

    Args:
        cur: Cursor de base de datos activo.
        data (dict): Diccionario transformado.
    """
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
    """
    Inserta o ignora un registro en la tabla 'albums'. Reconstruye fechas parciales.

    Args:
        cur: Cursor de base de datos activo.
        data (dict): Diccionario transformado.
    """
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
        (
            data["album_id"],
            data["album_name"][:255],
            data.get("artist_id"),
            release_date,
        ),
    )


def _upsert_track(cur, data: dict) -> bool:
    """
    Inserta o actualiza metadatos principales en la tabla 'tracks'.

    Actualiza estadisticas volatiles como 'popularity' o el 'source' en caso de conflicto.

    Args:
        cur: Cursor de base de datos activo.
        data (dict): Diccionario transformado.

    Returns:
        bool: True si el registro afecto alguna fila, False si no.
    """
    cur.execute(
        """
        INSERT INTO tracks (
            id, name, artist_id, album_id,
            duration_ms, explicit, popularity, track_genre, source
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            popularity  = EXCLUDED.popularity,
            track_genre = EXCLUDED.track_genre,
            source      = EXCLUDED.source
        """,
        (
            data["track_id"],
            data["track_name"],
            data["artist_id"] or None,
            data["album_id"] or None,
            data["duration_ms"],
            data["explicit"],
            data["popularity"],
            data.get("track_genre") or None,
            data["source"],
        ),
    )
    return cur.rowcount > 0


def _upsert_audio_features(cur, data: dict) -> None:
    """
    Inserta metricas acusticas en la tabla 'audio_features'.

    Valida que exista informacion musical util antes de intentar persistir la fila.

    Args:
        cur: Cursor de base de datos activo.
        data (dict): Diccionario transformado.
    """
    fields_to_check = ("danceability", "energy", "valence")
    if all(data.get(k) is None for k in fields_to_check):
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
            data["danceability"],
            data["energy"],
            data["valence"],
            data["tempo"],
            data["loudness"],
            data["speechiness"],
            data["acousticness"],
            data["instrumentalness"],
            data["liveness"],
            data["key"],
            data["mode"],
            data["time_signature"],
        ),
    )


def _refresh_genre_stats(conn) -> None:
    """
    Recalcula las estadisticas agregadas por genero y actualiza la tabla 'gold_genre_stats'.

    Se encarga de promediar atributos vitales de negocio como party_index, popularity y tempo,
    permitiendo a los Dashboards de Grafana leer estos datos cacheados en lugar de escanear
    la tabla entera.

    Args:
        conn: Conexion activa a PostgreSQL.
    """
    sql = """
        INSERT INTO gold_genre_stats (
            track_genre, track_count,
            avg_popularity, avg_danceability, avg_energy,
            avg_valence, avg_tempo, avg_party_index, updated_at
        )
        SELECT
            COALESCE(t.track_genre, a.genres[1], 'Unknown') AS track_genre,
            COUNT(*)                                         AS track_count,
            ROUND(AVG(t.popularity), 2),
            ROUND(AVG(af.danceability), 3),
            ROUND(AVG(af.energy), 3),
            ROUND(AVG(af.valence), 3),
            ROUND(AVG(af.tempo), 2),
            ROUND(AVG(af.party_index), 3),
            NOW()
        FROM tracks t
        LEFT JOIN artists a  ON t.artist_id = a.id
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
    logger.info("Tabla agregada gold_genre_stats ha sido actualizada con exito.")


def _refresh_temporal_trends(conn) -> None:
    """
    Recalcula la evolucion temporal y metricas medias agrupadas por ano de lanzamiento.
    Actualiza la tabla 'gold_temporal_trends'.

    Filtra fechas inconsistentes o anomalas y genera una foto analitica rapida.

    Args:
        conn: Conexion activa a PostgreSQL.
    """
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
        JOIN audio_features af ON t.id      = af.track_id
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
    logger.info("Tabla agregada gold_temporal_trends ha sido actualizada con exito.")


# Manejo y Ciclo de Vida de los Mensajes


def _process_message(conn, raw_payload: dict) -> None:
    """
    Orquesta la recepcion de un mensaje individual a traves del pipeline.

    Fases:
    1. Resguardo del dato crudo en Bronze.
    2. Transformacion en memoria.
    3. Insercion normalizada en Gold (artistas, albumes, pistas y propiedades acusticas).
    4. Evaluacion para lanzar regeneracion de vistas materializadas si se alcanza el umbral.

    Maneja excepciones silenciosamente para no detener el consumidor, registrando el fallo.

    Args:
        conn: Conexion activa a PostgreSQL.
        raw_payload (dict): Payload extraido del mensaje recibido.
    """
    global _message_counter, _discarded_counter

    # Fase 1: Preservacion en la capa Bronze
    try:
        _insert_bronze(conn, raw_payload)
    except Exception as exc:
        logger.error("Fallo al insertar datos en la capa bronze_raw: %s", exc)
        # Se asume una desconexion y se solicita reconectar
        conn = _get_pg_connection()
        return

    # Fase 2: Validacion e higienizacion
    data = _transform(raw_payload)
    if data is None:
        _discarded_counter += 1
        return

    # Fase 3: Integracion en el modelo relacional (Gold)
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
        logger.error(
            "Fallo al persistir la pista %s en modelo relacional: %s",
            data.get("track_id"),
            exc,
        )
        _discarded_counter += 1
    finally:
        cur.close()

    # Fase 4: Actualizacion periodica de analiticas para evitar recalculo por cada fila
    if _message_counter > 0 and _message_counter % REFRESH_EVERY_N == 0:
        logger.info(
            "Ciclo de consolidacion: %d procesados | %d descartados. "
            "Lanzando actualizacion analitica...",
            _message_counter,
            _discarded_counter,
        )
        try:
            _refresh_genre_stats(conn)
            _refresh_temporal_trends(conn)
        except Exception as exc:
            logger.error(
                "Error critico al recalcular las vistas de agregacion: %s", exc
            )


# Bloque Principal


def main() -> None:
    """
    Inicializacion del demonio consumidor.

    Instancia la base de datos y el cliente MQTT. Configura los manejadores de eventos
    para establecer la subscripcion y despachar cada mensaje entrante al ciclo de
    procesamiento de datos de forma indefinida.
    """
    conn = _get_pg_connection()

    def on_connect(client, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            logger.info(
                "Conexion MQTT exitosa. Iniciando escucha permanente en el topico: %s",
                MQTT_TOPIC,
            )
            client.subscribe(MQTT_TOPIC, qos=1)
        else:
            logger.error(
                "Fallo durante el handshake MQTT. Codigo devuelto: %s", reason_code
            )

    def on_message(client, userdata, msg):
        nonlocal conn
        try:
            data = json.loads(msg.payload.decode("utf-8"))
            _process_message(conn, data)
        except json.JSONDecodeError as exc:
            logger.error(
                "Estructura invalida recibida: El mensaje no es un JSON parseable. "
                "Detalles: %s",
                exc,
            )
        except Exception as exc:
            logger.error(
                "Inestabilidad no controlada en el procesamiento: %s. "
                "Procediendo a regenerar conexion...",
                exc,
            )
            conn = _get_pg_connection()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="spotify_consumer")
    client.on_connect = on_connect
    client.on_message = on_message

    # Estrategia de reintento sobre el socket de conexion MQTT
    while True:
        try:
            client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
            break
        except OSError:
            logger.warning(
                "El broker MQTT esta rechazando conexiones. "
                "Nuevo intento en 5 segundos..."
            )
            time.sleep(5)

    logger.info("Motor consumidor MQTT cargado. En espera de streams entrantes.")
    client.loop_forever()


if __name__ == "__main__":
    main()
