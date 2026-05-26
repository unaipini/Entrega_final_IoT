"""
airflow/dags/spotify_etl.py

DAG de Apache Airflow que orquesta el pipeline ETL (Extraccion, Transformacion y Carga)
de datos provenientes de la API de Spotify.

Estructura del Flujo de Trabajo (DAG):
1. extract_from_api: Recupera informacion en bruto de los endpoints de Spotify (lanzamientos
   recientes y busquedas especificas) y la almacena temporalmente utilizando XCom.
2. transform: Procesa los datos crudos, normalizando formatos de fechas, calculando campos
   derivados exclusivos del dominio de negocio (como party_index) y descartando anomalias.
3. load_to_postgres: Persiste los datos transformados en el data warehouse (PostgreSQL)
   mediante operaciones idempotentes para evitar duplicidad ante posibles re-ejecuciones.

Frecuencia de ejecucion: Configurada para ejecutarse cada hora y proveer un refresco
constante de datos en la demo.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

import psycopg2
import spotipy
from airflow.decorators import dag, task
from spotipy.oauth2 import SpotifyClientCredentials

# Configuracion del registrador de eventos (Logger)
logger = logging.getLogger(__name__)

# Diccionario de argumentos predeterminados para la inicializacion del DAG
DEFAULT_ARGS = {
    "owner": "iot_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Obtencion de parametros y credenciales de acceso desde el entorno seguro
SPOTIFY_CLIENT_ID = os.environ.get("SPOTIFY_CLIENT_ID", "")
SPOTIFY_CLIENT_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET", "")

PG_HOST = os.environ.get("POSTGRES_HOST", "postgres")
PG_PORT = int(os.environ.get("POSTGRES_PORT", 5432))
PG_DB = os.environ.get("POSTGRES_DB", "spotify_db")
PG_USER = os.environ.get("POSTGRES_USER", "spotify_user")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "")


def _get_spotify_client() -> spotipy.Spotify:
    """
    Inicializa y devuelve una instancia autenticada del cliente de Spotify.

    Returns:
        spotipy.Spotify: Cliente configurado para realizar peticiones a la API.
    """
    credentials = SpotifyClientCredentials(
        client_id=SPOTIFY_CLIENT_ID,
        client_secret=SPOTIFY_CLIENT_SECRET,
    )
    return spotipy.Spotify(auth_manager=credentials)


def _get_pg_connection():
    """
    Establece una nueva conexion sincrona hacia la base de datos PostgreSQL.

    Returns:
        psycopg2.extensions.connection: Conexion activa lista para ejecutar transacciones.
    """
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


@dag(
    dag_id="spotify_etl",
    description=(
        "Pipeline ETL: extrae canciones de Spotify, "
        "calcula party_index y carga en PostgreSQL."
    ),
    default_args=DEFAULT_ARGS,
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["spotify", "iot", "etl"],
)
def spotify_etl():
    """
    Declaracion principal del Directed Acyclic Graph (DAG) para el pipeline ETL.

    Utiliza el decorador @dag propio de la API moderna de Airflow (TaskFlow API)
    que permite una sintaxis mas clara y pasaje automatico de variables XCom
    entre tareas decoradas con @task.
    """

    @task
    def extract_from_api() -> dict:
        """
        Primera tarea del pipeline (Extraccion).

        Interactua con la API de Spotify empleando multiples endpoints:
        - Endpoint de 'Nuevos Lanzamientos' para garantizar frescura de datos.
        - Endpoint de 'Busqueda' enfocado en el genero pop para ampliar la muestra.

        Las listas obtenidas se cruzan para eliminar duplicados. Luego se realizan
        peticiones en lote para obtener detalles profundos de las pistas y
        sus caracteristicas sonoras.

        Returns:
            dict: Estructura contenedora de metadatos de artistas, albumes,
                  pistas y sus caracteristicas en su forma cruda.
        """
        logger.info("Iniciando fase de extraccion desde la API de Spotify...")

        sp = _get_spotify_client()

        # Fase 1: Obtencion de nuevos lanzamientos en la plataforma
        new_releases_response = sp.new_releases(limit=20)
        albums_raw = new_releases_response.get("albums", {}).get("items", [])

        track_ids_from_releases: list[str] = []
        albums_data: list[dict] = []

        for album in albums_raw:
            album_tracks = sp.album_tracks(album["id"])
            tracks_in_album = album_tracks.get("items", [])
            for track in tracks_in_album[:3]:
                track_ids_from_releases.append(track["id"])
            albums_data.append(
                {
                    "id": album["id"],
                    "name": album["name"],
                    "release_date": album.get("release_date"),
                    "total_tracks": album.get("total_tracks", 0),
                    "artist_id": (
                        album["artists"][0]["id"] if album["artists"] else None
                    ),
                }
            )

        # Fase 2: Busqueda adicional (genero Pop) para enriquecer el historico
        search_result = sp.search(q="genre:pop", type="track", limit=20)
        search_tracks = search_result.get("tracks", {}).get("items", [])
        track_ids_from_search = [t["id"] for t in search_tracks]

        # Consolidacion de IDs omitiendo elementos repetidos
        all_track_ids = list(set(track_ids_from_releases + track_ids_from_search))
        logger.info(
            "Identificadas %d pistas unicas para extraccion profunda.",
            len(all_track_ids),
        )

        # Fase 3: Peticiones en lote para obtener la informacion completa de cada pista
        tracks_full: list[dict] = []
        artists_map: dict[str, dict] = {}

        for i in range(0, len(all_track_ids), 50):
            batch = all_track_ids[i : i + 50]
            tracks_response = sp.tracks(batch)
            for track in tracks_response.get("tracks", []):
                if track is None:
                    continue
                tracks_full.append(track)
                for artist in track.get("artists", []):
                    if artist["id"] not in artists_map:
                        artists_map[artist["id"]] = artist

        # Fase 4: Recuperacion en lote de los parametros acusticos de las pistas
        audio_features_raw: list[dict] = []
        for i in range(0, len(all_track_ids), 100):
            batch = all_track_ids[i : i + 100]
            features_batch = sp.audio_features(batch)
            audio_features_raw.extend([f for f in features_batch if f is not None])

        # Fase 5: Enriquecimiento de la entidad artista incorporando generos y nivel de popularidad
        artist_ids = list(artists_map.keys())
        artists_with_genres: dict[str, dict] = {}
        for i in range(0, len(artist_ids), 50):
            batch = artist_ids[i : i + 50]
            artists_response = sp.artists(batch)
            for artist in artists_response.get("artists", []):
                if artist:
                    artists_with_genres[artist["id"]] = {
                        "id": artist["id"],
                        "name": artist["name"],
                        "genres": artist.get("genres", []),
                        "popularity": artist.get("popularity"),
                    }

        logger.info(
            "Fase de extraccion finalizada exitosamente. Totales -> Artistas: %d, Pistas: %d",
            len(artists_with_genres),
            len(tracks_full),
        )

        return {
            "artists": list(artists_with_genres.values()),
            "albums": albums_data,
            "tracks": tracks_full,
            "audio_features": audio_features_raw,
            "source": "api",
        }

    @task
    def transform(raw_data: dict) -> dict:
        """
        Segunda tarea del pipeline (Transformacion).

        Limpia, estandariza y calcula derivaciones sobre el conjunto de datos obtenido
        en la fase previa. Se realizan tareas criticas como truncamiento de campos de
        texto, reparacion de fechas incompletas proporcionadas por la API de Spotify
        y ejecucion de la logica de negocio particular (party_index).

        Args:
            raw_data (dict): Objeto inyectado automaticamente por Airflow XCom.

        Returns:
            dict: Estructuras de datos purificadas y preparadas para persistencia relacional.
        """
        logger.info("Iniciando fase de transformacion y normalizacion de datos...")

        # Transformacion de la coleccion de artistas
        artists_clean = [
            {
                "id": a["id"],
                "name": a["name"][:255],
                "genres": a.get("genres", []),
                "popularity": a.get("popularity"),
            }
            for a in raw_data["artists"]
            if a.get("id") and a.get("name")
        ]

        # Transformacion de la coleccion de albumes y correccion de fechas truncadas
        albums_clean = []
        for album in raw_data["albums"]:
            release_date = album.get("release_date")
            if release_date and len(release_date) == 4:
                release_date = release_date + "-01-01"
            elif release_date and len(release_date) == 7:
                release_date = release_date + "-01"
            albums_clean.append(
                {
                    "id": album["id"],
                    "name": album["name"][:255],
                    "artist_id": album.get("artist_id"),
                    "release_date": release_date,
                    "total_tracks": album.get("total_tracks", 0),
                }
            )

        # Transformacion de la coleccion de pistas
        tracks_clean = []
        for track in raw_data["tracks"]:
            artist_id = track["artists"][0]["id"] if track.get("artists") else None
            album_id = track["album"]["id"] if track.get("album") else None
            tracks_clean.append(
                {
                    "id": track["id"],
                    "name": track["name"][:255],
                    "artist_id": artist_id,
                    "album_id": album_id,
                    "duration_ms": track.get("duration_ms"),
                    "explicit": track.get("explicit", False),
                    "popularity": track.get("popularity"),
                    "source": raw_data["source"],
                }
            )

        # Transformacion de las propiedades acusticas y logica de negocio
        features_clean = []
        for feat in raw_data["audio_features"]:
            if feat is None or not feat.get("id"):
                continue

            danceability = feat.get("danceability") or 0.0
            energy = feat.get("energy") or 0.0
            valence = feat.get("valence") or 0.0

            # Calculo del 'party_index' utilizando ponderaciones especificas
            party_index = round(danceability * 0.40 + energy * 0.35 + valence * 0.25, 3)

            features_clean.append(
                {
                    "track_id": feat["id"],
                    "danceability": danceability,
                    "energy": energy,
                    "valence": valence,
                    "tempo": feat.get("tempo"),
                    "loudness": feat.get("loudness"),
                    "speechiness": feat.get("speechiness"),
                    "acousticness": feat.get("acousticness"),
                    "instrumentalness": feat.get("instrumentalness"),
                    "liveness": feat.get("liveness"),
                    "party_index": party_index,
                    "key": feat.get("key"),
                    "mode": feat.get("mode"),
                    "time_signature": feat.get("time_signature"),
                }
            )

        logger.info(
            "Fase de transformacion finalizada. Cantidades -> "
            "Artistas: %d, Albumes: %d, Pistas: %d, Propiedades acusticas: %d",
            len(artists_clean),
            len(albums_clean),
            len(tracks_clean),
            len(features_clean),
        )

        return {
            "artists": artists_clean,
            "albums": albums_clean,
            "tracks": tracks_clean,
            "audio_features": features_clean,
        }

    @task
    def load_to_postgres(transformed_data: dict) -> None:
        """
        Tercera tarea del pipeline (Carga).

        Realiza la insercion fisica de los diccionarios transformados en el motor
        PostgreSQL. Emplea la clausula "ON CONFLICT DO NOTHING" / "DO UPDATE" de
        forma sistematica para garantizar la idempotencia de las transacciones, lo
        que protege al esquema contra fallos que causen duplicacion de claves.

        Args:
            transformed_data (dict): Objeto procesado inyectado por Airflow XCom.
        """
        logger.info("Iniciando fase de carga persistente en PostgreSQL...")

        conn = _get_pg_connection()
        cur = conn.cursor()

        try:
            # Volcado de entidad: Artistas
            for artist in transformed_data["artists"]:
                cur.execute(
                    """
                    INSERT INTO artists (id, name, genres, popularity)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE
                        SET popularity = EXCLUDED.popularity,
                            genres     = EXCLUDED.genres
                    """,
                    (
                        artist["id"],
                        artist["name"],
                        artist["genres"],
                        artist["popularity"],
                    ),
                )

            # Volcado de entidad: Albumes
            for album in transformed_data["albums"]:
                cur.execute(
                    """
                    INSERT INTO albums (id, name, artist_id, release_date, total_tracks)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    (
                        album["id"],
                        album["name"],
                        album["artist_id"],
                        album["release_date"],
                        album["total_tracks"],
                    ),
                )

            # Volcado de entidad: Pistas
            tracks_inserted = 0
            for track in transformed_data["tracks"]:
                cur.execute(
                    """
                    INSERT INTO tracks (
                        id, name, artist_id, album_id,
                        duration_ms, explicit, popularity, source
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    (
                        track["id"],
                        track["name"],
                        track["artist_id"],
                        track["album_id"],
                        track["duration_ms"],
                        track["explicit"],
                        track["popularity"],
                        track["source"],
                    ),
                )
                if cur.rowcount > 0:
                    tracks_inserted += 1

            # Volcado de entidad: Propiedades Acusticas
            features_inserted = 0
            for feat in transformed_data["audio_features"]:
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
                        feat["track_id"],
                        feat["danceability"],
                        feat["energy"],
                        feat["valence"],
                        feat["tempo"],
                        feat["loudness"],
                        feat["speechiness"],
                        feat["acousticness"],
                        feat["instrumentalness"],
                        feat["liveness"],
                        feat["key"],
                        feat["mode"],
                        feat["time_signature"],
                    ),
                )
                if cur.rowcount > 0:
                    features_inserted += 1

            conn.commit()
            logger.info(
                "Fase de carga finalizada con exito. Metricas de insercion -> "
                "Nuevas Pistas: %d, Nuevas Propiedades acusticas: %d",
                tracks_inserted,
                features_inserted,
            )

        except Exception as exc:
            conn.rollback()
            logger.error(
                "Se produjo una falla grave durante la escritura en base de datos. "
                "Se realiza rollback: %s",
                exc,
            )
            raise
        finally:
            cur.close()
            conn.close()

    # Definicion explicita del grafo de dependencias y flujo de datos
    raw = extract_from_api()
    transformed = transform(raw)
    load_to_postgres(transformed)


# Invocacion requerida para que el planificador de Airflow reconozca el objeto DAG
spotify_etl_dag = spotify_etl()
