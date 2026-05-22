"""
airflow/dags/spotify_etl.py
DAG de Airflow que implementa el pipeline ETL de Spotify.

Flujo:  extract_from_api  →  transform  →  load_to_postgres

- extract_from_api: llama a los endpoints de Spotify y guarda los datos en XCom.
- transform: calcula party_index y normaliza los datos (no los persiste aún).
- load_to_postgres: inserta artistas, álbumes, pistas y audio_features en PostgreSQL.

El DAG se ejecuta cada hora. En cada ejecución obtiene nuevos lanzamientos de
Spotify (hasta 20 canciones) para mantener el flujo de datos en tiempo real
durante la demo.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta

import psycopg2
import spotipy
from airflow.decorators import dag, task
from airflow.models import Variable
from spotipy.oauth2 import SpotifyClientCredentials

# ---------------------------------------------------------------------------
# Configuración del logger
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Argumentos por defecto del DAG
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "iot_team",
    "depends_on_past": False,
    # En producción se activarían alertas por email aquí
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ---------------------------------------------------------------------------
# Parámetros leídos del entorno (inyectados por docker-compose)
# ---------------------------------------------------------------------------
SPOTIFY_CLIENT_ID = os.environ.get("SPOTIFY_CLIENT_ID", "")
SPOTIFY_CLIENT_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET", "")

PG_HOST = os.environ.get("POSTGRES_HOST", "postgres")
PG_PORT = int(os.environ.get("POSTGRES_PORT", 5432))
PG_DB = os.environ.get("POSTGRES_DB", "spotify_db")
PG_USER = os.environ.get("POSTGRES_USER", "spotify_user")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "")


def _get_spotify_client() -> spotipy.Spotify:
    """Crea y devuelve un cliente autenticado de la API de Spotify."""
    credentials = SpotifyClientCredentials(
        client_id=SPOTIFY_CLIENT_ID,
        client_secret=SPOTIFY_CLIENT_SECRET,
    )
    return spotipy.Spotify(auth_manager=credentials)


def _get_pg_connection():
    """Devuelve una conexión activa a PostgreSQL."""
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


# ---------------------------------------------------------------------------
# Definición del DAG con el decorador @dag (API funcional de Airflow 2.x)
# ---------------------------------------------------------------------------
@dag(
    dag_id="spotify_etl",
    description="Pipeline ETL: extrae canciones de Spotify, calcula party_index y carga en PostgreSQL.",
    default_args=DEFAULT_ARGS,
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["spotify", "iot", "etl"],
)
def spotify_etl():
    """
    DAG principal del pipeline ETL de Spotify.

    Cada tarea es una función Python decorada con @task.
    Los datos entre tareas se pasan mediante XCom (almacenamiento temporal de Airflow).
    """

    # -----------------------------------------------------------------------
    # TAREA 1: Extract — obtiene datos desde la API de Spotify
    # -----------------------------------------------------------------------
    @task
    def extract_from_api() -> dict:
        """
        Llama a dos endpoints de la API de Spotify:
        1. /browse/new-releases: nuevos lanzamientos (frecuencia: cada hora).
        2. /search: canciones de género pop para enriquecer el histórico.

        Devuelve un dict con las canciones y sus audio_features en formato raw.
        """
        logger.info("Iniciando extracción desde la API de Spotify...")

        sp = _get_spotify_client()

        # --- Endpoint 1: nuevos lanzamientos ---
        new_releases_response = sp.new_releases(limit=20)
        albums_raw = new_releases_response.get("albums", {}).get("items", [])

        # Obtenemos las pistas de cada álbum nuevo
        track_ids_from_releases: list[str] = []
        albums_data: list[dict] = []

        for album in albums_raw:
            album_tracks = sp.album_tracks(album["id"])
            tracks_in_album = album_tracks.get("items", [])
            for track in tracks_in_album[:3]:  # Máximo 3 pistas por álbum
                track_ids_from_releases.append(track["id"])
            albums_data.append(
                {
                    "id": album["id"],
                    "name": album["name"],
                    "release_date": album.get("release_date"),
                    "total_tracks": album.get("total_tracks", 0),
                    "artist_id": album["artists"][0]["id"] if album["artists"] else None,
                }
            )

        # --- Endpoint 2: búsqueda por género pop (enriquecimiento del histórico) ---
        search_result = sp.search(q="genre:pop", type="track", limit=20)
        search_tracks = search_result.get("tracks", {}).get("items", [])
        track_ids_from_search = [t["id"] for t in search_tracks]

        # Unificamos todos los IDs de pistas sin duplicados
        all_track_ids = list(set(track_ids_from_releases + track_ids_from_search))
        logger.info("Total de pistas extraídas: %d", len(all_track_ids))

        # --- Obtener detalles completos de las pistas ---
        tracks_full: list[dict] = []
        artists_map: dict[str, dict] = {}

        # La API de Spotify solo acepta hasta 50 IDs por llamada
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

        # --- Obtener audio_features de todas las pistas ---
        audio_features_raw: list[dict] = []
        for i in range(0, len(all_track_ids), 100):
            batch = all_track_ids[i : i + 100]
            features_batch = sp.audio_features(batch)
            audio_features_raw.extend([f for f in features_batch if f is not None])

        # --- Enriquecer artistas con géneros ---
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

        logger.info("Extracción completada. Artistas: %d, Pistas: %d", len(artists_with_genres), len(tracks_full))

        return {
            "artists": list(artists_with_genres.values()),
            "albums": albums_data,
            "tracks": tracks_full,
            "audio_features": audio_features_raw,
            "source": "api",
        }

    # -----------------------------------------------------------------------
    # TAREA 2: Transform — normaliza y calcula campos derivados
    # -----------------------------------------------------------------------
    @task
    def transform(raw_data: dict) -> dict:
        """
        Transforma los datos extraídos de la API:
        - Normaliza los campos de cada pista al esquema de la BD.
        - Calcula party_index = (danceability + energy + valence) / 3.
          Este campo NO viene de Spotify; lo genera nuestro pipeline.
        - Filtra valores nulos o fuera de rango.

        Devuelve un dict con listas limpias listas para insertar en PostgreSQL.
        """
        logger.info("Iniciando transformación de datos...")

        # --- Normalizar artistas ---
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

        # --- Normalizar álbumes ---
        albums_clean = []
        for album in raw_data["albums"]:
            release_date = album.get("release_date")
            # Spotify devuelve fechas en formatos: "2024-01-15", "2024-01" o "2024"
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

        # --- Normalizar pistas ---
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

        # --- Normalizar audio_features y calcular party_index ---
        features_clean = []
        for feat in raw_data["audio_features"]:
            if feat is None or not feat.get("id"):
                continue

            danceability = feat.get("danceability") or 0.0
            energy = feat.get("energy") or 0.0
            valence = feat.get("valence") or 0.0

            # party_index: campo calculado propio del pipeline.
            # En PostgreSQL también está definido como GENERATED ALWAYS AS
            # (redundancia intencionada para que sea visible en ambas capas).
            party_index = round((danceability + energy + valence) / 3.0, 3)

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
            "Transformación completada. Artistas: %d, Álbumes: %d, Pistas: %d, Features: %d",
            len(artists_clean), len(albums_clean), len(tracks_clean), len(features_clean),
        )

        return {
            "artists": artists_clean,
            "albums": albums_clean,
            "tracks": tracks_clean,
            "audio_features": features_clean,
        }

    # -----------------------------------------------------------------------
    # TAREA 3: Load — inserta los datos en PostgreSQL
    # -----------------------------------------------------------------------
    @task
    def load_to_postgres(transformed_data: dict) -> None:
        """
        Inserta todos los registros en PostgreSQL.

        Usa INSERT ... ON CONFLICT DO NOTHING para ser idempotente:
        si el DAG se re-ejecuta, no falla por duplicados.
        """
        logger.info("Iniciando carga en PostgreSQL...")

        conn = _get_pg_connection()
        cur = conn.cursor()

        try:
            # --- Insertar artistas ---
            for artist in transformed_data["artists"]:
                cur.execute(
                    """
                    INSERT INTO artists (id, name, genres, popularity)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE
                        SET popularity = EXCLUDED.popularity,
                            genres     = EXCLUDED.genres
                    """,
                    (artist["id"], artist["name"], artist["genres"], artist["popularity"]),
                )

            # --- Insertar álbumes ---
            for album in transformed_data["albums"]:
                cur.execute(
                    """
                    INSERT INTO albums (id, name, artist_id, release_date, total_tracks)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    (
                        album["id"], album["name"], album["artist_id"],
                        album["release_date"], album["total_tracks"],
                    ),
                )

            # --- Insertar pistas ---
            tracks_inserted = 0
            for track in transformed_data["tracks"]:
                cur.execute(
                    """
                    INSERT INTO tracks (id, name, artist_id, album_id, duration_ms, explicit, popularity, source)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    (
                        track["id"], track["name"], track["artist_id"], track["album_id"],
                        track["duration_ms"], track["explicit"], track["popularity"], track["source"],
                    ),
                )
                if cur.rowcount > 0:
                    tracks_inserted += 1

            # --- Insertar audio_features ---
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
                        feat["track_id"], feat["danceability"], feat["energy"], feat["valence"],
                        feat["tempo"], feat["loudness"], feat["speechiness"], feat["acousticness"],
                        feat["instrumentalness"], feat["liveness"], feat["key"], feat["mode"],
                        feat["time_signature"],
                    ),
                )
                if cur.rowcount > 0:
                    features_inserted += 1

            conn.commit()
            logger.info(
                "Carga completada. Pistas nuevas: %d, Features nuevas: %d",
                tracks_inserted, features_inserted,
            )

        except Exception as exc:
            conn.rollback()
            logger.error("Error durante la carga en PostgreSQL: %s", exc)
            raise
        finally:
            cur.close()
            conn.close()

    # -----------------------------------------------------------------------
    # Encadenado de tareas: define el orden de ejecución del DAG
    # extract → transform → load
    # -----------------------------------------------------------------------
    raw = extract_from_api()
    transformed = transform(raw)
    load_to_postgres(transformed)


# Instancia el DAG para que Airflow lo detecte al escanear la carpeta dags/
spotify_etl_dag = spotify_etl()
