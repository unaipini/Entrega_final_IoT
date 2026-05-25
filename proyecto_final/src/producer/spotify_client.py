"""
src/producer/spotify_client.py
Cliente de la API de Spotify para el servicio productor.
Responsabilidad única: autenticarse y generar pistas de nuevos lanzamientos.
"""

import logging
import os
from typing import Generator

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

logger = logging.getLogger(__name__)


def _build_client() -> spotipy.Spotify:
    """Crea un cliente Spotify autenticado con Client Credentials Flow."""
    credentials = SpotifyClientCredentials(
        client_id=os.environ["SPOTIFY_CLIENT_ID"],
        client_secret=os.environ["SPOTIFY_CLIENT_SECRET"],
    )
    return spotipy.Spotify(auth_manager=credentials)


def stream_new_releases(limit: int = 20) -> Generator[dict, None, None]:
    """
    Genera pistas de nuevos lanzamientos de Spotify.

    Llama a /browse/new-releases, luego recupera hasta 3 pistas de cada
    álbum para simular llegada continua de datos durante la demo.

    Yields
    ------
    dict
        Campos de la pista listos para serializar a JSON y publicar en MQTT.
    """
    sp = _build_client()

    try:
        response = sp.new_releases(limit=limit)
        albums = response.get("albums", {}).get("items", [])
        logger.info("Obtenidos %d álbumes nuevos de Spotify.", len(albums))

        for album in albums:
            album_tracks = sp.album_tracks(album["id"])
            for track in album_tracks.get("items", [])[:3]:
                artist = album["artists"][0] if album.get("artists") else {}
                yield {
                    "track_id": track["id"],
                    "track_name": track["name"],
                    "artist_id": artist.get("id"),
                    "artist_name": artist.get("name"),
                    "album_id": album["id"],
                    "album_name": album["name"],
                    "duration_ms": track.get("duration_ms"),
                    "explicit": track.get("explicit", False),
                    "source": "api",
                }
    except spotipy.exceptions.SpotifyException as exc:
        logger.error("Error en la API de Spotify: %s", exc)
        raise
