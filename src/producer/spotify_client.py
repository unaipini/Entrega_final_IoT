"""
src/producer/spotify_client.py

Cliente de la API de Spotify para el servicio productor.
Responsabilidad principal: Autenticarse contra la API de Spotify y extraer pistas de nuevos
lanzamientos para simular un flujo continuo de datos en tiempo real.
"""

import logging
import os
from typing import Generator

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

logger = logging.getLogger(__name__)


def _build_client() -> spotipy.Spotify:
    """
    Crea un cliente de Spotify autenticado utilizando el flujo de credenciales de cliente
    (Client Credentials Flow).

    Esta funcion lee las credenciales almacenadas en las variables de entorno
    SPOTIFY_CLIENT_ID y SPOTIFY_CLIENT_SECRET. No requiere interaccion del usuario
    y es ideal para la comunicacion servidor a servidor.

    Returns:
        spotipy.Spotify: Una instancia autenticada del cliente de la API de Spotify.
    """
    credentials = SpotifyClientCredentials(
        client_id=os.environ["SPOTIFY_CLIENT_ID"],
        client_secret=os.environ["SPOTIFY_CLIENT_SECRET"],
    )
    return spotipy.Spotify(auth_manager=credentials)


def stream_new_releases(limit: int = 20) -> Generator[dict, None, None]:
    """
    Genera pistas de nuevos lanzamientos extraidas desde la API de Spotify.

    El flujo de ejecucion es el siguiente:
    1. Realiza una peticion al endpoint /browse/new-releases para obtener los ultimos albumes.
    2. Itera sobre cada album y obtiene hasta 3 pistas utilizando el endpoint /albums/{id}/tracks.
    3. Construye una lista preliminar con metadatos de las pistas.
    4. Agrupa los IDs de las pistas y realiza peticiones en lote (batch) al endpoint
       /audio-features para recuperar caracteristicas musicales de forma eficiente.
    5. Combina los metadatos de las pistas con sus respectivas caracteristicas de audio.

    Args:
        limit (int): Numero maximo de nuevos albumes a recuperar en la peticion inicial.

    Yields:
        dict: Diccionario que contiene todos los campos de la pista y sus caracteristicas de audio,
              preparado para ser serializado a formato JSON y publicado en MQTT.
    """
    sp = _build_client()

    try:
        # Obtencion de nuevos albumes
        response = sp.new_releases(limit=limit)
        albums = response.get("albums", {}).get("items", [])
        logger.info("Obtenidos %d albumes nuevos de Spotify.", len(albums))

        # Fase de extraccion de pistas por cada album obtenido
        tracks_buffer = []
        for album in albums:
            album_tracks = sp.album_tracks(album["id"])
            for track in album_tracks.get("items", [])[:3]:
                artist = album["artists"][0] if album.get("artists") else {}
                tracks_buffer.append({
                    "track_id":   track["id"],
                    "track_name": track["name"],
                    "artist_id":  artist.get("id"),
                    "artist_name": artist.get("name"),
                    "album_id":   album["id"],
                    "album_name": album["name"],
                    "duration_ms": track.get("duration_ms"),
                    "explicit":   track.get("explicit", False),
                    "source":     "api",
                })

        # Fase de enriquecimiento con caracteristicas de audio en lotes
        # Se agrupan los IDs en fragmentos de maximo 100 elementos por restriccion de la API
        track_ids = [t["track_id"] for t in tracks_buffer if t["track_id"]]
        features_map: dict = {}
        for i in range(0, len(track_ids), 100):
            batch = track_ids[i:i + 100]
            results = sp.audio_features(batch) or []
            for feat in results:
                if feat:
                    features_map[feat["id"]] = feat

        # Fase de ensamblaje final de los diccionarios a retornar
        for track in tracks_buffer:
            feat = features_map.get(track["track_id"], {})
            yield {
                **track,
                "danceability":     feat.get("danceability"),
                "energy":           feat.get("energy"),
                "valence":          feat.get("valence"),
                "tempo":            feat.get("tempo"),
                "loudness":         feat.get("loudness"),
                "speechiness":      feat.get("speechiness"),
                "acousticness":     feat.get("acousticness"),
                "instrumentalness": feat.get("instrumentalness"),
                "liveness":         feat.get("liveness"),
                "key":              feat.get("key"),
                "mode":             feat.get("mode"),
                "time_signature":   feat.get("time_signature"),
            }

    except spotipy.exceptions.SpotifyException as exc:
        logger.error("Error en la comunicacion con la API de Spotify: %s", exc)
        raise
