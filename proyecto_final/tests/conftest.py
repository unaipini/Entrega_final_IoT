# tests/conftest.py
# Configuración compartida de pytest.
# Define fixtures reutilizables en todos los módulos de test.

import pytest


@pytest.fixture
def sample_track_payload():
    """
    Payload MQTT de ejemplo representando una pista del CSV.
    Usado en tests del consumidor para simular un mensaje recibido.
    """
    return {
        "track_id": "4iV5W9uYEdYUVa79Axb7Rh",
        "track_name": "Blinding Lights",
        "artist_id": "1Xyo4u8uXC1ZmMpatF05PJ",
        "artist_name": "The Weeknd",
        "album_id": "4yP0hdKOZPNshxUOjY0cZj",
        "album_name": "After Hours",
        "duration_ms": 200040,
        "explicit": False,
        "popularity": 87,
        "danceability": 0.514,
        "energy": 0.730,
        "valence": 0.334,
        "tempo": 171.005,
        "loudness": -5.934,
        "speechiness": 0.0598,
        "acousticness": 0.00146,
        "instrumentalness": 0.0,
        "liveness": 0.0897,
        "source": "csv",
    }


@pytest.fixture
def sample_api_payload():
    """
    Payload MQTT de ejemplo representando una pista obtenida de la API de Spotify.
    El campo source='api' es lo que lo diferencia del CSV.
    """
    return {
        "track_id": "4LRPiXqCikLlN15c3yImP7",
        "track_name": "As It Was",
        "artist_id": "6KImCVD70vtIoJWnq6nqn1",
        "artist_name": "Harry Styles",
        "album_id": "5r36AJ6VOJtp00oxSkBZ5h",
        "album_name": "Harry's House",
        "duration_ms": 167303,
        "explicit": False,
        "popularity": 90,
        "source": "api",
    }
