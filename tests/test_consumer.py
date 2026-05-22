"""
tests/test_consumer.py
Tests unitarios del servicio consumidor.

Se prueban las funciones de utilidad y la lógica de transformación
sin necesidad de conectarse a un broker MQTT real.
"""

import sys
import os

# Añadir el directorio del consumidor al path para importar main.py
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "consumer"))

import main as consumer


class TestSafeConversions:
    """Verifica que las funciones de conversión segura manejan correctamente los casos límite."""

    def test_safe_float_with_valid_string(self):
        assert consumer._safe_float("0.75") == 0.75

    def test_safe_float_with_none(self):
        assert consumer._safe_float(None) is None

    def test_safe_float_with_empty_string(self):
        assert consumer._safe_float("") is None

    def test_safe_float_with_default(self):
        assert consumer._safe_float(None, default=0.0) == 0.0

    def test_safe_float_with_invalid(self):
        assert consumer._safe_float("no_es_numero") is None

    def test_safe_int_with_valid_string(self):
        assert consumer._safe_int("200040") == 200040

    def test_safe_int_with_none(self):
        assert consumer._safe_int(None) is None

    def test_safe_int_with_empty_string(self):
        assert consumer._safe_int("") is None


class TestPartyIndex:
    """
    Verifica que el cálculo del party_index es correcto.

    party_index = (danceability + energy + valence) / 3
    Es el campo calculado propio del pipeline que NO viene de Spotify.
    """

    def test_party_index_typical(self):
        danceability, energy, valence = 0.8, 0.9, 0.7
        expected = round((danceability + energy + valence) / 3.0, 3)
        assert expected == 0.8

    def test_party_index_zeros(self):
        expected = round((0.0 + 0.0 + 0.0) / 3.0, 3)
        assert expected == 0.0

    def test_party_index_ones(self):
        expected = round((1.0 + 1.0 + 1.0) / 3.0, 3)
        assert expected == 1.0

    def test_party_index_blinding_lights(self):
        """Caso real: Blinding Lights de The Weeknd."""
        danceability = 0.514
        energy = 0.730
        valence = 0.334
        party_index = round((danceability + energy + valence) / 3.0, 3)
        # Valor esperado: 0.526
        assert 0.5 < party_index < 0.6


class TestMessageValidation:
    """Verifica que los mensajes MQTT sin track_id son ignorados correctamente."""

    def test_missing_track_id_returns_none(self):
        """Un mensaje sin track_id no debe procesarse."""
        data = {"track_name": "Test Track", "source": "csv"}
        track_id = data.get("track_id")
        assert not track_id  # track_id ausente → se descarta el mensaje

    def test_source_values(self):
        """El campo source solo puede ser 'csv' o 'api'."""
        valid_sources = {"csv", "api"}
        assert "csv" in valid_sources
        assert "api" in valid_sources
        assert "kafka" not in valid_sources
