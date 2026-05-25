"""
tests/test_producer.py
Tests unitarios del servicio productor.
"""

import sys
import os
import csv
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "producer"))


class TestCsvParsing:
    """Verifica que el CSV de pistas se parsea correctamente."""

    def test_csv_has_required_columns(self):
        """El CSV de muestra debe tener los campos mÃ­nimos requeridos."""
        csv_path = os.path.join(
            os.path.dirname(__file__), "..", "datos", "dataset.csv"
        )
        required_columns = {
            "id", "name", "artists", "duration_ms",
            "danceability", "energy", "valence",
        }
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            assert required_columns.issubset(set(reader.fieldnames or []))

    def test_csv_has_rows(self):
        """El CSV debe tener al menos una fila de datos."""
        csv_path = os.path.join(
            os.path.dirname(__file__), "..", "datos", "dataset.csv"
        )
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        assert len(rows) > 0

    def test_csv_numeric_fields(self):
        """Los campos numÃ©ricos deben ser convertibles a float."""
        csv_path = os.path.join(
            os.path.dirname(__file__), "..", "datos", "dataset.csv"
        )
        numeric_fields = ["danceability", "energy", "valence", "tempo", "loudness"]
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                for field in numeric_fields:
                    value = row.get(field)
                    if value:
                        parsed = float(value)
                        assert isinstance(parsed, float)

    def test_danceability_range(self):
        """danceability debe estar entre 0.0 y 1.0."""
        csv_path = os.path.join(
            os.path.dirname(__file__), "..", "datos", "dataset.csv"
        )
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                value = row.get("danceability")
                if value:
                    assert 0.0 <= float(value) <= 1.0


class TestPayloadBuilding:
    """Verifica que el payload MQTT se construye correctamente desde una fila del CSV."""

    def _build_payload(self, row: dict) -> dict:
        """Replica la lÃ³gica de construcciÃ³n de payload de main.py."""
        return {
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
            "source": "csv",
        }

    def test_payload_source_is_csv(self):
        row = {"id": "abc", "name": "Test", "duration_ms": "180000",
               "explicit": "False", "popularity": "75",
               "danceability": "0.7", "energy": "0.8", "valence": "0.6"}
        payload = self._build_payload(row)
        assert payload["source"] == "csv"

    def test_payload_explicit_false(self):
        row = {"id": "abc", "name": "Test", "duration_ms": "180000",
               "explicit": "False", "popularity": "75",
               "danceability": "0.7", "energy": "0.8", "valence": "0.6"}
        payload = self._build_payload(row)
        assert payload["explicit"] is False

    def test_payload_explicit_true(self):
        row = {"id": "abc", "name": "Test", "duration_ms": "180000",
               "explicit": "True", "popularity": "75",
               "danceability": "0.7", "energy": "0.8", "valence": "0.6"}
        payload = self._build_payload(row)
        assert payload["explicit"] is True

    def test_payload_duration_is_int(self):
        row = {"id": "abc", "name": "Test", "duration_ms": "200040",
               "explicit": "False", "popularity": "87",
               "danceability": "0.514", "energy": "0.73", "valence": "0.334"}
        payload = self._build_payload(row)
        assert payload["duration_ms"] == 200040
        assert isinstance(payload["duration_ms"], int)
