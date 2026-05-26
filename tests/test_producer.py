"""
tests/test_producer.py

Suite de pruebas unitarias enfocada en garantizar la calidad y fiabilidad
del servicio productor (MQTT Publisher).

Este modulo evalua dos aspectos criticos del funcionamiento:
1. Validacion estructural e integridad del dataset origen (CSV).
2. Correcta inicializacion y formateo del payload JSON previo a su inyeccion
   en el broker MQTT, asegurando consistencia con el esquema esperado por
   los modulos consumidores downstream.
"""

import csv
import os
import sys

# Insercion del directorio raiz de fuentes (src) en el PATH de ejecucion para
# permitir la resolucion de modulos internos independientemente de donde se
# invoque pytest.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "producer"))


class TestCsvParsing:
    """
    Agrupacion de casos de prueba dedicados a la verificacion de la estructura
    fisica y logica del archivo de datos origen (dataset historico).
    """

    def test_csv_has_required_columns(self):
        """
        Verifica que el archivo CSV origen declare explícitamente en su cabecera
        los atributos que conforman el dominio principal del sistema. Si falta alguna
        columna clave, la prueba fallara indicando una anomalia en el origen.
        """
        csv_path = os.path.join(os.path.dirname(__file__), "..", "datos", "dataset.csv")
        required_columns = {
            "track_id",
            "track_name",
            "artists",
            "duration_ms",
            "danceability",
            "energy",
            "valence",
            "track_genre",
        }
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            assert required_columns.issubset(set(reader.fieldnames or []))

    def test_csv_has_rows(self):
        """
        Garantiza que el archivo origen no se encuentre vacio. Esta asercion es basica
        para evitar arrancar un proceso de migracion masiva que carezca de datos.
        """
        csv_path = os.path.join(os.path.dirname(__file__), "..", "datos", "dataset.csv")
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        assert len(rows) > 0

    def test_csv_numeric_fields(self):
        """
        Valida que todos los campos acusticos vitales para el calculo del
        'party_index' en etapas posteriores sean representables como numeros
        de coma flotante validos (float).
        """
        csv_path = os.path.join(os.path.dirname(__file__), "..", "datos", "dataset.csv")
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
        """
        Comprueba que los datos acusticos respeten las restricciones de dominio
        definidas por la API de Spotify (ej. danceability siempre deberia oscilar
        entre 0.0 y 1.0).
        """
        csv_path = os.path.join(os.path.dirname(__file__), "..", "datos", "dataset.csv")
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                value = row.get("danceability")
                if value:
                    assert 0.0 <= float(value) <= 1.0


class TestPayloadBuilding:
    """
    Agrupacion de casos de prueba enfocados en validar el proceso de ensamblaje
    del diccionario resultante (payload) que se publicara hacia el cluster MQTT.
    """

    def _build_payload(self, row: dict) -> dict:
        """
        Metodo auxiliar que simula localmente la logica de parseo y extraccion
        que efectua main.py sobre una fila en crudo.

        Args:
            row (dict): Registro en crudo (tipo string-string) extraido del dataset.

        Returns:
            dict: Objeto tipado y enriquecido preparado para ser inyectado.
        """
        return {
            "track_id": row.get("track_id", ""),
            "track_name": row.get("track_name", ""),
            "artist_id": row.get("artists", ""),
            "artist_name": row.get("artists", ""),
            "album_id": row.get("album_id", ""),
            "album_name": row.get("album_name", ""),
            "track_genre": row.get("track_genre", "Unknown"),
            "duration_ms": int(row["duration_ms"]) if row.get("duration_ms") else None,
            "explicit": row.get("explicit", "False").lower() == "true",
            "popularity": int(row["popularity"]) if row.get("popularity") else None,
            "danceability": (
                float(row["danceability"]) if row.get("danceability") else None
            ),
            "energy": float(row["energy"]) if row.get("energy") else None,
            "valence": float(row["valence"]) if row.get("valence") else None,
            "source": "csv",
        }

    def test_payload_source_is_csv(self):
        """
        Comprueba que toda inyeccion en lote proveniente de este proceso asigne
        automaticamente la metadata 'csv' para permitir la trazabilidad correcta
        en la capa analitica final.
        """
        row = {
            "track_id": "abc",
            "track_name": "Test",
            "duration_ms": "180000",
            "explicit": "False",
            "popularity": "75",
            "danceability": "0.7",
            "energy": "0.8",
            "valence": "0.6",
        }
        payload = self._build_payload(row)
        assert payload["source"] == "csv"

    def test_payload_explicit_false(self):
        """
        Asegura que las representaciones en cadena como 'False' (indiferente
        a la capitalizacion) sean convertidas al tipo booleano False de Python.
        """
        row = {
            "track_id": "abc",
            "track_name": "Test",
            "duration_ms": "180000",
            "explicit": "False",
            "popularity": "75",
            "danceability": "0.7",
            "energy": "0.8",
            "valence": "0.6",
        }
        payload = self._build_payload(row)
        assert payload["explicit"] is False

    def test_payload_explicit_true(self):
        """
        Asegura que las representaciones en cadena como 'True' (indiferente
        a la capitalizacion) sean convertidas al tipo booleano True de Python.
        """
        row = {
            "track_id": "abc",
            "track_name": "Test",
            "duration_ms": "180000",
            "explicit": "True",
            "popularity": "75",
            "danceability": "0.7",
            "energy": "0.8",
            "valence": "0.6",
        }
        payload = self._build_payload(row)
        assert payload["explicit"] is True

    def test_payload_duration_is_int(self):
        """
        Comprueba que los campos de tiempo como duracion en milisegundos se
        casteen rigurosamente a tipo de dato entero en lugar de enviarlos como
        cadenas al topico MQTT.
        """
        row = {
            "track_id": "abc",
            "track_name": "Test",
            "duration_ms": "200040",
            "explicit": "False",
            "popularity": "87",
            "danceability": "0.514",
            "energy": "0.73",
            "valence": "0.334",
        }
        payload = self._build_payload(row)
        assert payload["duration_ms"] == 200040
        assert isinstance(payload["duration_ms"], int)
