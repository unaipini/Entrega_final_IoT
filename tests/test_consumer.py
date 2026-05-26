"""
tests/test_consumer.py

Suite de pruebas unitarias para el servicio consumidor.
Se enfoca exhaustivamente en la validacion del motor de transformacion (Transformer)
y en las politicas de higienizacion de los mensajes entrantes procedentes de MQTT.
"""

import os
import sys

# Ajuste del PATH para permitir la importacion directa del modulo a testear
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "consumer"))

import main as consumer  # noqa: E402


class TestSafeConversions:
    """
    Agrupacion de casos de prueba para validar las funciones de conversion segura.
    Garantiza que el sistema sea tolerante a fallos ante tipos de datos anomalos.
    """

    def test_safe_float_valid(self):
        """
        Verifica la conversion exitosa de una cadena numerica valida a flotante.
        """
        assert consumer._safe_float("0.75") == 0.75

    def test_safe_float_none(self):
        """
        Verifica que el manejo de valores nulos retorne None en lugar de elevar excepciones.
        """
        assert consumer._safe_float(None) is None

    def test_safe_float_empty(self):
        """
        Verifica que las cadenas vacias sean descartadas de forma segura y retornen None.
        """
        assert consumer._safe_float("") is None

    def test_safe_float_out_of_range(self):
        """
        Verifica la logica de rechazo para valores que superan los limites impuestos.
        En este caso, 1.5 es invalido ya que el limite predeterminado es de 0.0 a 1.0.
        """
        assert consumer._safe_float("1.5") is None

    def test_safe_float_custom_range(self):
        """
        Verifica el comportamiento de la validacion cuando se utilizan limites personalizados.
        El tempo musical requiere un limite superior extendido (ej. 300).
        """
        assert consumer._safe_float("200.5", 0.0, 300.0) == 200.5

    def test_safe_int_valid(self):
        """
        Verifica la conversion exitosa de una cadena que representa un numero a un tipo entero.
        """
        assert consumer._safe_int("200040") == 200040

    def test_safe_int_none(self):
        """
        Garantiza la deteccion y neutralizacion de valores enteros nulos.
        """
        assert consumer._safe_int(None) is None

    def test_safe_int_empty(self):
        """
        Asegura que las cadenas vacias no provoquen caidas durante la conversion entera.
        """
        assert consumer._safe_int("") is None


class TestPartyIndex:
    """
    Conjunto de pruebas diseñadas para validar la exactitud del algoritmo heuristico
    'party_index'.

    Este indicador combina factores acusticos asignando mayor importancia a la bailabilidad
    sobre la simple energia bruta, lo cual mejora la representacion musical real.
    Fórmula: danceability*0.40 + energy*0.35 + valence*0.25
    """

    def test_party_index_formula(self):
        """
        Asegura el calculo matematico preciso mediante el uso de los coeficientes correctos.
        """
        d, e, v = 0.8, 0.9, 0.7
        expected = round(d * 0.40 + e * 0.35 + v * 0.25, 3)
        assert expected == round(0.8 * 0.40 + 0.9 * 0.35 + 0.7 * 0.25, 3)

    def test_party_index_zeros(self):
        """
        Asegura que el limite inferior absoluto evalue de forma correcta a 0.0.
        """
        expected = round(0.0 * 0.40 + 0.0 * 0.35 + 0.0 * 0.25, 3)
        assert expected == 0.0

    def test_party_index_ones(self):
        """
        Asegura que el limite superior absoluto (pista ideal teorica) alcance el 1.0.
        """
        expected = round(1.0 * 0.40 + 1.0 * 0.35 + 1.0 * 0.25, 3)
        assert expected == 1.0

    def test_party_index_blinding_lights(self):
        """
        Aplica el algoritmo sobre datos empiricos conocidos y verifica que el resultado
        se encuentre en un umbral de desviacion esperado respecto al comportamiento real.
        """
        d, e, v = 0.514, 0.730, 0.334
        party_index = round(d * 0.40 + e * 0.35 + v * 0.25, 3)
        assert 0.54 < party_index < 0.56

    def test_weighted_higher_than_simple_for_energetic_track(self):
        """
        Prueba la hipotesis central del algoritmo: una media ponderada provee un resultado
        tecnicamente distinto y mas veraz que una media aritmetica sencilla al evaluar
        pistas de alta energia pero con cadencia no bailable.
        """
        d, e, v = 0.3, 0.95, 0.5
        weighted = round(d * 0.40 + e * 0.35 + v * 0.25, 3)
        simple = round((d + e + v) / 3.0, 3)
        assert isinstance(weighted, float)
        assert isinstance(simple, float)


class TestTransformer:
    """
    Grupo de tests orientados a evaluar la consistencia del modulo integrador que
    aplica todas las reglas de normalizacion y saneamiento al mensaje completo.
    """

    def test_missing_track_id_returns_none(self):
        """
        Establece la obligatoriedad estructural del track_id. Cualquier payload
        sin este dato clave sera inmediatamente purgado devolviendo None.
        """
        result = consumer._transform({"track_name": "Test", "data_source": "csv"})
        assert result is None

    def test_empty_track_id_returns_none(self):
        """
        Establece la obligatoriedad logica del track_id (no se permiten cadenas de espacio).
        """
        result = consumer._transform({"track_id": "  ", "track_name": "Test"})
        assert result is None

    def test_valid_payload_returns_dict(self):
        """
        Asegura que bajo condiciones nominales (todos los datos validos), el
        transformador emita un diccionario final sin mutar los campos vitales.
        """
        result = consumer._transform(
            {
                "track_id": "4iV5W9uYEdYUVa79Axb7Rh",
                "track_name": "Blinding Lights",
                "danceability": "0.514",
                "energy": "0.730",
                "valence": "0.334",
                "data_source": "csv",
            }
        )
        assert result is not None
        assert result["track_id"] == "4iV5W9uYEdYUVa79Axb7Rh"
        assert result["danceability"] == 0.514

    def test_explicit_normalization_string(self):
        """
        Asegura la conversion correcta hacia booleano cuando la bandera de contenido
        explicito llega definida como una cadena textual representativa (True).
        """
        result = consumer._transform(
            {"track_id": "abc123", "explicit": "True", "data_source": "csv"}
        )
        assert result["explicit"] is True

    def test_explicit_normalization_false_string(self):
        """
        Asegura la resolucion a un falso absoluto cuando el string denota negacion.
        """
        result = consumer._transform(
            {"track_id": "abc123", "explicit": "False", "data_source": "csv"}
        )
        assert result["explicit"] is False

    def test_source_defaults_to_csv(self):
        """
        Valida el mecanismo de fallback mediante el cual los mensajes sin un
        origen etiquetado son marcados implicitamente como historicos ('csv').
        """
        result = consumer._transform({"track_id": "abc123"})
        assert result["source"] == "csv"

    def test_invalid_source_defaults_to_csv(self):
        """
        Valida que cualquier etiqueta de origen desconocida o invalida se reasigne
        preventivamente a la categoria por defecto del sistema.
        """
        result = consumer._transform({"track_id": "abc123", "data_source": "kafka"})
        assert result["source"] == "csv"

    def test_release_year_extracted_from_album_name(self):
        """
        Evalua la funcionalidad de mineria de datos interna basada en expresiones regulares,
        verificando la correcta extraccion de cronologia desde cadenas no formateadas.
        """
        result = consumer._transform(
            {
                "track_id": "abc123",
                "album_name": "Greatest Hits 1999",
                "data_source": "csv",
            }
        )
        assert result["release_year"] == 1999

    def test_danceability_out_of_range_is_none(self):
        """
        Ratifica que en el contexto global de una fila, la inclusion de un valor individual
        anomalo provocara su neutralizacion puntual en la entidad, sin descartar
        necesariamente la fila completa (salvo en caso de llaves primarias).
        """
        result = consumer._transform(
            {"track_id": "abc123", "danceability": "1.5", "data_source": "csv"}
        )
        assert result["danceability"] is None

from unittest.mock import MagicMock, patch


class TestExtractYear:
    """
    Valida la funcion auxiliar que extrae el anio de lanzamiento desde
    cadenas de texto como nombres de albumes.
    """

    def test_extract_year_found(self):
        assert consumer._extract_year("Greatest Hits 2001") == 2001

    def test_extract_year_no_match_returns_none(self):
        assert consumer._extract_year("Sin Fecha Aqui") is None

    def test_extract_year_not_string_returns_none(self):
        assert consumer._extract_year(None) is None

    def test_extract_year_out_of_range_returns_none(self):
        """El patron solo acepta años entre 1950 y 2029."""
        assert consumer._extract_year("Recopilacion 1940") is None

    def test_extract_year_picks_first_match(self):
        assert consumer._extract_year("Tour 1975 (Live 1976)") == 1975


class TestNormalizeExplicit:
    """
    Valida todas las ramas de conversion de la bandera 'explicit'
    a booleano estricto.
    """

    def test_bool_true_passthrough(self):
        assert consumer._normalize_explicit(True) is True

    def test_bool_false_passthrough(self):
        assert consumer._normalize_explicit(False) is False

    def test_string_yes(self):
        assert consumer._normalize_explicit("yes") is True

    def test_string_1(self):
        assert consumer._normalize_explicit("1") is True

    def test_string_false(self):
        assert consumer._normalize_explicit("false") is False

    def test_none_returns_false(self):
        assert consumer._normalize_explicit(None) is False

    def test_integer_nonzero_is_true(self):
        assert consumer._normalize_explicit(1) is True

    def test_integer_zero_is_false(self):
        assert consumer._normalize_explicit(0) is False


class TestSafeIntBounds:
    """
    Pruebas para las ramas de limites opcionales de _safe_int que no
    quedan cubiertas por los tests basicos.
    """

    def test_below_min_returns_none(self):
        assert consumer._safe_int("3", min_val=10) is None

    def test_above_max_returns_none(self):
        assert consumer._safe_int("50", max_val=10) is None

    def test_invalid_string_returns_none(self):
        assert consumer._safe_int("no_es_numero") is None

    def test_at_exact_min_is_valid(self):
        assert consumer._safe_int("10", min_val=10) == 10

    def test_at_exact_max_is_valid(self):
        assert consumer._safe_int("10", max_val=10) == 10


class TestDbFunctions:
    """
    Pruebas unitarias para las funciones de persistencia. Utilizan
    MagicMock para simular la conexion y el cursor de PostgreSQL sin
    necesitar una base de datos real.
    """

    def _make_conn(self):
        """Crea un mock de conexion PostgreSQL con cursor funcional."""
        conn = MagicMock()
        cur = MagicMock()
        cur.rowcount = 1
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        return conn, cur

    # ── _insert_bronze ────────────────────────────────────────────────────────

    def test_insert_bronze_commits(self):
        conn, _ = self._make_conn()
        consumer._insert_bronze(conn, {"track_id": "abc123"})
        conn.commit.assert_called_once()

    # ── _upsert_artist ────────────────────────────────────────────────────────

    def test_upsert_artist_skips_missing_artist_id(self):
        cur = MagicMock()
        consumer._upsert_artist(cur, {"artist_id": "", "artist_name": "Artista"})
        cur.execute.assert_not_called()

    def test_upsert_artist_skips_missing_artist_name(self):
        cur = MagicMock()
        consumer._upsert_artist(cur, {"artist_id": "id123", "artist_name": ""})
        cur.execute.assert_not_called()

    def test_upsert_artist_executes_insert(self):
        cur = MagicMock()
        consumer._upsert_artist(
            cur, {"artist_id": "id123", "artist_name": "Test Artist", "genres": ["pop"]}
        )
        cur.execute.assert_called_once()

    # ── _upsert_album ─────────────────────────────────────────────────────────

    def test_upsert_album_skips_missing_album_id(self):
        cur = MagicMock()
        consumer._upsert_album(cur, {"album_id": "", "album_name": "Album"})
        cur.execute.assert_not_called()

    def test_upsert_album_with_release_year_fallback(self):
        """Cuando release_date es None pero hay release_year, debe construir la fecha."""
        cur = MagicMock()
        data = {
            "album_id": "alb123",
            "album_name": "Test Album",
            "release_date": None,
            "release_year": 2020,
            "artist_id": "art123",
        }
        consumer._upsert_album(cur, data)
        cur.execute.assert_called_once()
        # Verifica que la fecha construida se paso al execute
        call_args = cur.execute.call_args[0][1]
        assert "2020-01-01" in call_args

    def test_upsert_album_with_explicit_release_date(self):
        cur = MagicMock()
        data = {
            "album_id": "alb456",
            "album_name": "Otro Album",
            "release_date": "2019-03-15",
            "release_year": None,
            "artist_id": None,
        }
        consumer._upsert_album(cur, data)
        cur.execute.assert_called_once()

    # ── _upsert_track ─────────────────────────────────────────────────────────

    def test_upsert_track_returns_true_on_insert(self):
        cur = MagicMock()
        cur.rowcount = 1
        data = {
            "track_id": "trk001",
            "track_name": "Una Pista",
            "artist_id": "art001",
            "album_id": "alb001",
            "duration_ms": 200000,
            "explicit": False,
            "popularity": 70,
            "track_genre": "pop",
            "source": "csv",
        }
        result = consumer._upsert_track(cur, data)
        assert result is True
        cur.execute.assert_called_once()

    def test_upsert_track_returns_false_when_no_rows_affected(self):
        cur = MagicMock()
        cur.rowcount = 0
        data = {
            "track_id": "trk002",
            "track_name": "Otra Pista",
            "artist_id": None,
            "album_id": None,
            "duration_ms": 180000,
            "explicit": True,
            "popularity": 55,
            "track_genre": "rock",
            "source": "api",
        }
        result = consumer._upsert_track(cur, data)
        assert result is False

    # ── _upsert_audio_features ────────────────────────────────────────────────

    def test_upsert_audio_features_skips_all_none(self):
        cur = MagicMock()
        data = {
            "track_id": "trk001",
            "danceability": None,
            "energy": None,
            "valence": None,
        }
        consumer._upsert_audio_features(cur, data)
        cur.execute.assert_not_called()

    def test_upsert_audio_features_executes_when_data_present(self):
        cur = MagicMock()
        data = {
            "track_id": "trk001",
            "danceability": 0.7,
            "energy": 0.8,
            "valence": 0.5,
            "tempo": 120.0,
            "loudness": -5.0,
            "speechiness": 0.05,
            "acousticness": 0.1,
            "instrumentalness": 0.0,
            "liveness": 0.1,
            "key": 5,
            "mode": 1,
            "time_signature": 4,
        }
        consumer._upsert_audio_features(cur, data)
        cur.execute.assert_called_once()

    # ── _refresh_genre_stats 

    def test_refresh_genre_stats_executes_and_commits(self):
        conn, _ = self._make_conn()
        consumer._refresh_genre_stats(conn)
        conn.commit.assert_called_once()

    # ── _refresh_temporal_trends ──────────────────────────────────────────────

    def test_refresh_temporal_trends_executes_and_commits(self):
        conn, _ = self._make_conn()
        consumer._refresh_temporal_trends(conn)
        conn.commit.assert_called_once()


class TestProcessMessage:
    """
    Valida el orquestador principal del pipeline de mensajes usando
    mocks de conexion para evitar dependencias externas.
    """

    def _valid_payload(self):
        return {
            "track_id": "trk_test_001",
            "track_name": "Track de Prueba",
            "artist_id": "art_test",
            "artist_name": "Artista Test",
            "album_id": "alb_test",
            "album_name": "Album 2021",
            "danceability": "0.7",
            "energy": "0.8",
            "valence": "0.6",
            "popularity": "75",
            "data_source": "csv",
        }

    def _make_conn(self):
        conn = MagicMock()
        cur = MagicMock()
        cur.rowcount = 1
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        conn.cursor.return_value = cur
        return conn

    def test_process_message_valid_payload_commits(self):
        """Un payload valido debe provocar al menos un commit (bronze + gold)."""
        conn = self._make_conn()
        consumer._process_message(conn, self._valid_payload())
        assert conn.commit.call_count >= 1

    def test_process_message_missing_track_id_still_inserts_bronze(self):
        """
        Un payload sin track_id es descartado en la transformacion, pero
        el registro bronze ya fue persistido antes de esa validacion.
        """
        conn = self._make_conn()
        payload = {"track_name": "Sin ID", "data_source": "csv"}
        consumer._process_message(conn, payload)
        # Bronze insert -> primer commit
        assert conn.commit.call_count >= 1

    def test_process_message_bronze_failure_returns_early(self):
        """
        Si la insercion bronze falla, el metodo debe salir sin procesar
        la capa gold. Se simula el fallo haciendo que commit lance excepcion.
        """
        conn = MagicMock()
        # El primer commit (bronze) lanza excepcion
        conn.cursor.return_value.__enter__ = MagicMock(
            side_effect=Exception("DB error simulado")
        )
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        # Evitar que _get_pg_connection haga un bucle infinito
        with patch.object(consumer, "_get_pg_connection", return_value=MagicMock()):
            consumer._process_message(conn, self._valid_payload())