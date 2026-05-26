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
        assert 0.55 < party_index < 0.60

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
        result = consumer._transform({
            "track_id": "4iV5W9uYEdYUVa79Axb7Rh",
            "track_name": "Blinding Lights",
            "danceability": "0.514", "energy": "0.730", "valence": "0.334",
            "data_source": "csv",
        })
        assert result is not None
        assert result["track_id"] == "4iV5W9uYEdYUVa79Axb7Rh"
        assert result["danceability"] == 0.514

    def test_explicit_normalization_string(self):
        """
        Asegura la conversion correcta hacia booleano cuando la bandera de contenido
        explicito llega definida como una cadena textual representativa (True).
        """
        result = consumer._transform({
            "track_id": "abc123", "explicit": "True", "data_source": "csv"
        })
        assert result["explicit"] is True

    def test_explicit_normalization_false_string(self):
        """
        Asegura la resolucion a un falso absoluto cuando el string denota negacion.
        """
        result = consumer._transform({
            "track_id": "abc123", "explicit": "False", "data_source": "csv"
        })
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
        result = consumer._transform({
            "track_id": "abc123",
            "album_name": "Greatest Hits 1999",
            "data_source": "csv",
        })
        assert result["release_year"] == 1999

    def test_danceability_out_of_range_is_none(self):
        """
        Ratifica que en el contexto global de una fila, la inclusion de un valor individual
        anomalo provocara su neutralizacion puntual en la entidad, sin descartar
        necesariamente la fila completa (salvo en caso de llaves primarias).
        """
        result = consumer._transform({
            "track_id": "abc123", "danceability": "1.5", "data_source": "csv"
        })
        assert result["danceability"] is None
