# Explicación completa del Pipeline IoT de Spotify

## ¿Qué hace este proyecto?

Es un **pipeline de datos completo** que simula un sistema IoT donde los "dispositivos" son fuentes de música. El sistema captura canciones desde dos orígenes, las transmite por red usando el protocolo estándar IoT, las almacena en una base de datos y las visualiza en tiempo real.

---

## Tecnologías y por qué se usan

| Tecnología | Rol en el proyecto | Por qué esta y no otra |
|---|---|---|
| **MQTT / Mosquitto** | Transporte de mensajes | Protocolo estándar en IoT. Ligero, basado en publish/subscribe. Es lo que usan sensores, dispositivos industriales y wearables reales. |
| **PostgreSQL** | Almacén de datos | Base de datos relacional robusta, gratuita y compatible con Grafana de forma nativa. |
| **Apache Airflow** | Orquestador ETL | Permite definir pipelines como código Python. Es el estándar de mercado en ingeniería de datos. El profesor lo reconocerá sin explicación. |
| **Grafana** | Visualización | Se conecta directamente a PostgreSQL. Los dashboards se configuran sin código. Es la herramienta de observabilidad más usada en entornos DevOps e IoT. |
| **Docker Compose** | Infraestructura | Levanta todos los servicios con un solo comando. Aísla cada componente en su propio contenedor. Reproducible en cualquier máquina. |
| **Spotipy** | Librería Python | Cliente oficial de la API REST de Spotify. Gestiona la autenticación OAuth2 automáticamente. |
| **Paho-MQTT** | Librería Python | Cliente MQTT estándar para Python. Usado tanto en el productor (publish) como en el consumidor (subscribe). |
| **Psycopg2** | Librería Python | Driver estándar de PostgreSQL para Python. Permite ejecutar SQL directamente desde el consumidor y el DAG de Airflow. |
| **Pre-commit** | Calidad de código | Ejecuta verificaciones automáticas antes de cada `git commit`. Evita que entre código mal formateado o con vulnerabilidades. |
| **GitHub Actions** | CI/CD | Pipeline automatizado en la nube que valida el código en cada push. |

---

## Diagrama del flujo de información

```
┌─────────────────────────────────────────────────────────────────┐
│                      FUENTES DE DATOS                           │
│                                                                 │
│   spotify_tracks.csv          API de Spotify                    │
│   (histórico local)           (tiempo real)                     │
│         │                          │                            │
│         └──────────┬───────────────┘                            │
│                    │  source = "csv" | "api"                    │
└────────────────────┼────────────────────────────────────────────┘
                     │
                     ▼
          ┌──────────────────┐
          │    PRODUCTOR     │  src/producer/main.py
          │  (Python)        │  - Lee CSV fila a fila (cada 5s)
          │                  │  - Llama a Spotify cada 1h
          └────────┬─────────┘
                   │  JSON por MQTT (QoS 1)
                   │  topic: spotify/tracks
                   ▼
          ┌──────────────────┐
          │    MOSQUITTO     │  broker MQTT
          │  (broker MQTT)   │  - Puerto 1883 (TCP)
          │                  │  - Puerto 9001 (WebSocket)
          └────────┬─────────┘
                   │  subscribe al topic
                   ▼
          ┌──────────────────┐
          │   CONSUMIDOR     │  src/consumer/main.py
          │  (Python)        │  - Deserializa el JSON
          │                  │  - INSERT en PostgreSQL
          └────────┬─────────┘
                   │  INSERT / ON CONFLICT DO NOTHING
                   ▼
          ┌──────────────────┐
          │   POSTGRESQL     │  esquema: artists, albums,
          │                  │  tracks, audio_features
          │                  │  party_index (campo calculado)
          └────────┬─────────┘
                   │
          ┌────────┴──────────────┐
          │                       │
          ▼                       ▼
  ┌──────────────┐      ┌──────────────────┐
  │   GRAFANA    │      │    AIRFLOW       │
  │  dashboards  │      │  DAG: extract    │
  │  auto-       │      │  → transform     │
  │  provisionados│      │  → load (1h)    │
  └──────────────┘      └──────────────────┘
```

---

## Estructura de carpetas explicada

### Raíz del proyecto

```
Final/
├── docker-compose.yml       ← cerebro de la infraestructura
├── .env.example             ← plantilla de credenciales
├── .env                     ← credenciales reales (NO subir a Git)
├── .gitignore               ← excluye .env, __pycache__, logs
├── .pre-commit-config.yaml  ← reglas de calidad de código
├── Makefile                 ← atajos de comandos
└── README.md                ← documentación de arranque
```

**`docker-compose.yml`** — El fichero más importante de la infraestructura. Define los 7 servicios del stack y cómo se relacionan entre sí:
- Qué imagen/Dockerfile usa cada servicio
- Qué variables de entorno recibe
- Qué puertos expone al exterior
- De qué otros servicios depende (orden de arranque)
- Qué volúmenes de disco monta

**`.env`** — Contiene todas las contraseñas y claves API. Docker Compose lo lee automáticamente. Nunca se sube a Git (está en `.gitignore`).

**`Makefile`** — Atajos para no tener que recordar comandos largos:
- `make up` → arranca todo
- `make down` → para todo
- `make logs` → ver logs en tiempo real
- `make lint` → ejecutar verificaciones de código
- `make fernet` → generar clave de Airflow

---

### `mosquitto/`

```
mosquitto/
└── config/
    └── mosquitto.conf   ← configuración del broker MQTT
```

**`mosquitto.conf`** — Configura el broker MQTT:
- **Listener 1883**: puerto estándar MQTT sobre TCP. Los contenedores del productor y consumidor se conectan aquí.
- **Listener 9001**: WebSocket, útil para herramientas de depuración desde el navegador.
- **`allow_anonymous true`**: en red Docker interna no se necesita autenticación. En producción real se usaría `password_file`.
- **Persistencia**: los mensajes se guardan en disco por si el broker se reinicia.

---

### `postgres/`

```
postgres/
└── init/
    └── 01_schema.sql   ← se ejecuta automáticamente al primer arranque
```

**`01_schema.sql`** — PostgreSQL ejecuta este script automáticamente la primera vez que arranca el contenedor y el volumen está vacío. Crea cuatro tablas:

| Tabla | Qué almacena |
|---|---|
| `artists` | Artistas únicos con nombre, géneros y popularidad |
| `albums` | Álbumes vinculados a su artista |
| `tracks` | Pistas con referencia al artista y álbum. Campo `source` indica si vino del CSV o de la API |
| `audio_features` | Características de audio por pista. Incluye `party_index` como columna calculada |

El campo **`party_index`** es clave para la evaluación: es un campo que **el pipeline calcula**, no viene de Spotify:
```sql
party_index GENERATED ALWAYS AS ((danceability + energy + valence) / 3.0) STORED
```

También hay una **vista** (`vw_tracks_with_party`) que une todas las tablas. Grafana la usa directamente para simplificar las consultas de los paneles.

---

### `grafana/`

```
grafana/
└── provisioning/
    ├── datasources/
    │   └── postgres.yml    ← conecta Grafana a PostgreSQL automáticamente
    └── dashboards/
        ├── dashboard.yml   ← dice a Grafana dónde buscar los JSON
        └── spotify.json    ← definición de los 5 paneles del dashboard
```

El concepto clave aquí es el **provisioning**: Grafana, al arrancar, lee estos ficheros y configura automáticamente el datasource y el dashboard. No hay que hacer nada a mano en la interfaz web.

**`postgres.yml`** — Define la conexión a PostgreSQL: host, puerto, usuario y contraseña. Grafana se conecta directamente a la base de datos para leer los datos de los paneles.

**`spotify.json`** — Contiene la definición completa del dashboard en formato JSON. Los 5 paneles incluidos:
1. **Total de pistas ingestadas** → contador simple
2. **Índice de Fiesta medio** → stat con umbral de color (rojo < 0.4 < amarillo < 0.7 < verde)
3. **Pistas por fuente** → gráfico donut CSV vs API
4. **Pistas ingestadas por minuto** → serie temporal (muestra el pipeline en tiempo real)
5. **Top 10 por party_index** → barra horizontal con gradiente de color

---

### `airflow/`

```
airflow/
├── Dockerfile           ← imagen base de Airflow + dependencias propias
├── requirements.txt     ← spotipy, psycopg2, paho-mqtt
└── dags/
    └── spotify_etl.py   ← el DAG con las 3 tareas
```

**¿Qué es un DAG?** Un DAG (Directed Acyclic Graph) es la forma en que Airflow define un pipeline: un conjunto de tareas con un orden definido sin ciclos.

**`spotify_etl.py`** — Define el DAG con 3 tareas encadenadas:

```
extract_from_api  →  transform  →  load_to_postgres
```

- **`extract_from_api`**: llama a dos endpoints de Spotify y devuelve los datos crudos:
  - `/browse/new-releases` → últimos álbumes publicados (hasta 20)
  - `/search?genre:pop` → canciones de género pop para enriquecer el histórico
  
- **`transform`**: recibe los datos crudos y los normaliza:
  - Trunca strings al tamaño máximo de la columna
  - Normaliza fechas (`"2024"` → `"2024-01-01"`)
  - Calcula `party_index = (danceability + energy + valence) / 3`
  
- **`load_to_postgres`**: inserta en las 4 tablas usando `ON CONFLICT DO NOTHING` (idempotente: si el DAG se re-ejecuta no falla).

El DAG se ejecuta `@hourly`. Los datos entre tareas se pasan mediante **XCom**, el mecanismo de comunicación interno de Airflow.

---

### `src/producer/`

```
src/producer/
├── Dockerfile          ← imagen Python 3.11 slim
├── requirements.txt    ← paho-mqtt, spotipy, python-dotenv
├── main.py             ← bucle principal del productor
└── spotify_client.py   ← módulo de acceso a la API de Spotify
```

**`spotify_client.py`** — Tiene una sola responsabilidad: autenticarse con Spotify (usando Client Credentials Flow, sin login de usuario) y generar pistas de nuevos lanzamientos. Es un generador Python: produce una pista a la vez sin cargar todas en memoria.

**`main.py`** — El bucle principal:
1. **Al arrancar**: lee el CSV fila a fila y publica cada pista en MQTT con `source="csv"`. Espera `CSV_PUBLISH_INTERVAL` segundos entre filas (por defecto 5s) para simular un dispositivo IoT enviando datos de forma continua.
2. **Cada hora**: llama a `spotify_client.stream_new_releases()` y publica cada pista con `source="api"`.

Cada mensaje publicado es un **JSON** con los campos de la pista:
```json
{
  "track_id": "4iV5W9uYEdYUVa79Axb7Rh",
  "track_name": "Blinding Lights",
  "artist_name": "The Weeknd",
  "danceability": 0.514,
  "energy": 0.730,
  "valence": 0.334,
  "source": "csv"
}
```

---

### `src/consumer/`

```
src/consumer/
├── Dockerfile    ← imagen Python 3.11 slim
├── requirements.txt
└── main.py       ← suscriptor MQTT que persiste en PostgreSQL
```

**`main.py`** — Funciona de forma reactiva (event-driven):
1. Se suscribe al topic MQTT `spotify/tracks`.
2. Por cada mensaje recibido, se ejecuta el callback `on_message`.
3. Dentro del callback, deserializa el JSON, inserta el artista, la pista y los audio_features en PostgreSQL en una única transacción.
4. Si PostgreSQL no está disponible al arrancar, reintenta la conexión cada 5 segundos (tolerancia a fallos).

El consumidor usa `INSERT ... ON CONFLICT DO NOTHING` para ser **idempotente**: si por algún motivo el mismo mensaje llega dos veces, no falla ni duplica datos.

---

### `data/`

```
data/
└── spotify_tracks.csv   ← 15 canciones reales con todos los campos
```

CSV con 15 canciones reales incluyendo: nombre, artista, álbum, duración, popularidad, y todos los campos de audio_features (danceability, energy, valence, tempo, loudness…). El productor lo lee al arrancar para poblar la BD con datos históricos antes de que lleguen los datos de la API en tiempo real.

---

### `tests/`

```
tests/
├── conftest.py        ← fixtures compartidas (payloads de ejemplo)
├── test_consumer.py   ← tests del consumidor
└── test_producer.py   ← tests del productor
```

**`conftest.py`** — Define fixtures de pytest: datos de ejemplo reutilizables en todos los tests. Por ejemplo, un payload MQTT completo de "Blinding Lights".

**`test_consumer.py`** — Verifica:
- Que `_safe_float()` y `_safe_int()` manejan `None`, strings vacíos y valores inválidos.
- Que la fórmula `party_index = (danceability + energy + valence) / 3` es correcta.
- Que los mensajes sin `track_id` se descartan.

**`test_producer.py`** — Verifica:
- Que el CSV tiene las columnas requeridas.
- Que los campos numéricos son convertibles a float.
- Que `danceability` está entre 0.0 y 1.0.
- Que el payload MQTT se construye correctamente.

---

### `.pre-commit-config.yaml`

Define los hooks que se ejecutan automáticamente antes de cada `git commit`:

| Hook | Qué hace | Por qué es importante |
|---|---|---|
| `black` | Formatea el código Python | Estilo consistente sin discusiones |
| `isort` | Ordena los imports | Legibilidad y evita conflictos de merge |
| `flake8` | Detecta errores de estilo PEP 8 | El código Python tiene un estilo estándar |
| **`bandit`** | Análisis de seguridad del código | Detecta inyecciones SQL, `eval()` peligroso, algoritmos de cifrado débiles, contraseñas hardcodeadas **en el código** |
| **`gitleaks`** | Detecta secretos en Git | Detecta claves API o contraseñas que se hayan colado en commits anteriores |
| `check-yaml` | Valida YAML | Evita subir ficheros de configuración rotos |
| `check-json` | Valida JSON | El dashboard de Grafana es un JSON |

> **Bandit vs Gitleaks**: son complementarios, no redundantes. Bandit analiza el código Python en busca de patrones peligrosos. Gitleaks analiza el historial de Git en busca de strings que parezcan credenciales reales.

---

### `.github/workflows/ci.yml`

Pipeline de CI/CD con 3 fases encadenadas:

```
lint → test → build
```

**Fase lint**: ejecuta pre-commit sobre todo el repositorio en GitHub. Si algún hook falla, el pipeline se para y no se puede hacer merge.

**Fase test**: levanta un PostgreSQL real como "servicio" de GitHub Actions, aplica el esquema SQL y ejecuta los tests con cobertura mínima del 70%.

**Fase build**: construye las 3 imágenes Docker (productor, consumidor, Airflow) para verificar que los Dockerfiles son válidos.

Se activa en cada `push` y `pull request` a `main` y `develop`.

---

## Flujo completo durante la demo

1. `make up` → arrancan los 7 contenedores
2. El productor lee el CSV y publica 15 pistas en MQTT (una cada 5s)
3. El consumidor las recibe e inserta en PostgreSQL
4. Grafana muestra los datos en tiempo real (refresh cada 30s)
5. Cada hora, el productor llama a Spotify y publica nuevas canciones con `source=api`
6. En Grafana, el panel donut muestra que los datos vienen de dos fuentes distintas
7. El DAG de Airflow aparece en `http://localhost:8080` y se puede ejecutar manualmente

---

## Resumen de puertos expuestos

| Puerto | Servicio | Para qué |
|---|---|---|
| `3000` | Grafana | Dashboards → http://localhost:3000 |
| `8080` | Airflow webserver | DAG UI → http://localhost:8080 |
| `5432` | PostgreSQL | Conexión directa con DBeaver, psql, etc. |
| `1883` | Mosquitto MQTT | Clientes MQTT externos |
| `9001` | Mosquitto WebSocket | Depuración desde navegador |
