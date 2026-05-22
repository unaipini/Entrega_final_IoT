# Sistema de Análisis Musical IoT - Spotify

Proyecto final para la asignatura de Aplicaciones IoT. Sistema integral que gestiona el ciclo de vida del dato musical combinando CSV y tiempo real, API de Spotify.

## 🚀 Arquitectura del Proyecto
* **Captura:** CSV + Spotify Web API (`spotipy`)
* **Envío:** Protocolo MQTT con TLS
* **Procesamiento:** Pipeline ETL con Apache Airflow
* **Persistencia:** PostgreSQL
* **Visualización:** Grafana OSS

## 🛠️ Requisitos Previos
* Docker y Docker Compose
* Python 3.11+


## Estructura del proyecto
nombre-del-repositorio/
├── .github/
│   └── workflows/          # Aquí irá el archivo ci.yml de GitHub Actions
├── airflow/                # Archivos y DAGs de Apache Airflow
├── datos/
│   └── tu_archivo.csv      # El que ya subió tu compañero
├── docker/                 # Configuraciones específicas de contenedores
│   └── mosquitto/
├── src/                    # Todo tu código fuente Python
│   ├── productor.py
│   └── consumidor.py
├── .gitignore              # El que acabamos de crear
├── .pre-commit-config.yaml # Tu configuración de seguridad
├── docker-compose.yml      # El orquestador de contenedores
└── README.md               # La documentación