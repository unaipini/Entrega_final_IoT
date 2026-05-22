# Makefile
# Atajos para las operaciones más comunes del proyecto.
# Uso: make <objetivo>

# ---------------------------------------------------------------------------
# Variables
# ---------------------------------------------------------------------------
COMPOSE = docker compose
PROJECT = spotify-iot

.PHONY: help up down logs ps build clean fernet install-hooks lint test

# ---------------------------------------------------------------------------
# Ayuda: muestra todos los objetivos disponibles
# ---------------------------------------------------------------------------
help:
	@echo ""
	@echo "Spotify IoT Pipeline — comandos disponibles:"
	@echo ""
	@echo "  make up           Arranca todos los servicios en segundo plano"
	@echo "  make down         Para y elimina los contenedores (mantiene volúmenes)"
	@echo "  make down-v       Para y elimina contenedores Y volúmenes (datos borrados)"
	@echo "  make logs         Muestra los logs de todos los servicios en tiempo real"
	@echo "  make ps           Lista el estado de todos los contenedores"
	@echo "  make build        Reconstruye las imágenes Docker sin caché"
	@echo "  make clean        Elimina imágenes sin usar (prune)"
	@echo "  make fernet       Genera una clave Fernet para AIRFLOW_FERNET_KEY"
	@echo "  make install-hooks  Instala los hooks de pre-commit en el repo local"
	@echo "  make lint         Ejecuta pre-commit sobre todos los ficheros"
	@echo "  make test         Ejecuta los tests unitarios con pytest"
	@echo ""

# ---------------------------------------------------------------------------
# Docker Compose
# ---------------------------------------------------------------------------
up:
	$(COMPOSE) up -d
	@echo ""
	@echo "Servicios iniciados:"
	@echo "  Grafana    → http://localhost:3000"
	@echo "  Airflow    → http://localhost:8080"
	@echo "  PostgreSQL → localhost:5432"
	@echo "  Mosquitto  → localhost:1883"
	@echo ""

down:
	$(COMPOSE) down

down-v:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f

ps:
	$(COMPOSE) ps

build:
	$(COMPOSE) build --no-cache

clean:
	docker image prune -f

# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------
# Genera una clave Fernet válida para Airflow
fernet:
	@python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Instala los hooks de pre-commit en el repositorio Git local
install-hooks:
	pip install pre-commit
	pre-commit install
	@echo "Hooks de pre-commit instalados correctamente."

# Ejecuta todos los hooks de pre-commit manualmente
lint:
	pre-commit run --all-files

# Ejecuta los tests unitarios
test:
	pytest tests/ -v --cov=src --cov-report=term-missing
