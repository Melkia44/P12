# ═══════════════════════════════════════════════════════════════════════
#  Sport Data Solution — Image pipeline runnable
#
#  Cette image est utilisée de deux manières :
#    1) `make run`      → lance le pipeline complet en local
#    2) Kestra tâches   → chaque étape du flow exécute un module Python
#                         isolé via cette même image (reproductibilité)
#
#  Build : `docker build -t sds-pipeline:latest .`
# ═══════════════════════════════════════════════════════════════════════
FROM python:3.12-slim

# Dépendances système minimales (pour psycopg, openpyxl)
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Deps Python (cache Docker → on ne réinstalle que si requirements change)
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Code projet (on copie en dernier pour maximiser le cache)
COPY config/   ./config/
COPY pipeline/ ./pipeline/
COPY sql/      ./sql/
COPY data/     ./data/
COPY run_pipeline.py ./

# Création des dossiers runtime (évite les FileNotFoundError)
RUN mkdir -p /app/output /app/monitoring

# Le PYTHONPATH permet d'importer `config` et `pipeline` depuis n'importe où
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Entrypoint par défaut = aide ; Kestra override cette commande par tâche
CMD ["python", "-c", "print('Image sds-pipeline prête. Utilisez python -m pipeline.<module> --run-id <id>')"]
