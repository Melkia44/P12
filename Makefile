# ═══════════════════════════════════════════════════════════════════════
#  Sport Data Solution — Makefile v3.2.0
#  Usage : make <cible>
# ═══════════════════════════════════════════════════════════════════════
PYTHON := .venv/bin/python
PIP    := .venv/bin/pip

.PHONY: help venv install build up down reset check run demo logs clean \
        kestra-up kestra-open kestra-sync test coverage healthcheck

help:
	@echo "═══ Sport Data Solution v3.2.0 ═══════════════════════════════"
	@echo ""
	@echo "  📦 Installation"
	@echo "    venv          — Crée l'environnement virtuel .venv/"
	@echo "    install       — Installe les dépendances Python"
	@echo "    build         — Build l'image Docker sds-pipeline:latest"
	@echo ""
	@echo "  🐳 Infrastructure Docker"
	@echo "    up            — Lance Postgres + pgAdmin + Kestra"
	@echo "    down          — Arrête les conteneurs (données préservées)"
	@echo "    reset         — ⚠️  Détruit les volumes et repart à zéro"
	@echo "    healthcheck   — Vérifie l'état complet de la stack"
	@echo ""
	@echo "  🚀 Exécution pipeline"
	@echo "    check         — Vérifie uniquement la connexion BDD"
	@echo "    run           — Lance le pipeline en local (sans Kestra)"
	@echo "    demo          — Même chose avec logs DEBUG"
	@echo ""
	@echo "  🎯 Orchestration Kestra (UI : http://localhost:8089)"
	@echo "    kestra-up     — Lance Kestra + pré-build de l'image"
	@echo "    kestra-open   — Ouvre l'UI Kestra dans le navigateur"
	@echo "    kestra-sync   — Recharge les flows"
	@echo ""
	@echo "  🧪 Qualité code"
	@echo "    test          — Lance la suite de tests pytest"
	@echo "    coverage      — Tests + rapport de couverture HTML"
	@echo ""
	@echo "  🧹 Maintenance"
	@echo "    logs          — Affiche les dernières lignes du log"
	@echo "    clean         — Nettoie output/ et monitoring/"

# ─── Python & environnement ────────────────────────────────────────────
venv:
	@if [ ! -d ".venv" ]; then \
	    echo "🔧 Création de l'environnement virtuel..."; \
	    python3 -m venv .venv; \
	    echo "✅ Venv créé dans .venv/"; \
	else \
	    echo "✓ Venv déjà présent dans .venv/"; \
	fi

install: venv
	@echo "📦 Installation des dépendances Python..."
	@$(PIP) install --upgrade pip --quiet
	@$(PIP) install -r requirements.txt
	@echo "✅ Dépendances installées"

# ─── Pipeline ──────────────────────────────────────────────────────────
check:
	@$(PYTHON) run_pipeline.py --check-only

run:
	@$(PYTHON) run_pipeline.py

demo:
	@LOG_LEVEL=DEBUG $(PYTHON) run_pipeline.py

# ─── Docker & infrastructure ───────────────────────────────────────────
build:
	@echo "🔨 Build de l'image sds-pipeline:latest..."
	@docker compose build sds-pipeline

up: build
	@docker compose up -d postgres pgadmin kestra
	@echo "⏳ Attente que PostgreSQL soit prêt..."
	@until docker compose exec -T postgres pg_isready -U $${POSTGRES_USER:-sds} >/dev/null 2>&1; do sleep 1; done
	@echo ""
	@echo "✅ Infrastructure prête :"
	@echo "   PostgreSQL : localhost:5432"
	@echo "   pgAdmin    : http://localhost:5050"
	@echo "   Kestra UI  : http://localhost:8089"

down:
	@docker compose down

reset:
	@docker compose down -v
	@$(MAKE) up
	@echo "⚠️  Infrastructure réinitialisée"

healthcheck:
	@echo "═══ 🐳 Containers ═══"
	@docker compose ps --format "table {{.Name}}\t{{.Status}}"
	@echo ""
	@echo "═══ 🌐 HTTP services ═══"
	@curl -s -o /dev/null -w "pgAdmin  (5050): %{http_code}\n" http://localhost:5050
	@curl -s -o /dev/null -w "Kestra   (8089): %{http_code}\n" http://localhost:8089
	@echo ""
	@echo "═══ 🐘 PostgreSQL ═══"
	@docker compose exec -T postgres pg_isready -U sds

# ─── Kestra ────────────────────────────────────────────────────────────
kestra-up: up
	@echo ""
	@echo "🎯 Kestra est prêt sur http://localhost:8089"

kestra-open:
	@python3 -c "import webbrowser; webbrowser.open('http://localhost:8089')" 2>/dev/null || \
	  xdg-open http://localhost:8089 2>/dev/null || \
	  echo "Ouvrez manuellement : http://localhost:8089"

kestra-sync:
	@echo "📂 Les flows sont montés en read-only depuis orchestration/kestra/flows/"
	@echo "   Si les flows ne s'affichent pas : docker compose restart kestra"

# ─── Qualité code ──────────────────────────────────────────────────────
test:
	@$(PYTHON) -m pytest tests/ -v --cov=pipeline --cov-report=term-missing

coverage:
	@$(PYTHON) -m pytest tests/ --cov=pipeline --cov-report=html --cov-report=term
	@echo "Rapport HTML généré dans htmlcov/index.html"

# ─── Maintenance ───────────────────────────────────────────────────────
logs:
	@tail -n 50 monitoring/pipeline.log 2>/dev/null || echo "Aucun log pour l'instant."

clean:
	@rm -rf output/*.xlsx monitoring/*.json monitoring/*.log
	@echo "✓ Outputs et monitoring nettoyés"