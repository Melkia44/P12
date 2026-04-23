# ═══════════════════════════════════════════════════════════════════════
#  Sport Data Solution — Makefile v3.1.0
#  Usage : make <cible>
# ═══════════════════════════════════════════════════════════════════════
.PHONY: help install build up down reset check run demo logs clean \
        kestra-up kestra-open kestra-sync test coverage

help:
	@echo "═══ Sport Data Solution v3.1.0 ═══════════════════════════════"
	@echo ""
	@echo "  📦 Installation"
	@echo "    install       — Installe les dépendances Python locales"
	@echo "    build         — Build l'image Docker sds-pipeline:latest"
	@echo ""
	@echo "  🐳 Infrastructure Docker"
	@echo "    up            — Lance Postgres + pgAdmin + Kestra"
	@echo "    down          — Arrête les conteneurs (données préservées)"
	@echo "    reset         — ⚠️  Détruit les volumes et repart à zéro"
	@echo ""
	@echo "  🚀 Exécution pipeline"
	@echo "    check         — Vérifie uniquement la connexion BDD"
	@echo "    run           — Lance le pipeline en local (sans Kestra)"
	@echo "    demo          — Même chose avec logs DEBUG"
	@echo ""
	@echo "  🎯 Orchestration Kestra (UI : http://localhost:8089)"
	@echo "    kestra-up     — Lance Kestra + pré-build de l'image pipeline"
	@echo "    kestra-open   — Ouvre l'UI Kestra dans le navigateur"
	@echo "    kestra-sync   — Recharge les flows depuis orchestration/kestra/flows/"
	@echo ""
	@echo "  🧪 Qualité code"
	@echo "    test          — Lance la suite de tests pytest"
	@echo "    coverage      — Tests + rapport de couverture HTML"
	@echo ""
	@echo "  🧹 Maintenance"
	@echo "    logs          — Affiche les dernières lignes du log"
	@echo "    clean         — Nettoie output/ et monitoring/"

install:
	pip install -r requirements.txt

build:
	@echo "🔨 Build de l'image sds-pipeline:latest (utilisée par Kestra)..."
	docker compose build sds-pipeline

up: build
	docker compose up -d postgres pgadmin kestra
	@echo "⏳ Attente que PostgreSQL soit prêt..."
	@until docker compose exec -T postgres pg_isready -U $${POSTGRES_USER:-sds} >/dev/null 2>&1; do sleep 1; done
	@echo ""
	@echo "✅ Infrastructure prête :"
	@echo "   PostgreSQL : localhost:5432"
	@echo "   pgAdmin    : http://localhost:5050"
	@echo "   Kestra UI  : http://localhost:8089"

down:
	docker compose down

reset:
	docker compose down -v
	@$(MAKE) up
	@echo "⚠️  Infrastructure réinitialisée — tous les runs précédents sont perdus"

check:
	python run_pipeline.py --check-only

run:
	python run_pipeline.py

demo:
	LOG_LEVEL=DEBUG python run_pipeline.py

kestra-up: up
	@echo ""
	@echo "🎯 Kestra est prêt sur http://localhost:8089"
	@echo "   Les flows du dossier orchestration/kestra/flows/ y sont déjà synchronisés."
	@echo "   Exécutez le flow 'sds.poc.sds_pipeline' depuis l'UI."

kestra-open:
	@python -c "import webbrowser; webbrowser.open('http://localhost:8089')" 2>/dev/null || \
	  xdg-open http://localhost:8089 2>/dev/null || \
	  open http://localhost:8089 2>/dev/null || \
	  echo "Ouvrez manuellement : http://localhost:8089"

kestra-sync:
	@echo "📂 Les flows sont montés en lecture depuis orchestration/kestra/flows/"
	@echo "   Kestra les détecte automatiquement. Si ce n'est pas le cas :"
	@echo "   docker compose restart kestra"

# ─── Qualité code ──────────────────────────────────────────────────────
test:
	@echo "🧪 Lancement des tests unitaires..."
	pytest tests/ -v --cov=pipeline --cov-report=term-missing

coverage:
	pytest tests/ --cov=pipeline --cov-report=html --cov-report=term
	@echo "Rapport HTML généré dans htmlcov/index.html"

logs:
	@tail -n 50 monitoring/pipeline.log 2>/dev/null || echo "Aucun log pour l'instant."

clean:
	rm -rf output/*.xlsx monitoring/*.json monitoring/*.log
	@echo "✓ Outputs et monitoring nettoyés"
