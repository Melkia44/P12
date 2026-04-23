"""
╔══════════════════════════════════════════════════════════════════════╗
║  SPORT DATA SOLUTION — Orchestrateur local (rétrocompat sans Kestra) ║
║                                                                      ║
║  Pipeline 7 étapes :                                                 ║
║    1. EXTRACT       — Excel sources  -> PostgreSQL raw.*             ║
║    2. GENERATE      — Simulateur Strava-like 12 mois                 ║
║    3. GEO           — Géocodage Google Maps + validation seuils      ║
║    4. DATA QUALITY  — Great Expectations + scoring métier            ║
║    5. TRANSFORM     — Règles métier + row_hash idempotency           ║
║    6. LOAD          — Export Excel 8 feuilles pour PowerBI           ║
║    7. NOTIFY        — Slack individuel + récap global                ║
║                                                                      ║
║  Usage :                                                             ║
║      python run_pipeline.py                                          ║
║      python run_pipeline.py --check-only                             ║
╚══════════════════════════════════════════════════════════════════════╝
"""
import sys
import time
import logging
import argparse
from datetime import datetime
from pathlib import Path

# ─── Import des modules pipeline ────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent))

from config                    import PIPELINE_VERSION, DQ_THRESHOLD, LOG_LEVEL, MONITORING_DIR
from pipeline.db               import ping
from pipeline.extract          import load_sources_to_db
from pipeline.generateur_strava import generate_activities
from pipeline.validation_geo   import validate_geo
from pipeline.data_quality_ge  import run_full_dq
from pipeline.transform        import compute_rewards
from pipeline.load             import export_to_excel
from pipeline.slack_notifier   import notify_activity, send_global_recap
from pipeline.monitoring       import record_run


# ─── Logging ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(MONITORING_DIR / "pipeline.log", mode="a", encoding="utf-8"),
    ],
)
log = logging.getLogger("SDS.runner")


def main():
    parser = argparse.ArgumentParser(description="Sport Data Solution — Pipeline v3.1.0")
    parser.add_argument("--check-only", action="store_true",
                        help="Vérifie uniquement la connexion à la base.")
    args = parser.parse_args()

    log.info("═" * 70)
    log.info(f"SPORT DATA SOLUTION — Pipeline v{PIPELINE_VERSION}")
    log.info("═" * 70)

    if not ping():
        log.critical("Impossible de joindre PostgreSQL. "
                     "Avez-vous lancé `docker compose up -d` et configuré DATABASE_URL dans .env ?")
        sys.exit(2)

    if args.check_only:
        log.info("Connexion BDD OK — sortie (mode --check-only)")
        return

    run_id    = datetime.now().strftime("%Y%m%d%H%M%S")
    run_start = time.time()
    run_date  = datetime.now()

    try:
        log.info("ÉTAPE 1/7 — EXTRACT (Excel -> PostgreSQL)")
        volumes = load_sources_to_db()

        log.info("ÉTAPE 2/7 — GÉNÉRATION STRAVA-LIKE (12 mois)")
        df_activities = generate_activities()

        log.info("ÉTAPE 3/7 — VALIDATION GÉOGRAPHIQUE")
        df_geo = validate_geo()
        n_alerts = int((df_geo["geo_status"] == "ALERTE").sum())

        log.info("ÉTAPE 4/7 — DATA QUALITY (Great Expectations + métier)")
        dq_report = run_full_dq()
        if dq_report["blocking"]:
            log.critical(f"Score DQ {dq_report['score']}/100 < seuil {DQ_THRESHOLD} -> pipeline bloqué")
            sys.exit(3)

        log.info("ÉTAPE 5/7 — TRANSFORM (règles métier)")
        df_rewards = compute_rewards(run_id=run_id, pipeline_version=PIPELINE_VERSION)

        log.info("ÉTAPE 6/7 — LOAD (export Excel pour PowerBI)")
        excel_path = export_to_excel(run_id, df_rewards, dq_report)

        log.info("ÉTAPE 7/7 — NOTIFICATIONS SLACK")
        kpis = {
            "n_employees": len(df_rewards),
            "n_prime":     int(df_rewards["eligible_prime"].sum()),
            "n_wellness":  int(df_rewards["eligible_wellness"].sum()),
            "cost_eur":    round(float(df_rewards["prime_amount"].sum()), 2),
            "n_alerts":    n_alerts,
        }
        send_global_recap({
            "run_date": run_date.strftime("%Y-%m-%d %H:%M"),
            "n_total":  kpis["n_employees"],
            "n_prime":  kpis["n_prime"],
            "n_wellness": kpis["n_wellness"],
            "cost_eur": kpis["cost_eur"],
            "n_alerts": kpis["n_alerts"],
            "dq_score": dq_report["score"],
        })
        # On notifie seulement les 3 dernières activités pour la démo
        df_emp = df_rewards.set_index("employee_id")
        for _, act in df_activities.head(3).iterrows():
            emp = df_emp.loc[act["employee_id"]].to_dict() if act["employee_id"] in df_emp.index else {}
            notify_activity(act.to_dict(), emp)

        duration = round(time.time() - run_start, 2)
        record_run(
            run_id=run_id, run_date=run_date, version=PIPELINE_VERSION,
            status="SUCCESS", dq_score=dq_report["score"],
            duration_s=duration, kpis=kpis,
            details={
                "volumes":    volumes,
                "dq_checks":  dq_report["checks"],
                "ge_summary": dq_report["ge_summary"],
                "excel_path": str(excel_path),
            },
        )

        log.info("═" * 70)
        log.info(f"PIPELINE TERMINÉ EN {duration}s")
        log.info(f"   Run ID  : {run_id}")
        log.info(f"   DQ      : {dq_report['score']}/100")
        log.info(f"   Prime   : {kpis['n_prime']} | Bien-être : {kpis['n_wellness']}")
        log.info(f"   Coût    : {kpis['cost_eur']:,.2f} EUR / an")
        log.info(f"   Excel   : {excel_path}")
        log.info("═" * 70)

    except Exception as e:
        log.critical(f"ERREUR : {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
