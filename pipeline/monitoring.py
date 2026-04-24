"""Monitoring des runs — double écriture (table BDD + fichier JSON).

Surveille la volumétrie et l'état d'exécution à chaque run :
    * table monitoring.pipeline_runs (queryable SQL, JSONB details)
    * fichier monitoring/*.json      (audit trail sur disque)

Utilisation :
    record_run(...)      -> INSERT initial en fin de pipeline local
    finalize_run(...)    -> UPDATE en fin de tâche Kestra (CLI)
"""
import json
import logging
from datetime import datetime

import pandas as pd
from sqlalchemy import text

from config import MONITORING_DIR
from pipeline.db import get_engine, connection

log = logging.getLogger("SDS.monitoring")


def record_run(run_id: str, run_date: datetime, version: str,
               status: str, dq_score: int, duration_s: float,
               kpis: dict, details: dict) -> None:
    """Persiste un rapport de run en base + sur disque."""

    # 1. Écriture base
    with connection() as conn:
        conn.execute(text("""
            INSERT INTO monitoring.pipeline_runs
              (run_id, run_date, pipeline_version, status, dq_score,
               n_employees, n_prime, n_wellness, total_cost_eur,
               n_geo_alerts, duration_seconds, details_json)
            VALUES
              (:run_id, :run_date, :v, :status, :score,
               :n_emp, :n_p, :n_w, :cost, :n_alerts, :dur, CAST(:details AS JSONB))
        """), {
            "run_id": run_id, "run_date": run_date, "v": version,
            "status": status, "score": dq_score,
            "n_emp": kpis["n_employees"], "n_p": kpis["n_prime"],
            "n_w": kpis["n_wellness"],     "cost": kpis["cost_eur"],
            "n_alerts": kpis["n_alerts"],  "dur": duration_s,
            "details": json.dumps(details, default=str),
        })

    # 2. Fichier JSON (audit trail)
    path = MONITORING_DIR / f"monitoring_{run_id}.json"
    path.write_text(json.dumps({
        "run_id":   run_id,
        "run_date": run_date.isoformat(),
        "version":  version,
        "status":   status,
        "dq_score": dq_score,
        "duration_seconds": duration_s,
        "kpis":     kpis,
        "details":  details,
    }, indent=2, default=str), encoding="utf-8")

    log.info(f"Monitoring enregistré : BDD + {path.name}")


def finalize_run(run_id: str, duration_s: float = 0.0) -> None:
    """UPDATE de la ligne monitoring.pipeline_runs avec les KPI finaux.

    Relit les résultats depuis raw.rewards (requête paramétrée) et met à
    jour la ligne créée par la tâche data_quality.
    """
    eng = get_engine()
    df = pd.read_sql(
        text("""
            SELECT eligible_prime, prime_amount, eligible_wellness, geo_alert_prime
            FROM raw.rewards WHERE run_id = :run_id
        """),
        eng,
        params={"run_id": run_id},
    )

    if df.empty:
        log.warning(f"Aucun reward pour {run_id} — finalize no-op")
        return

    kpis = {
        "n_employees": len(df),
        "n_prime":     int(df["eligible_prime"].fillna(False).sum()),
        "n_wellness":  int(df["eligible_wellness"].fillna(False).sum()),
        "cost_eur":    round(float(df["prime_amount"].fillna(0).sum()), 2),
        "n_alerts":    int(df["geo_alert_prime"].fillna(False).sum()),
    }

    with connection() as conn:
        conn.execute(text("""
            UPDATE monitoring.pipeline_runs SET
                status           = 'SUCCESS',
                n_employees      = :n_emp,
                n_prime          = :n_p,
                n_wellness       = :n_w,
                total_cost_eur   = :cost,
                n_geo_alerts     = :n_alerts,
                duration_seconds = :dur
            WHERE run_id = :run_id
        """), {"run_id": run_id,
               "n_emp": kpis["n_employees"], "n_p": kpis["n_prime"],
               "n_w":   kpis["n_wellness"],  "cost": kpis["cost_eur"],
               "n_alerts": kpis["n_alerts"], "dur": duration_s})

    log.info(f"Monitoring finalisé pour run_id={run_id} : {kpis}")


if __name__ == "__main__":
    import argparse
    import sys
    logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(asctime)s | %(levelname)-8s | %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["finalize"],
                        help="Seule 'finalize' est exposée en CLI (le start est géré par DQ)")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--duration", type=float, default=0.0)
    args = parser.parse_args()
    try:
        if args.command == "finalize":
            finalize_run(args.run_id, args.duration)
        sys.exit(0)
    except Exception as e:
        log.critical(f"monitoring KO : {e}", exc_info=True)
        sys.exit(1)
