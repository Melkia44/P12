"""Export Excel multi-feuilles pour consommation PowerBI."""
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from config import OUTPUT_DIR, PIPELINE_VERSION
from pipeline.db import get_engine

log = logging.getLogger("SDS.load")

# Feuilles produites (ordre fixé pour PowerBI) :
# Salariés_Complet, Éligibles_Prime, Éligibles_BienEtre, Alertes_Geo,
# KPI_Synthèse, Activités_Strava, Data_Quality, Historique_Runs.


def export_to_excel(run_id: str, df_rewards=None, dq_report=None) -> Path:
    """Exporte le résultat d'un run dans un Excel horodaté.

    Si `df_rewards` ou `dq_report` sont None (appel Kestra isolé), on les
    relit depuis la base. Sinon on utilise les objets déjà en mémoire.
    """
    if dq_report is None:
        from pipeline.data_quality_ge import fetch_dq_report
        dq_report = fetch_dq_report(run_id)

    eng = get_engine()
    ts  = datetime.now().strftime("%Y%m%d_%H%M%S")
    outpath = OUTPUT_DIR / f"sport_rewards_v3_{ts}.xlsx"

    # Requête paramétrée (évite toute injection via run_id)
    df_full = pd.read_sql(
        text("""
            SELECT e.employee_id, e.last_name, e.first_name, e.business_unit,
                   e.contract_type, e.gross_salary,
                   EXTRACT(YEAR FROM AGE(e.birth_date))::INT AS age,
                   ROUND(EXTRACT(DAY FROM (NOW() - e.hire_date)) / 365.25, 1) AS seniority_years,
                   e.transport_mode, e.distance_km, e.geo_status, e.geo_reason,
                   s.declared_sport,
                   r.nb_activities,
                   r.eligible_prime, r.prime_amount, r.geo_alert_prime,
                   r.eligible_wellness, r.wellness_days, r.reward_category,
                   r.row_hash, r.pipeline_version, r.created_at AS pipeline_run_date
            FROM raw.employees e
            LEFT JOIN raw.employee_sports s USING (employee_id)
            LEFT JOIN raw.rewards r ON r.employee_id = e.employee_id
                                    AND r.run_id = :run_id
            ORDER BY e.employee_id
        """),
        eng,
        params={"run_id": run_id},
    )

    df_activities = pd.read_sql("SELECT * FROM raw.activities ORDER BY start_date DESC", eng)

    with pd.ExcelWriter(outpath, engine="openpyxl") as w:
        df_full.to_excel(w, sheet_name="Salariés_Complet", index=False)

        (df_full[df_full["eligible_prime"] == True]
            .sort_values("prime_amount", ascending=False)
            .to_excel(w, sheet_name="Éligibles_Prime", index=False))

        (df_full[df_full["eligible_wellness"] == True]
            .sort_values("nb_activities", ascending=False)
            .to_excel(w, sheet_name="Éligibles_BienEtre", index=False))

        (df_full[df_full["geo_status"] == "ALERTE"]
            [["employee_id", "last_name", "first_name", "transport_mode",
              "distance_km", "geo_reason"]]
            .to_excel(w, sheet_name="Alertes_Geo", index=False))

        # Feuille 5 — KPI synthèse, source directe du dashboard
        n_tot   = len(df_full)
        n_prime = int((df_full["eligible_prime"] == True).sum())
        n_be    = int((df_full["eligible_wellness"] == True).sum())
        cost    = float(df_full["prime_amount"].fillna(0).sum())
        kpi = pd.DataFrame([
            ("Total salariés",             n_tot,                    "personnes"),
            ("Éligibles prime 5 %",        n_prime,                  "personnes"),
            ("dont alertes géo exclues",   int((df_full['geo_alert_prime']==True).sum()), "personnes"),
            ("Éligibles bien-être",        n_be,                     "personnes"),
            ("Éligibles DEUX avantages",
             int(((df_full['eligible_prime']==True) & (df_full['eligible_wellness']==True)).sum()),
             "personnes"),
            ("Coût total primes/an",       round(cost, 2),           "EUR"),
            ("Prime moyenne/bénéf.",
             round(cost/n_prime, 2) if n_prime else 0,               "EUR"),
            ("Score Data Quality",         dq_report["score"],       "/100"),
            ("GE expectations passées",
             f"{dq_report['ge_summary']['passed']}/{dq_report['ge_summary']['total']}",
             "checks"),
            ("Version pipeline",           PIPELINE_VERSION,         ""),
            ("Run ID",                     run_id,                   ""),
        ], columns=["Indicateur", "Valeur", "Unité"])
        kpi.to_excel(w, sheet_name="KPI_Synthèse", index=False)

        df_activities.to_excel(w, sheet_name="Activités_Strava", index=False)

        pd.DataFrame(dq_report["checks"]).to_excel(w, sheet_name="Data_Quality", index=False)

        # Feuille 8 — historique des runs (source du reprocessing PowerBI)
        df_hist = pd.read_sql("""
            SELECT r.run_id, r.employee_id, e.last_name, e.first_name,
                   r.eligible_prime, r.prime_amount,
                   r.eligible_wellness, r.wellness_days,
                   r.nb_activities, r.reward_category,
                   r.row_hash, r.pipeline_version, r.created_at
            FROM raw.rewards r
            JOIN raw.employees e USING (employee_id)
            ORDER BY r.created_at DESC, r.employee_id
        """, eng)
        df_hist.to_excel(w, sheet_name="Historique_Runs", index=False)

    log.info(f"Export Excel : {outpath.name}")
    return outpath


if __name__ == "__main__":
    import argparse, sys
    logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(asctime)s | %(levelname)-8s | %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    try:
        path = export_to_excel(args.run_id)
        log.info(f"load OK — {path}")
        sys.exit(0)
    except Exception as e:
        log.critical(f"load KO : {e}", exc_info=True)
        sys.exit(1)
