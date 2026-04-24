"""
Data Quality — Great Expectations + controles metier ponderes.

Strategie en deux couches :
  1. Great Expectations (API fluent) sur 3 suites (employees, activities,
     employee_sports) -> validations declaratives, auditables.
  2. Scoring metier pondere /100 avec 7 controles -> garde-fou runtime.
     Si score < DQ_THRESHOLD (defaut 80), le module exit 2 et Kestra
     stoppe le flow.

Sortie : dict {score, blocking, checks, ge_report, ge_summary}
"""
import json
import logging
from datetime import datetime

import pandas as pd
import great_expectations as gx
from sqlalchemy import text

from config import DQ_THRESHOLD, MOYENS_ACTIFS, PIPELINE_VERSION
from pipeline.db import get_engine, connection

log = logging.getLogger("SDS.dq")


# -- Suites Great Expectations ------------------------------------------
def _build_ge_context():
    """Contexte GX in-memory (ephemeral) -- pas de dossier gx/ persistant."""
    return gx.get_context(mode="ephemeral")


def _check_df_with_ge(df: pd.DataFrame, suite_name: str, expectations: list) -> dict:
    """Applique une liste [(nom_methode, kwargs), ...] sur un DataFrame.

    API fluent GE 0.18 : ctx.sources -> asset -> validator avec appels
    directs des methodes snake_case expect_xxx() sur le validator.
    """
    ctx = _build_ge_context()
    source = ctx.sources.add_pandas(name=f"src_{suite_name}")
    asset = source.add_dataframe_asset(name=f"asset_{suite_name}")
    batch_request = asset.build_batch_request(dataframe=df)
    validator = ctx.get_validator(batch_request=batch_request)

    results = []
    for exp_name, kwargs in expectations:
        method = getattr(validator, exp_name, None)
        if method is None:
            log.warning(f"Methode {exp_name} absente du validator")
            results.append({"type": exp_name, "column": kwargs.get("column"), "success": False})
            continue
        try:
            result = method(**kwargs)
            results.append({
                "type": exp_name,
                "column": kwargs.get("column"),
                "success": result.success,
            })
        except Exception as e:
            log.warning(f"Expectation {exp_name} erreur : {e}")
            results.append({"type": exp_name, "column": kwargs.get("column"), "success": False})

    passed = sum(1 for r in results if r["success"])
    return {
        "suite":    suite_name,
        "total":    len(results),
        "passed":   passed,
        "success":  passed == len(results),
        "details":  results,
    }


def run_ge_suites() -> list:
    """Execute les 3 suites GE sur les donnees en base."""
    eng = get_engine()

    df_emp = pd.read_sql("SELECT * FROM raw.employees", eng)
    emp_exp = [
        ("expect_column_values_to_not_be_null", {"column": "employee_id"}),
        ("expect_column_values_to_be_unique",   {"column": "employee_id"}),
        ("expect_column_values_to_not_be_null", {"column": "gross_salary"}),
        ("expect_column_values_to_be_between",  {"column": "gross_salary", "min_value": 1, "max_value": 500000}),
        ("expect_column_values_to_be_in_set",   {
            "column": "transport_mode",
            "value_set": list(MOYENS_ACTIFS) + ["véhicule thermique/électrique", "Transports en commun"],
        }),
    ]

    df_act = pd.read_sql("SELECT * FROM raw.activities", eng)
    act_exp = [
        ("expect_column_values_to_be_unique",   {"column": "activity_id"}),
        ("expect_column_values_to_not_be_null", {"column": "employee_id"}),
        ("expect_column_values_to_not_be_null", {"column": "start_date"}),
        ("expect_column_values_to_not_be_null", {"column": "end_date"}),
        ("expect_column_values_to_not_be_null", {"column": "sport_type"}),
        ("expect_column_values_to_be_between",  {"column": "elapsed_seconds", "min_value": 0, "max_value": 86400}),
        ("expect_column_values_to_be_between",  {"column": "distance_m", "min_value": 0, "max_value": 200000, "mostly": 1.0}),
    ]

    df_sport = pd.read_sql("SELECT * FROM raw.employee_sports", eng)
    sport_exp = [
        ("expect_column_values_to_be_unique", {"column": "employee_id"}),
    ]

    return [
        _check_df_with_ge(df_emp,   "employees",       emp_exp),
        _check_df_with_ge(df_act,   "activities",      act_exp),
        _check_df_with_ge(df_sport, "employee_sports", sport_exp),
    ]


# -- Controles metier ponderes (/100) -----------------------------------
def run_business_checks() -> dict:
    """Controles metier avec ponderation -- garde-fou runtime du pipeline."""
    eng = get_engine()
    df_rh    = pd.read_sql("SELECT * FROM raw.employees", eng)
    df_sport = pd.read_sql("SELECT * FROM raw.employee_sports", eng)
    df_act   = pd.read_sql("SELECT * FROM raw.activities", eng)

    checks, score = [], 100

    def add(name, value, ok, poids, detail=""):
        nonlocal score
        status = "OK" if ok else ("ERREUR" if poids >= 15 else "WARNING")
        if not ok:
            score -= poids
        checks.append({
            "name": name, "value": value, "status": status,
            "weight": poids, "detail": detail,
        })

    add("Doublons ID salarie (RH)",
        int(df_rh["employee_id"].duplicated().sum()),
        df_rh["employee_id"].is_unique, 20)

    add("IDs Sport sans correspondance RH",
        len(set(df_sport["employee_id"]) - set(df_rh["employee_id"])),
        len(set(df_sport["employee_id"]) - set(df_rh["employee_id"])) == 0, 10)

    invalid_sal = int(((df_rh["gross_salary"].isna()) | (df_rh["gross_salary"] <= 0)).sum())
    add("Salaires invalides (null / <= 0)", invalid_sal, invalid_sal == 0, 20)

    modes_valides = set(MOYENS_ACTIFS) | {"véhicule thermique/électrique", "Transports en commun"}
    bad_modes = int((~df_rh["transport_mode"].isin(modes_valides)).sum())
    add("Modes de deplacement inconnus", bad_modes, bad_modes == 0, 5)

    future = int((pd.to_datetime(df_rh["hire_date"]) > pd.Timestamp.now()).sum())
    add("Dates d'embauche dans le futur", future, future == 0, 5)

    neg_dist = int(((df_act["distance_m"].notna()) & (df_act["distance_m"] < 0)).sum())
    add("Activites distance negative", neg_dist, neg_dist == 0, 10,
        "Controle coherence physique")

    n_alertes = int((df_rh["geo_status"] == "ALERTE").sum())
    add("Declarations geo suspectes", n_alertes, n_alertes == 0, 5,
        "Mode de transport incoherent avec la distance domicile/entreprise")

    return {"score": max(0, score), "checks": checks, "blocking": score < DQ_THRESHOLD}


def run_full_dq() -> dict:
    """Execute GE + checks metier et retourne un rapport consolide."""
    log.info("Execution suites Great Expectations...")
    try:
        ge_report = run_ge_suites()
        ge_total  = sum(s["total"]  for s in ge_report)
        ge_passed = sum(s["passed"] for s in ge_report)
        log.info(f"GE : {ge_passed}/{ge_total} expectations OK")
    except Exception as e:
        log.warning(f"GE indisponible ({e}) -- on continue avec les checks metier")
        ge_report, ge_total, ge_passed = [], 0, 0

    log.info("Execution checks metier ponderes...")
    biz = run_business_checks()
    log.info(f"Score DQ metier : {biz['score']}/100"
             + (" (BLOQUANT)" if biz['blocking'] else ""))

    return {
        "score":     biz["score"],
        "blocking":  biz["blocking"],
        "checks":    biz["checks"],
        "ge_report": ge_report,
        "ge_summary": {"total": ge_total, "passed": ge_passed},
    }


# -- Persistance du rapport DQ ------------------------------------------
def _persist_dq_report(run_id: str, report: dict) -> None:
    """Ecrit le rapport DQ dans monitoring.pipeline_runs en UPSERT."""
    details = {
        "score":      report["score"],
        "blocking":   report["blocking"],
        "checks":     report["checks"],
        "ge_summary": report["ge_summary"],
    }
    with connection() as conn:
        conn.execute(text("""
            INSERT INTO monitoring.pipeline_runs
                  (run_id, run_date, pipeline_version, status, dq_score, details_json)
            VALUES (:run_id, :run_date, :v, 'DQ_CHECKED', :score, CAST(:details AS JSONB))
            ON CONFLICT (run_id) DO UPDATE SET
                dq_score     = EXCLUDED.dq_score,
                details_json = EXCLUDED.details_json,
                status       = 'DQ_CHECKED'
        """), {
            "run_id":  run_id,
            "run_date": datetime.now(),
            "v":       PIPELINE_VERSION,
            "score":   report["score"],
            "details": json.dumps(details, default=str),
        })


def fetch_dq_report(run_id: str) -> dict:
    """Relit le rapport DQ depuis monitoring.pipeline_runs pour un run donne."""
    with connection() as conn:
        row = conn.execute(
            text("SELECT dq_score, details_json FROM monitoring.pipeline_runs WHERE run_id = :r"),
            {"r": run_id},
        ).fetchone()
    if not row:
        return {"score": 0, "ge_summary": {"total": 0, "passed": 0}, "checks": []}
    details = row[1] if isinstance(row[1], dict) else (json.loads(row[1]) if row[1] else {})
    return {
        "score":      row[0] or 0,
        "ge_summary": details.get("ge_summary", {"total": 0, "passed": 0}),
        "checks":     details.get("checks", []),
    }


# CLI : exit 2 si score bloquant -> Kestra marque la tache FAILED
if __name__ == "__main__":
    import argparse
    import sys
    logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(asctime)s | %(levelname)-8s | %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    try:
        report = run_full_dq()
        _persist_dq_report(args.run_id, report)
        if report["blocking"]:
            log.critical(f"DQ BLOQUANT -- score {report['score']}/100 < seuil")
            sys.exit(2)
        log.info(f"DQ OK -- score {report['score']}/100")
        sys.exit(0)
    except Exception as e:
        log.critical(f"DQ KO : {e}", exc_info=True)
        sys.exit(1)
