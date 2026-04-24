"""Calcul des récompenses : prime 5 % + 5 jours bien-être + row_hash."""
import hashlib
import logging

import numpy as np
import pandas as pd
from sqlalchemy import text

from config import TAUX_PRIME, JOURS_BIENETRE, MIN_ACTIVITES_AN, MOYENS_ACTIFS
from pipeline.db import get_engine

log = logging.getLogger("SDS.transform")

# Colonnes qui composent le hash d'idempotency — changer l'une d'elles
# invalide le hash et PowerBI rafraîchit la ligne au prochain run.
HASH_COLS = ["employee_id", "gross_salary", "transport_mode",
             "declared_sport", "nb_activities"]


def _row_hash(row: pd.Series) -> str:
    """MD5 des champs qui influencent l'éligibilité — clé d'idempotency."""
    payload = "|".join(str(row.get(c, "NULL")) for c in HASH_COLS)
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


def compute_rewards(run_id: str, pipeline_version: str) -> pd.DataFrame:
    """Applique les 2 règles métier et persiste dans raw.rewards."""
    eng = get_engine()

    df = pd.read_sql("""
        SELECT e.*, s.declared_sport,
               COALESCE(cnt.n, 0) AS nb_activities
        FROM raw.employees e
        LEFT JOIN raw.employee_sports s USING (employee_id)
        LEFT JOIN (SELECT employee_id, COUNT(*) AS n
                   FROM raw.activities
                   GROUP BY employee_id) cnt USING (employee_id)
    """, eng)

    # Règle 1 — Prime 5 % : transport actif + geo OK (ou N/A si mode non contrôlé)
    geo_ok = df["geo_status"].isin(["OK", "N/A"])
    df["eligible_prime"] = df["transport_mode"].isin(MOYENS_ACTIFS) & geo_ok
    df["geo_alert_prime"] = (df["transport_mode"].isin(MOYENS_ACTIFS) &
                             (df["geo_status"] == "ALERTE"))
    df["prime_amount"] = np.where(
        df["eligible_prime"],
        (df["gross_salary"].astype(float) * TAUX_PRIME).round(2),
        0.0,
    )

    # Règle 2 — 5 jours bien-être : sport déclaré + >= 15 activités / an
    df["sport_declare"] = df["declared_sport"].notna()
    df["eligible_wellness"] = df["sport_declare"] & (df["nb_activities"] >= MIN_ACTIVITES_AN)
    df["wellness_days"] = np.where(df["eligible_wellness"], JOURS_BIENETRE, 0)

    # Catégorie de récompense
    conditions = [
        df["eligible_prime"] & df["eligible_wellness"],
        df["eligible_prime"] & ~df["eligible_wellness"],
        ~df["eligible_prime"] & df["eligible_wellness"],
    ]
    df["reward_category"] = np.select(
        conditions,
        ["Prime + Bien-être", "Prime uniquement", "Bien-être uniquement"],
        default="Aucun avantage",
    )

    # Row hash — clé d'idempotency pour le reprocessing PowerBI
    df["row_hash"] = df.apply(_row_hash, axis=1)
    df["run_id"] = run_id
    df["pipeline_version"] = pipeline_version

    cols_to_save = [
        "run_id", "employee_id", "nb_activities",
        "eligible_prime", "prime_amount",
        "eligible_wellness", "wellness_days",
        "reward_category", "geo_alert_prime",
        "row_hash", "pipeline_version",
    ]
    # Idempotence : purge des lignes du même run avant ré-insertion
    with eng.begin() as conn:
        conn.execute(
            text("DELETE FROM raw.rewards WHERE run_id = :run_id"),
            {"run_id": run_id},
        )
    df[cols_to_save].to_sql("rewards", eng, schema="raw",
                            if_exists="append", index=False)

    log.info(f"Éligibles prime      : {int(df['eligible_prime'].sum())} "
             f"({df['eligible_prime'].mean()*100:.1f} %)")
    log.info(f"Alertes géo exclues  : {int(df['geo_alert_prime'].sum())}")
    log.info(f"Éligibles bien-être  : {int(df['eligible_wellness'].sum())} "
             f"({df['eligible_wellness'].mean()*100:.1f} %)")
    log.info(f"Coût primes / an     : {df['prime_amount'].sum():,.2f} EUR")

    return df


if __name__ == "__main__":
    import argparse
    import sys
    logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(asctime)s | %(levelname)-8s | %(message)s")
    from config import PIPELINE_VERSION
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    try:
        df = compute_rewards(run_id=args.run_id, pipeline_version=PIPELINE_VERSION)
        log.info(f"transform OK — {len(df)} lignes")
        sys.exit(0)
    except Exception as e:
        log.critical(f"transform KO : {e}", exc_info=True)
        sys.exit(1)
