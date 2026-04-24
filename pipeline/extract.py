"""
Chargement des deux fichiers Excel sources (RH + Sport) dans PostgreSQL.

Le pipeline part de ces Excel : mapping colonnes FR -> EN, nettoyage types,
déduplication sur employee_id, puis INSERT dans raw.employees et
raw.employee_sports. Idempotent : TRUNCATE avant INSERT.
"""
import re
import logging
import pandas as pd

from config import DATA_DIR
from pipeline.db import get_engine, truncate_raw_tables

log = logging.getLogger("SDS.extract")

# Mapping colonnes Excel (français) -> colonnes DB (anglais)
RH_COLS_MAP = {
    "ID salarié":           "employee_id",
    "Nom":                  "last_name",
    "Prénom":               "first_name",
    "Date de naissance":    "birth_date",
    "Date d'embauche":      "hire_date",
    "Adresse du domicile":  "address",
    "Type de contrat":      "contract_type",
    "BU":                   "business_unit",
    "Salaire brut":         "gross_salary",
    "Moyen de déplacement": "transport_mode",
}

SPORT_COLS_MAP = {
    "ID salarié":         "employee_id",
    "Pratique d'un sport": "declared_sport",
}

# Normalisation des libellés côté source (typos RH) → libellés canoniques
# utilisés dans PROFILS_SPORT, EMOJIS_SPORT, ACCROCHES, etc.
SPORT_NAME_FIXES = {
    "Runing": "Running",
}

# Colonnes attendues pour une validation basique du schéma d'entrée
EXPECTED_RH_SOURCE_COLS = set(RH_COLS_MAP.keys())
EXPECTED_SPORT_SOURCE_COLS = set(SPORT_COLS_MAP.keys())


def _extract_postal_code(addr):
    """Extrait un code postal FR (5 chiffres) d'une adresse."""
    if not isinstance(addr, str):
        return None
    m = re.search(r"\b(\d{5})\b", addr)
    return m.group(1) if m else None


def _check_source_columns(df: pd.DataFrame, expected: set, name: str) -> None:
    """Lève si des colonnes attendues sont absentes (data contract minimal)."""
    missing = expected - set(df.columns)
    if missing:
        raise ValueError(
            f"Fichier {name} incomplet. Colonnes manquantes: {missing}. "
            f"Colonnes trouvées: {set(df.columns)}"
        )
    extra = set(df.columns) - expected
    if extra:
        log.warning(f"Colonnes inattendues dans {name}: {extra}")


def load_sources_to_db() -> dict:
    """Lit les 2 Excel, normalise et charge raw.employees + raw.employee_sports.

    Returns:
        dict avec les volumétries {'employees': N, 'sports': N}
    """
    log.info("Lecture des fichiers Excel sources...")

    df_rh = pd.read_excel(
        DATA_DIR / "DonneesRH.xlsx",
        dtype={"ID salarié": str},
        parse_dates=["Date de naissance", "Date d'embauche"],
    )
    df_sport = pd.read_excel(
        DATA_DIR / "DonneesSportive.xlsx",
        dtype={"ID salarié": str},
    )

    _check_source_columns(df_rh, EXPECTED_RH_SOURCE_COLS, "DonneesRH.xlsx")
    _check_source_columns(df_sport, EXPECTED_SPORT_SOURCE_COLS, "DonneesSportive.xlsx")

    # Renommage vers les noms DB
    df_rh = df_rh.rename(columns=RH_COLS_MAP)
    df_sport = df_sport.rename(columns=SPORT_COLS_MAP)

    # On ne garde que les colonnes cibles (drop les extras éventuels)
    df_rh = df_rh[[c for c in RH_COLS_MAP.values() if c in df_rh.columns]].copy()
    df_sport = df_sport[[c for c in SPORT_COLS_MAP.values() if c in df_sport.columns]].copy()

    # Normalisation des libellés de sport (la source RH contient "Runing")
    df_sport["declared_sport"] = df_sport["declared_sport"].replace(SPORT_NAME_FIXES)

    # Code postal dérivé — utile si le géocodage échoue
    df_rh["postal_code"] = df_rh["address"].apply(_extract_postal_code)

    # Cast des dates — coerce les NaT pour que PostgreSQL accepte
    for date_col in ("birth_date", "hire_date"):
        if date_col in df_rh.columns:
            df_rh[date_col] = pd.to_datetime(df_rh[date_col], errors="coerce")

    # Déduplication — garde-fou sur la source RH
    before = len(df_rh)
    df_rh = df_rh.drop_duplicates(subset=["employee_id"], keep="first")
    if len(df_rh) < before:
        log.warning(f"{before - len(df_rh)} doublons RH supprimés")

    truncate_raw_tables()

    eng = get_engine()
    df_rh.to_sql("employees", eng, schema="raw", if_exists="append", index=False)
    df_sport.to_sql("employee_sports", eng, schema="raw", if_exists="append", index=False)

    log.info(f"Chargé raw.employees       : {len(df_rh)} lignes")
    log.info(f"Chargé raw.employee_sports : {len(df_sport)} lignes")

    return {"employees": len(df_rh), "sports": len(df_sport)}


def read_employees_with_sport() -> pd.DataFrame:
    """Jointure RH + sport déclaré — source canonique pour la suite du pipeline."""
    eng = get_engine()
    sql = """
        SELECT e.*, s.declared_sport
        FROM raw.employees e
        LEFT JOIN raw.employee_sports s USING (employee_id)
    """
    return pd.read_sql(sql, eng)


# CLI — permet `python -m pipeline.extract --run-id XXX`
if __name__ == "__main__":
    import argparse
    import sys
    logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(asctime)s | %(levelname)-8s | %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True,
                        help="Identifiant de run (fourni par Kestra via execution.id)")
    args = parser.parse_args()
    try:
        volumes = load_sources_to_db()
        log.info(f"extract OK — {volumes}")
        sys.exit(0)
    except Exception as e:
        log.critical(f"extract KO : {e}", exc_info=True)
        sys.exit(1)
