"""Tests unitaires pour pipeline.transform — règles d'attribution des récompenses.

Ces tests ne dépendent pas de PostgreSQL : on teste les fonctions pures
(_row_hash) et on mocke les interactions BDD quand nécessaire.
"""
import pandas as pd
import pytest

from pipeline.transform import _row_hash, HASH_COLS
from config import TAUX_PRIME, JOURS_BIENETRE, MIN_ACTIVITES_AN, MOYENS_ACTIFS


# ─── row_hash : déterminisme et sensibilité ──────────────────────────────
def test_row_hash_deterministe(sample_employee_row):
    """Un hash doit être stable pour une même entrée."""
    h1 = _row_hash(sample_employee_row)
    h2 = _row_hash(sample_employee_row)
    assert h1 == h2
    assert len(h1) == 32  # MD5 = 32 hex chars


def test_row_hash_change_si_salaire_change(sample_employee_row):
    """Si le salaire change, le hash change (règle d'idempotency)."""
    row2 = sample_employee_row.copy()
    row2["gross_salary"] = 52_000.0
    assert _row_hash(sample_employee_row) != _row_hash(row2)


def test_row_hash_change_si_nb_activites_change(sample_employee_row):
    """Le nombre d'activités influe sur l'éligibilité → doit influencer le hash."""
    row2 = sample_employee_row.copy()
    row2["nb_activities"] = 21
    assert _row_hash(sample_employee_row) != _row_hash(row2)


def test_row_hash_change_si_transport_mode_change(sample_employee_row):
    row2 = sample_employee_row.copy()
    row2["transport_mode"] = "Transports en commun"
    assert _row_hash(sample_employee_row) != _row_hash(row2)


def test_row_hash_stable_sur_champ_non_hashe(sample_employee_row):
    """Un champ hors HASH_COLS ne doit PAS changer le hash."""
    row2 = sample_employee_row.copy()
    row2["last_name"] = "Mendes"  # champ non listé dans HASH_COLS
    assert _row_hash(sample_employee_row) == _row_hash(row2)


def test_row_hash_null_safe():
    """Un champ manquant est remplacé par 'NULL' et ne crash pas."""
    row = pd.Series({
        "employee_id": "X", "gross_salary": None, "transport_mode": None,
        "declared_sport": None, "nb_activities": 0,
    })
    h = _row_hash(row)
    assert len(h) == 32


def test_hash_cols_non_vide():
    """HASH_COLS doit contenir au minimum les colonnes business-critiques."""
    assert "employee_id" in HASH_COLS
    assert "gross_salary" in HASH_COLS
    assert "nb_activities" in HASH_COLS


# ─── Règle Prime 5 % — simulation sans BDD ──────────────────────────────
def _apply_prime_logic(df: pd.DataFrame) -> pd.DataFrame:
    """Reproduit la logique `compute_rewards` sans passer par la BDD.

    Utile pour valider les règles métier unitairement.
    """
    import numpy as np
    geo_ok = df["geo_status"].isin(["OK", "N/A"])
    df = df.copy()
    df["eligible_prime"] = df["transport_mode"].isin(MOYENS_ACTIFS) & geo_ok
    df["geo_alert_prime"] = (df["transport_mode"].isin(MOYENS_ACTIFS) &
                             (df["geo_status"] == "ALERTE"))
    df["prime_amount"] = np.where(
        df["eligible_prime"],
        (df["gross_salary"].astype(float) * TAUX_PRIME).round(2),
        0.0,
    )
    df["eligible_wellness"] = df["declared_sport"].notna() & (df["nb_activities"] >= MIN_ACTIVITES_AN)
    df["wellness_days"] = np.where(df["eligible_wellness"], JOURS_BIENETRE, 0)
    return df


def test_prime_eligible_velo_geo_ok(df_employees_mini):
    out = _apply_prime_logic(df_employees_mini)
    # E001 : vélo + OK
    e001 = out[out["employee_id"] == "E001"].iloc[0]
    assert bool(e001["eligible_prime"]) is True
    assert e001["prime_amount"] == pytest.approx(50_000 * TAUX_PRIME)


def test_prime_eligible_marche(df_employees_mini):
    out = _apply_prime_logic(df_employees_mini)
    e002 = out[out["employee_id"] == "E002"].iloc[0]
    assert bool(e002["eligible_prime"]) is True
    assert e002["prime_amount"] == pytest.approx(40_000 * TAUX_PRIME)


def test_prime_non_eligible_vehicule(df_employees_mini):
    out = _apply_prime_logic(df_employees_mini)
    e003 = out[out["employee_id"] == "E003"].iloc[0]
    assert bool(e003["eligible_prime"]) is False
    assert e003["prime_amount"] == 0.0


def test_prime_non_eligible_transports_commun(df_employees_mini):
    out = _apply_prime_logic(df_employees_mini)
    e004 = out[out["employee_id"] == "E004"].iloc[0]
    assert bool(e004["eligible_prime"]) is False


def test_prime_exclue_si_geo_alerte(df_employees_mini):
    """E005 : marche mais géo ALERTE → pas de prime."""
    out = _apply_prime_logic(df_employees_mini)
    e005 = out[out["employee_id"] == "E005"].iloc[0]
    assert bool(e005["eligible_prime"]) is False
    assert bool(e005["geo_alert_prime"]) is True
    assert e005["prime_amount"] == 0.0


# ─── Règle Bien-être — seuils ───────────────────────────────────────────
def test_bien_etre_seuil_exact_eligible(df_employees_mini):
    """15 activités exactes = éligible."""
    out = _apply_prime_logic(df_employees_mini)
    e006 = out[out["employee_id"] == "E006"].iloc[0]
    assert bool(e006["eligible_wellness"]) is True
    assert e006["wellness_days"] == 5


def test_bien_etre_juste_sous_seuil(df_employees_mini):
    """14 activités = non éligible."""
    out = _apply_prime_logic(df_employees_mini)
    e007 = out[out["employee_id"] == "E007"].iloc[0]
    assert bool(e007["eligible_wellness"]) is False
    assert e007["wellness_days"] == 0


def test_bien_etre_non_eligible_sans_sport_declare(df_employees_mini):
    """Pas de sport déclaré → pas éligible même avec activités."""
    out = _apply_prime_logic(df_employees_mini)
    e002 = out[out["employee_id"] == "E002"].iloc[0]  # pas de sport
    assert bool(e002["eligible_wellness"]) is False


# ─── Intégration : les 4 catégories cumulées ─────────────────────────────
def test_matrice_categories(df_employees_mini):
    out = _apply_prime_logic(df_employees_mini)
    # E001 : éligible prime ET bien-être
    assert bool(out[out["employee_id"] == "E001"].iloc[0]["eligible_prime"])
    assert bool(out[out["employee_id"] == "E001"].iloc[0]["eligible_wellness"])
    # E002 : prime uniquement
    assert bool(out[out["employee_id"] == "E002"].iloc[0]["eligible_prime"])
    assert not bool(out[out["employee_id"] == "E002"].iloc[0]["eligible_wellness"])
    # E003 : bien-être uniquement
    assert not bool(out[out["employee_id"] == "E003"].iloc[0]["eligible_prime"])
    assert bool(out[out["employee_id"] == "E003"].iloc[0]["eligible_wellness"])
    # E004 : aucun avantage
    assert not bool(out[out["employee_id"] == "E004"].iloc[0]["eligible_prime"])
    assert not bool(out[out["employee_id"] == "E004"].iloc[0]["eligible_wellness"])


def test_cout_total_primes(df_employees_mini):
    """Validation du calcul de coût cumulé — dérivé du taux courant."""
    out = _apply_prime_logic(df_employees_mini)
    cout = float(out["prime_amount"].sum())
    # Agrège les salaires éligibles (transport actif + géo OK/N/A)
    eligibles = df_employees_mini[
        df_employees_mini["transport_mode"].isin(MOYENS_ACTIFS)
        & df_employees_mini["geo_status"].isin(["OK", "N/A"])
    ]
    attendu = float(eligibles["gross_salary"].sum()) * TAUX_PRIME
    assert cout == pytest.approx(attendu)


def test_taux_prime_est_5_pourcent():
    """Protection contre une modification accidentelle du taux."""
    assert TAUX_PRIME == 0.05


def test_seuil_bien_etre_est_15():
    assert MIN_ACTIVITES_AN == 15


def test_jours_bien_etre_est_5():
    assert JOURS_BIENETRE == 5
