"""Tests unitaires pour pipeline.data_quality_ge — scoring métier pondéré."""
import pandas as pd
from unittest.mock import patch

from pipeline.data_quality_ge import run_business_checks
from config import DQ_THRESHOLD


def _mock_read_sql(sql, eng):
    """Mock de pd.read_sql qui retourne selon le contenu SQL."""
    sql_str = str(sql)
    if "raw.employees" in sql_str:
        return pd.DataFrame([
            {"employee_id": "E001", "gross_salary": 50_000,
             "transport_mode": "Vélo/Trottinette/Autres",
             "hire_date": "2020-01-01", "geo_status": "OK"},
            {"employee_id": "E002", "gross_salary": 45_000,
             "transport_mode": "Marche/running",
             "hire_date": "2021-06-15", "geo_status": "OK"},
        ])
    if "raw.employee_sports" in sql_str:
        return pd.DataFrame([
            {"employee_id": "E001", "declared_sport": "Running"},
        ])
    if "raw.activities" in sql_str:
        return pd.DataFrame([
            {"activity_id": 1, "employee_id": "E001",
             "distance_m": 5000, "elapsed_seconds": 1800},
        ])
    return pd.DataFrame()


def test_dq_score_parfait_tous_checks_ok():
    """Sur données clean, le score doit être 100/100."""
    with patch("pipeline.data_quality_ge.pd.read_sql", side_effect=_mock_read_sql), \
         patch("pipeline.data_quality_ge.get_engine"):
        result = run_business_checks()

    assert result["score"] == 100
    assert result["blocking"] is False
    assert len(result["checks"]) == 7
    assert all(c["status"] == "OK" for c in result["checks"])


def _mock_read_sql_with_doublons(sql, eng):
    sql_str = str(sql)
    if "raw.employees" in sql_str:
        # Doublon sur employee_id
        return pd.DataFrame([
            {"employee_id": "E001", "gross_salary": 50_000,
             "transport_mode": "Vélo/Trottinette/Autres", "hire_date": "2020-01-01", "geo_status": "OK"},
            {"employee_id": "E001", "gross_salary": 50_000,  # ← doublon
             "transport_mode": "Vélo/Trottinette/Autres", "hire_date": "2020-01-01", "geo_status": "OK"},
        ])
    if "raw.employee_sports" in sql_str:
        return pd.DataFrame(columns=["employee_id", "declared_sport"])
    if "raw.activities" in sql_str:
        return pd.DataFrame(columns=["activity_id", "employee_id", "distance_m", "elapsed_seconds"])
    return pd.DataFrame()


def test_dq_score_bloquant_si_doublons_rh():
    """Doublons employee_id → -20 points + BLOQUANT (80)."""
    with patch("pipeline.data_quality_ge.pd.read_sql", side_effect=_mock_read_sql_with_doublons), \
         patch("pipeline.data_quality_ge.get_engine"):
        result = run_business_checks()

    assert result["score"] == 80
    # 80 n'est pas strictement < DQ_THRESHOLD (80), donc blocking=False
    assert result["blocking"] is False
    # Mais si on baissait le seuil à 85, ce serait bloquant
    check_doublons = next(c for c in result["checks"] if "Doublons" in c["name"])
    assert check_doublons["status"] == "ERREUR"


def _mock_read_sql_negative_distance(sql, eng):
    sql_str = str(sql)
    if "raw.employees" in sql_str:
        return pd.DataFrame([
            {"employee_id": "E001", "gross_salary": 50_000,
             "transport_mode": "Vélo/Trottinette/Autres", "hire_date": "2020-01-01", "geo_status": "OK"},
        ])
    if "raw.employee_sports" in sql_str:
        return pd.DataFrame(columns=["employee_id", "declared_sport"])
    if "raw.activities" in sql_str:
        return pd.DataFrame([
            {"activity_id": 1, "employee_id": "E001",
             "distance_m": -100, "elapsed_seconds": 1800},  # ← négative
        ])
    return pd.DataFrame()


def test_dq_detecte_distance_negative():
    """Une distance négative doit être détectée (poids 10)."""
    with patch("pipeline.data_quality_ge.pd.read_sql", side_effect=_mock_read_sql_negative_distance), \
         patch("pipeline.data_quality_ge.get_engine"):
        result = run_business_checks()

    check_neg = next(c for c in result["checks"] if "distance negative" in c["name"])
    assert check_neg["status"] == "WARNING"
    assert check_neg["value"] == 1
    assert result["score"] == 90  # 100 - 10


def _mock_read_sql_salaire_invalide(sql, eng):
    sql_str = str(sql)
    if "raw.employees" in sql_str:
        return pd.DataFrame([
            {"employee_id": "E001", "gross_salary": 0,  # ← invalide
             "transport_mode": "Vélo/Trottinette/Autres", "hire_date": "2020-01-01", "geo_status": "OK"},
            {"employee_id": "E002", "gross_salary": -500,  # ← invalide
             "transport_mode": "Marche/running", "hire_date": "2020-01-01", "geo_status": "OK"},
        ])
    if "raw.employee_sports" in sql_str:
        return pd.DataFrame(columns=["employee_id", "declared_sport"])
    if "raw.activities" in sql_str:
        return pd.DataFrame(columns=["activity_id", "employee_id", "distance_m", "elapsed_seconds"])
    return pd.DataFrame()


def test_dq_detecte_salaire_invalide():
    """Salaires <= 0 détectés (poids 20 — ERREUR)."""
    with patch("pipeline.data_quality_ge.pd.read_sql", side_effect=_mock_read_sql_salaire_invalide), \
         patch("pipeline.data_quality_ge.get_engine"):
        result = run_business_checks()

    check_sal = next(c for c in result["checks"] if "Salaires invalides" in c["name"])
    assert check_sal["status"] == "ERREUR"
    assert check_sal["value"] == 2


def test_dq_threshold_limite_inclusive():
    """Score == DQ_THRESHOLD est non-bloquant (blocage = strictement inférieur)."""
    assert DQ_THRESHOLD == 80
    with patch("pipeline.data_quality_ge.pd.read_sql", side_effect=_mock_read_sql_with_doublons), \
         patch("pipeline.data_quality_ge.get_engine"):
        # -20 pts sur doublons → score = 80, égal au seuil
        result = run_business_checks()
    assert result["score"] == 80
    assert not result["blocking"]


def test_dq_threshold_bloquant_strict():
    """Score strictement < DQ_THRESHOLD => blocking=True."""
    def _mock_below_threshold(sql, eng):
        sql_str = str(sql)
        if "raw.employees" in sql_str:
            # Doublons (-20) + salaire invalide (-20) + mode inconnu (-5) = 55
            return pd.DataFrame([
                {"employee_id": "E001", "gross_salary": 0,
                 "transport_mode": "INCONNU", "hire_date": "2020-01-01", "geo_status": "OK"},
                {"employee_id": "E001", "gross_salary": 50_000,
                 "transport_mode": "Marche/running", "hire_date": "2020-01-01", "geo_status": "OK"},
            ])
        if "raw.employee_sports" in sql_str:
            return pd.DataFrame(columns=["employee_id", "declared_sport"])
        if "raw.activities" in sql_str:
            return pd.DataFrame(columns=["activity_id", "employee_id", "distance_m", "elapsed_seconds"])
        return pd.DataFrame()

    with patch("pipeline.data_quality_ge.pd.read_sql", side_effect=_mock_below_threshold), \
         patch("pipeline.data_quality_ge.get_engine"):
        result = run_business_checks()
    assert result["score"] < DQ_THRESHOLD
    assert result["blocking"] is True


def test_score_min_zero():
    """Le score ne peut jamais être négatif, même si tous les checks échouent."""

    def _all_fail(sql, eng):
        sql_str = str(sql)
        if "raw.employees" in sql_str:
            return pd.DataFrame([
                {"employee_id": "E001", "gross_salary": 0,
                 "transport_mode": "INCONNU", "hire_date": "2099-01-01", "geo_status": "ALERTE"},
                {"employee_id": "E001",  # doublon
                 "gross_salary": -1, "transport_mode": "INCONNU",
                 "hire_date": "2099-01-01", "geo_status": "ALERTE"},
            ])
        if "raw.employee_sports" in sql_str:
            return pd.DataFrame([
                {"employee_id": "ORPHAN", "declared_sport": "Running"},
            ])
        if "raw.activities" in sql_str:
            return pd.DataFrame([
                {"activity_id": 1, "employee_id": "E001",
                 "distance_m": -100, "elapsed_seconds": 1800},
            ])
        return pd.DataFrame()

    with patch("pipeline.data_quality_ge.pd.read_sql", side_effect=_all_fail), \
         patch("pipeline.data_quality_ge.get_engine"):
        result = run_business_checks()

    assert result["score"] >= 0
    assert result["blocking"] is True
