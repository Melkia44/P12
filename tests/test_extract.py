"""Tests unitaires pour pipeline.extract — data contract et parsing."""
import pandas as pd
import pytest

from pipeline.extract import (
    _extract_postal_code,
    _check_source_columns,
    EXPECTED_RH_SOURCE_COLS,
    RH_COLS_MAP,
    SPORT_COLS_MAP,
)


# ─── Extraction code postal ─────────────────────────────────────────────
def test_extract_cp_standard():
    assert _extract_postal_code("1362 Avenue des Platanes, 34970 Lattes") == "34970"


def test_extract_cp_paris():
    assert _extract_postal_code("10 rue de Rivoli, 75001 Paris") == "75001"


def test_extract_cp_absent():
    assert _extract_postal_code("Sans code postal") is None


def test_extract_cp_none():
    assert _extract_postal_code(None) is None


def test_extract_cp_non_string():
    assert _extract_postal_code(12345) is None


def test_extract_cp_premier_match_retenu():
    """Si plusieurs nombres à 5 chiffres, re.search retourne le premier."""
    assert _extract_postal_code("Boîte 12345 colis 67890") == "12345"


# ─── Data contract (validation du schéma d'entrée) ──────────────────────
def test_check_source_ok():
    """Toutes les colonnes attendues présentes → pas d'exception."""
    df = pd.DataFrame(columns=list(EXPECTED_RH_SOURCE_COLS))
    _check_source_columns(df, EXPECTED_RH_SOURCE_COLS, "DonneesRH.xlsx")


def test_check_source_missing_raises():
    """Colonne obligatoire manquante → ValueError."""
    df = pd.DataFrame(columns=["ID salarié"])  # incomplet
    with pytest.raises(ValueError, match="Colonnes manquantes"):
        _check_source_columns(df, EXPECTED_RH_SOURCE_COLS, "DonneesRH.xlsx")


def test_check_source_extra_warns_but_ok(caplog):
    """Colonnes en trop → warning mais pas d'exception."""
    cols = list(EXPECTED_RH_SOURCE_COLS) + ["ColonneInconnue"]
    df = pd.DataFrame(columns=cols)
    _check_source_columns(df, EXPECTED_RH_SOURCE_COLS, "DonneesRH.xlsx")


# ─── Mapping colonnes ──────────────────────────────────────────────────
def test_rh_mapping_complet():
    """Le mapping RH doit couvrir les 10 colonnes attendues."""
    expected_sources = {
        "ID salarié", "Nom", "Prénom", "Date de naissance", "Date d'embauche",
        "Adresse du domicile", "Type de contrat", "BU", "Salaire brut",
        "Moyen de déplacement",
    }
    assert set(RH_COLS_MAP.keys()) == expected_sources


def test_sport_mapping_complet():
    assert set(SPORT_COLS_MAP.keys()) == {"ID salarié", "Pratique d'un sport"}


def test_rh_mapping_vers_noms_db():
    """Les valeurs (noms DB) doivent matcher les colonnes de raw.employees."""
    expected_db_cols = {
        "employee_id", "last_name", "first_name", "birth_date", "hire_date",
        "address", "contract_type", "business_unit", "gross_salary", "transport_mode",
    }
    assert set(RH_COLS_MAP.values()) == expected_db_cols
