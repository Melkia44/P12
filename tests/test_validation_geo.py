"""Tests unitaires pour pipeline.validation_geo — haversine + seuils métier."""
import pandas as pd
import pytest

from pipeline.validation_geo import _haversine_km, _validate_row
from config import SEUILS_KM


# ─── Haversine ──────────────────────────────────────────────────────────
def test_haversine_distance_zero():
    """Distance entre un point et lui-même = 0."""
    pt = (43.5657, 3.9001)  # coords Lattes
    assert _haversine_km(pt, pt) == pytest.approx(0, abs=0.001)


def test_haversine_paris_montpellier():
    """Distance Paris-Montpellier ≈ 600 km."""
    paris = (48.8566, 2.3522)
    montpellier = (43.6108, 3.8767)
    d = _haversine_km(paris, montpellier)
    assert 590 < d < 610


def test_haversine_nord_sud_1_degre_env_111_km():
    """1° de latitude ≈ 111 km."""
    a = (43.0, 3.0)
    b = (44.0, 3.0)
    d = _haversine_km(a, b)
    assert 110 < d < 112


def test_haversine_symetrique():
    a = (48.0, 2.0)
    b = (43.5, 3.9)
    assert _haversine_km(a, b) == pytest.approx(_haversine_km(b, a))


# ─── Validation des seuils métier ───────────────────────────────────────
def test_validate_marche_sous_seuil():
    """Marche à 10 km → OK (seuil 15 km)."""
    row = pd.Series({"transport_mode": "Marche/running", "distance_km": 10.0})
    res = _validate_row(row)
    assert res["geo_status"] == "OK"


def test_validate_marche_seuil_exact():
    """Marche à 15 km exactement → OK (inclusif)."""
    row = pd.Series({"transport_mode": "Marche/running", "distance_km": 15.0})
    res = _validate_row(row)
    assert res["geo_status"] == "OK"


def test_validate_marche_au_dessus_seuil():
    """Marche à 50 km → ALERTE."""
    row = pd.Series({"transport_mode": "Marche/running", "distance_km": 50.0})
    res = _validate_row(row)
    assert res["geo_status"] == "ALERTE"
    assert "50.0 km > 15.0 km" in res["geo_reason"]


def test_validate_velo_sous_seuil():
    """Vélo à 20 km → OK (seuil 25 km)."""
    row = pd.Series({"transport_mode": "Vélo/Trottinette/Autres", "distance_km": 20.0})
    res = _validate_row(row)
    assert res["geo_status"] == "OK"


def test_validate_velo_au_dessus_seuil():
    """Vélo à 30 km → ALERTE."""
    row = pd.Series({"transport_mode": "Vélo/Trottinette/Autres", "distance_km": 30.0})
    res = _validate_row(row)
    assert res["geo_status"] == "ALERTE"


def test_validate_mode_non_controle():
    """Transports en commun → N/A (pas de seuil)."""
    row = pd.Series({"transport_mode": "Transports en commun", "distance_km": 100.0})
    res = _validate_row(row)
    assert res["geo_status"] == "N/A"


def test_validate_distance_inconnue():
    """Distance NaN → INCONNU."""
    row = pd.Series({"transport_mode": "Marche/running", "distance_km": float("nan")})
    res = _validate_row(row)
    assert res["geo_status"] == "INCONNU"


def test_seuils_km_conformes_enonce():
    """Protection contre une modification des seuils métier."""
    assert SEUILS_KM["Marche/running"] == 15.0
    assert SEUILS_KM["Vélo/Trottinette/Autres"] == 25.0
