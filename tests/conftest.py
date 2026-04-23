"""Fixtures partagées pour la suite de tests pytest."""
import pandas as pd
import pytest


@pytest.fixture
def sample_employee_row():
    """Un employé éligible : vélo + géo OK + salaire 50000."""
    return pd.Series({
        "employee_id": "12345",
        "gross_salary": 50_000.0,
        "transport_mode": "Vélo/Trottinette/Autres",
        "declared_sport": "Running",
        "nb_activities": 20,
        "geo_status": "OK",
    })


@pytest.fixture
def df_employees_mini():
    """Mini DataFrame RH simulé couvrant tous les cas métier."""
    return pd.DataFrame([
        # Éligible prime + bien-être
        {"employee_id": "E001", "gross_salary": 50_000, "transport_mode": "Vélo/Trottinette/Autres",
         "declared_sport": "Running", "nb_activities": 20, "geo_status": "OK", "geo_reason": "OK"},
        # Éligible prime uniquement (pas de sport)
        {"employee_id": "E002", "gross_salary": 40_000, "transport_mode": "Marche/running",
         "declared_sport": None, "nb_activities": 0, "geo_status": "OK", "geo_reason": "OK"},
        # Éligible bien-être uniquement (transport voiture)
        {"employee_id": "E003", "gross_salary": 60_000, "transport_mode": "véhicule thermique/électrique",
         "declared_sport": "Tennis", "nb_activities": 30, "geo_status": "N/A", "geo_reason": "Mode non contrôlé"},
        # Aucun avantage (pas de sport, transport passif)
        {"employee_id": "E004", "gross_salary": 35_000, "transport_mode": "Transports en commun",
         "declared_sport": None, "nb_activities": 0, "geo_status": "N/A", "geo_reason": "Mode non contrôlé"},
        # Transport actif mais géo ALERTE → exclus de la prime
        {"employee_id": "E005", "gross_salary": 45_000, "transport_mode": "Marche/running",
         "declared_sport": "Natation", "nb_activities": 18, "geo_status": "ALERTE",
         "geo_reason": "50 km > 15 km — déclaration suspecte"},
        # Seuil exact bien-être (15 activités)
        {"employee_id": "E006", "gross_salary": 55_000, "transport_mode": "Vélo/Trottinette/Autres",
         "declared_sport": "Football", "nb_activities": 15, "geo_status": "OK", "geo_reason": "OK"},
        # Juste sous le seuil (14 activités) → pas de bien-être
        {"employee_id": "E007", "gross_salary": 42_000, "transport_mode": "Vélo/Trottinette/Autres",
         "declared_sport": "Rugby", "nb_activities": 14, "geo_status": "OK", "geo_reason": "OK"},
    ])


@pytest.fixture
def df_activities_mini():
    """Mini DataFrame d'activités couvrant les cas de validation."""
    return pd.DataFrame([
        {"activity_id": 1, "employee_id": "E001", "sport_type": "Running",
         "distance_m": 5000, "elapsed_seconds": 1800},
        {"activity_id": 2, "employee_id": "E001", "sport_type": "Running",
         "distance_m": 7000, "elapsed_seconds": 2400},
        # Escalade : distance vide — cas licite
        {"activity_id": 3, "employee_id": "E006", "sport_type": "Escalade",
         "distance_m": None, "elapsed_seconds": 3600},
        # Distance négative — cas invalide à détecter
        {"activity_id": 4, "employee_id": "E007", "sport_type": "Running",
         "distance_m": -100, "elapsed_seconds": 1200},
    ])
