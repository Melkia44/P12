"""Tests unitaires pour pipeline.slack_notifier — format durée + payload."""
from unittest.mock import patch

from pipeline.slack_notifier import _format_duration, notify_activity, EMOJIS_SPORT


def test_format_duration_sous_heure():
    assert _format_duration(1800) == "30 min"
    assert _format_duration(60) == "1 min"
    assert _format_duration(0) == "0 min"


def test_format_duration_avec_heures():
    assert _format_duration(3600) == "1h00"
    assert _format_duration(4617) == "1h16"
    assert _format_duration(7200) == "2h00"


def test_notify_activity_mode_simulation(caplog):
    """Sans SLACK_WEBHOOK_URL, la notif doit partir en SIMULATION sans erreur."""
    with patch("pipeline.slack_notifier.SLACK_WEBHOOK_URL", ""):
        activity = {
            "sport_type": "Running",
            "distance_m": 10800,
            "elapsed_seconds": 2760,
            "start_date": "2026-04-22 09:00:00",
            "comment": "Belle session",
        }
        employee = {"first_name": "Juliette", "last_name": "Mendes", "business_unit": "RH"}
        notify_activity(activity, employee)
        # Pas d'exception = succès


def test_notify_activity_sans_distance_escalade():
    """Pour escalade/ping, distance_m peut être None — on log la durée seule."""
    with patch("pipeline.slack_notifier.SLACK_WEBHOOK_URL", ""):
        activity = {
            "sport_type": "Escalade",
            "distance_m": None,
            "elapsed_seconds": 3600,
            "start_date": "2026-04-22 18:00:00",
        }
        employee = {"first_name": "Paul", "last_name": "Dupont", "business_unit": "IT"}
        notify_activity(activity, employee)


def test_emojis_sport_couvre_15_disciplines():
    """Les 15 disciplines simulées doivent toutes avoir un émoji."""
    from pipeline.generateur_strava import PROFILS_SPORT
    for sport in PROFILS_SPORT.keys():
        assert sport in EMOJIS_SPORT, f"Émoji manquant pour : {sport}"


def test_emojis_sont_des_strings():
    for sport, emoji in EMOJIS_SPORT.items():
        assert isinstance(emoji, str)
        assert len(emoji) >= 1
