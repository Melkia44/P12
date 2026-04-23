"""Notifications Slack : message individuel par activité + récap global.

Si SLACK_WEBHOOK_URL est absent du .env, les messages sont logués en
mode SIMULATION. Aucune erreur : la démo live reste jouable même sans
webhook configuré (utile pour les présentations hors-ligne).
"""
import json
import logging
import urllib.request
import urllib.error
from datetime import datetime

import pandas as pd
from sqlalchemy import text

from config import SLACK_WEBHOOK_URL, PIPELINE_VERSION
from pipeline.db import get_engine
from pipeline.data_quality_ge import fetch_dq_report

log = logging.getLogger("SDS.slack")

EMOJIS_SPORT = {
    "Running": "🏃", "Randonnée": "🥾", "Tennis": "🎾", "Natation": "🏊",
    "Football": "⚽", "Rugby": "🏉", "Badminton": "🏸", "Voile": "⛵",
    "Judo": "🥋", "Boxe": "🥊", "Escalade": "🧗", "Triathlon": "🏅",
    "Équitation": "🐴", "Tennis de table": "🏓", "Basketball": "🏀",
}

ACCROCHES = {
    "Running":   "Tu viens de courir",
    "Randonnée": "Une randonnée de",
    "Tennis":    "Tu viens de jouer au tennis —",
    "Natation":  "Tu viens de nager",
    "Football":  "Super match de foot —",
    "Rugby":     "Belle partie de rugby —",
    "Badminton": "Top session de badminton —",
    "Voile":     "Belle sortie en mer —",
    "Judo":      "Super cours de judo —",
    "Boxe":      "Entraînement de boxe —",
    "Escalade":  "Belle session d'escalade —",
    "Triathlon": "Triathlon accompli —",
    "Équitation": "Belle balade à cheval —",
    "Tennis de table": "Super partie de ping —",
    "Basketball": "Belle partie de basket —",
}


def _post(payload: dict) -> bool:
    """POST JSON au webhook. Mode simulé si SLACK_WEBHOOK_URL vide."""
    if not SLACK_WEBHOOK_URL:
        log.info("[SIMULATION Slack] " + json.dumps(
            payload.get("blocks", [{}])[0].get("text", {}).get("text", "(message)")
        )[:160])
        return True
    try:
        req = urllib.request.Request(
            SLACK_WEBHOOK_URL,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status == 200
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError) as e:
        log.error(f"Slack KO : {e}")
        return False


def _format_duration(seconds: int) -> str:
    h, rem = divmod(seconds, 3600)
    m, _   = divmod(rem, 60)
    return f"{h}h{m:02d}" if h else f"{m} min"


def notify_activity(activity: dict, employee: dict) -> None:
    """Envoie une notification Slack pour une activité."""
    sport   = activity.get("sport_type", "Sport")
    prenom  = employee.get("first_name", "")
    nom     = employee.get("last_name", "")
    bu      = employee.get("business_unit", "")
    dist_km = round((activity.get("distance_m") or 0) / 1000, 1)
    duree   = _format_duration(int(activity.get("elapsed_seconds", 0)))
    emoji   = EMOJIS_SPORT.get(sport, "🏅")
    accroche = ACCROCHES.get(sport, "Activité réalisée —")

    if dist_km > 0:
        core = f"*{accroche}* *{dist_km} km* en *{duree}* !"
    else:
        core = f"*{accroche}* *{duree}* d'effort !"

    texte = f"*{prenom} {nom}* — {core} Quelle énergie ! {emoji} 🔥"
    if activity.get("comment"):
        texte += f"\n_{activity['comment']}_"

    payload = {
        "blocks": [
            {"type": "section", "text": {"type": "mrkdwn", "text": texte}},
            {"type": "context", "elements": [{
                "type": "mrkdwn",
                "text": f"📅 {activity.get('start_date', '')}  |  "
                        f"🏢 {bu}  |  ⚡ *Sport Data Solution*",
            }]},
            {"type": "divider"},
        ]
    }
    _post(payload)


def send_global_recap(summary: dict) -> None:
    """Récap du run — envoyé à chaque exécution."""
    payload = {
        "blocks": [
            {"type": "header", "text": {
                "type": "plain_text",
                "text": "Sport Data Solution — Rapport Récompenses Sportives"
            }},
            {"type": "section", "fields": [
                {"type": "mrkdwn", "text": f"*📅 Date :*\n{summary['run_date']}"},
                {"type": "mrkdwn", "text": f"*👥 Total salariés :*\n{summary['n_total']}"},
            ]},
            {"type": "section", "fields": [
                {"type": "mrkdwn", "text": f"*🚴 Éligibles prime 5 % :*\n{summary['n_prime']} salariés"},
                {"type": "mrkdwn", "text": f"*🧘 Éligibles bien-être :*\n{summary['n_wellness']} salariés"},
            ]},
            {"type": "section", "fields": [
                {"type": "mrkdwn", "text": f"*💰 Coût total primes :*\n{summary['cost_eur']:,.0f} EUR / an"},
                {"type": "mrkdwn", "text": f"*⚠️ Alertes géo (exclues) :*\n{summary['n_alerts']} salariés"},
            ]},
            {"type": "divider"},
            {"type": "context", "elements": [{
                "type": "mrkdwn",
                "text": f"Pipeline v{PIPELINE_VERSION} — "
                        f"Score DQ : {summary['dq_score']}/100 ✅",
            }]},
        ]
    }
    _post(payload)


def notify_run_from_db(run_id: str, n_activities_individual: int = 3) -> None:
    """Récap Slack + N notifications individuelles pour un run donné.

    Tout est relu depuis la base — permet un appel Kestra isolé.
    Requêtes paramétrées pour éviter toute injection.
    """
    eng = get_engine()

    df_run = pd.read_sql(
        text("""
            SELECT r.*, e.first_name, e.last_name, e.business_unit
            FROM raw.rewards r
            JOIN raw.employees e USING (employee_id)
            WHERE r.run_id = :run_id
        """),
        eng,
        params={"run_id": run_id},
    )

    if df_run.empty:
        log.warning(f"Aucun reward trouvé pour run_id={run_id} — skip Slack")
        return

    dq = fetch_dq_report(run_id)

    send_global_recap({
        "run_date":   datetime.now().strftime("%Y-%m-%d %H:%M"),
        "n_total":    len(df_run),
        "n_prime":    int(df_run["eligible_prime"].sum()),
        "n_wellness": int(df_run["eligible_wellness"].sum()),
        "cost_eur":   round(float(df_run["prime_amount"].fillna(0).sum()), 2),
        "n_alerts":   int(df_run["geo_alert_prime"].fillna(False).sum()),
        "dq_score":   dq["score"],
    })

    df_act = pd.read_sql(
        text("""
            SELECT a.*, e.first_name, e.last_name, e.business_unit
            FROM raw.activities a
            JOIN raw.employees e USING (employee_id)
            ORDER BY a.start_date DESC
            LIMIT :lim
        """),
        eng,
        params={"lim": int(n_activities_individual)},
    )

    for _, a in df_act.iterrows():
        notify_activity(
            activity={
                "sport_type":      a["sport_type"],
                "distance_m":      a["distance_m"],
                "elapsed_seconds": a["elapsed_seconds"],
                "start_date":      a["start_date"],
                "comment":         a.get("comment"),
            },
            employee={
                "first_name":    a["first_name"],
                "last_name":     a["last_name"],
                "business_unit": a["business_unit"],
            },
        )


if __name__ == "__main__":
    import argparse, sys
    logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(asctime)s | %(levelname)-8s | %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--individual", type=int, default=3,
                        help="Nombre de messages individuels à envoyer")
    args = parser.parse_args()
    try:
        notify_run_from_db(args.run_id, args.individual)
        log.info("slack OK")
        sys.exit(0)
    except Exception as e:
        log.critical(f"slack KO : {e}", exc_info=True)
        sys.exit(1)
