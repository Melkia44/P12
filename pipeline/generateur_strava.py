# ------------------------------------------------------------------
# generateur_strava — Simulateur d'activités sur 12 mois
#
# Génère une simulation d'activités Strava-like pour les salariés
# ayant un sport déclaré dans le référentiel RH. Alimente la table
# raw.activities avec les métadonnées :
#   activity_id, employee_id, start_date, end_date, sport_type,
#   distance_m (nullable), elapsed_seconds, comment
#
# Seed = 42 pour reproductibilité en démo live.
# ------------------------------------------------------------------
import random
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text

from pipeline.db import get_engine
from pipeline.extract import read_employees_with_sport

log = logging.getLogger("SDS.generator")

SEED = 42

# Profils de simulation : (distance_min_m, distance_max_m, vitesse_min_m/s, vitesse_max_m/s)
# Les libellés sont ceux produits par pipeline.extract (après normalisation
# SPORT_NAME_FIXES : la source RH contient "Runing", normalisé en "Running").
PROFILS_SPORT = {
    "Running":         (3_000,  18_000, 2.5,  4.5),
    "Randonnée":       (8_000,  25_000, 1.0,  1.8),
    "Tennis":          (1_000,   4_000, 3.0,  5.0),
    "Natation":        (1_000,   5_000, 1.2,  2.0),
    "Football":        (5_000,  12_000, 2.5,  4.0),
    "Rugby":           (4_000,  10_000, 2.0,  3.5),
    "Badminton":       (1_000,   3_000, 2.0,  4.0),
    "Voile":           (5_000,  30_000, 1.5,  4.0),
    "Judo":            (500,    2_000,  1.5,  3.0),
    "Boxe":            (500,    2_000,  2.0,  4.0),
    "Escalade":        (200,    1_000,  0.3,  0.8),
    "Triathlon":       (20_000, 60_000, 2.0,  4.5),
    "Équitation":      (5_000,  20_000, 1.5,  4.0),
    "Tennis de table": (500,    2_000,  2.0,  4.0),
    "Basketball":      (3_000,   8_000, 2.5,  4.0),
}

# Sports pour lesquels la distance n'a pas de sens métier
SPORTS_SANS_DISTANCE = {"Escalade", "Tennis de table"}

COMMENTAIRES = {
    "Running":    ["Belle sortie matinale !", "Reprise du sport :)", "Interval training", ""],
    "Randonnée":  ["Randonnée de St Guilhem le desert, je vous la conseille c'est top",
                   "Belle vue depuis le sommet", ""],
    "Tennis":     ["Match gagné !", "Entraînement club", ""],
    "Natation":   ["Piscine municipale", ""],
    "Football":   ["Match du dimanche", ""],
    "Rugby":      ["Match de championnat", ""],
    "Badminton":  ["Tournoi local", ""],
    "Voile":      ["Sortie en mer", ""],
    "Judo":       ["Cours dojo", ""],
    "Boxe":       ["Salle de boxe", ""],
    "Escalade":   ["Salle d'escalade", "Voie extérieure", ""],
    "Triathlon":  ["Triathlon S", ""],
    "Équitation": ["Balade à cheval", ""],
    "Tennis de table": ["Tournoi association", ""],
    "Basketball": ["Match 3x3", ""],
}


def generate_activities() -> pd.DataFrame:
    """Génère les activités des salariés sportifs et charge en base."""
    # Seed locale : reproductibilité sans polluer l'état global Python
    random.seed(SEED)
    np.random.seed(SEED)

    df_emp = read_employees_with_sport()
    df_sportifs = df_emp[df_emp["declared_sport"].notna()].copy()

    log.info(f"{len(df_sportifs)} salariés sportifs identifiés")

    activites = []
    id_counter = 1
    now = datetime.now()
    start_window = now - timedelta(days=365)

    for _, sal in df_sportifs.iterrows():
        sport  = sal["declared_sport"]
        profil = PROFILS_SPORT.get(sport, (2_000, 10_000, 2.0, 4.0))
        coms   = COMMENTAIRES.get(sport, [""])

        # Distribution empirique : ~30 % non éligibles (<15 activités),
        # ~70 % éligibles (>=15). Ajustée manuellement pour matcher un
        # split réaliste sur la base RH de 161 salariés.
        _p = np.array([3]*10 + [4]*5 + [5,5,6,6,7,7,7,7,7,7,7,7,7,6,6], dtype=float)
        n = int(np.random.choice(range(5, 35), p=_p/_p.sum()))

        dates_possibles = [start_window + timedelta(days=d) for d in range(366)]
        dates_choisies  = sorted(random.sample(dates_possibles, min(n, 366)))

        for d in dates_choisies:
            vitesse = random.uniform(profil[2], profil[3])

            # Distance vide pour escalade / tennis de table
            if sport in SPORTS_SANS_DISTANCE:
                dist_m = None
                temps_s = random.randint(1800, 5400)      # 30-90 min simulés
            else:
                dist_m  = random.randint(profil[0], profil[1])
                temps_s = int(dist_m / vitesse)

            # Heure réaliste : matin / midi / soir
            heure = random.choice([
                random.randint(6, 9),
                random.randint(12, 13),
                random.randint(17, 20),
            ])
            minute = random.randint(0, 59)
            dt_debut = d.replace(hour=heure, minute=minute, second=0, microsecond=0)
            dt_fin   = dt_debut + timedelta(seconds=temps_s)

            activites.append({
                "activity_id":     id_counter,
                "employee_id":     sal["employee_id"],
                "start_date":      dt_debut,
                "end_date":        dt_fin,
                "sport_type":      sport,
                "distance_m":      dist_m,
                "elapsed_seconds": temps_s,
                "comment":         random.choice(coms) or None,
            })
            id_counter += 1

    df = pd.DataFrame(activites)
    log.info(f"{len(df)} activités générées sur 12 mois")

    # Chargement idempotent
    eng = get_engine()
    with eng.begin() as conn:
        conn.execute(text("TRUNCATE raw.activities RESTART IDENTITY"))
    df.to_sql("activities", eng, schema="raw", if_exists="append", index=False)

    log.info(f"Chargé raw.activities : {len(df)} lignes")
    return df


if __name__ == "__main__":
    import argparse
    import sys
    logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(asctime)s | %(levelname)-8s | %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    try:
        df = generate_activities()
        log.info(f"generate OK — {len(df)} activités")
        sys.exit(0)
    except Exception as e:
        log.critical(f"generate KO : {e}", exc_info=True)
        sys.exit(1)
