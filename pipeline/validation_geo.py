"""Validation géographique des déclarations de mode de déplacement.

On géocode chaque adresse domicile, on calcule la distance à vol d'oiseau
avec le siège (1362 Av. des Platanes, Lattes), puis on compare au seuil
métier associé au mode de transport déclaré :

    Marche/running          -> <= 15 km
    Vélo/Trottinette/Autres -> <= 25 km

Au-delà, on marque la ligne en ALERTE — la prime sera refusée à cette
personne (suspicion d'erreur de déclaration).

Géocodeur primaire : Google Maps API. Si la clé est absente du .env, on
bascule automatiquement sur Nominatim (OpenStreetMap, gratuit) — la démo
reste jouable sans compte Google. Un cache JSON local évite les
re-géocodages inutiles.
"""
import json
import time
import logging
import pandas as pd
from typing import Optional, Tuple

from sqlalchemy import text

from config import (
    DATA_DIR, COMPANY_ADDRESS, COMPANY_COORDS,
    SEUILS_KM, GOOGLE_MAPS_API_KEY,
)
from pipeline.db import get_engine

log = logging.getLogger("SDS.geo")

CACHE_FILE = DATA_DIR / "geocoding_cache.json"


def _load_cache() -> dict:
    if CACHE_FILE.exists():
        try:
            return json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as e:
            log.warning(f"Cache géocodage illisible ({e}) — repart à vide")
            return {}
    return {}


def _save_cache(cache: dict) -> None:
    CACHE_FILE.parent.mkdir(exist_ok=True)
    CACHE_FILE.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


class GoogleMapsGeocoder:
    """Wrapper Google Maps Geocoding API."""

    def __init__(self, api_key: str):
        import googlemaps
        self.client = googlemaps.Client(key=api_key)

    def geocode(self, address: str) -> Optional[Tuple[float, float]]:
        try:
            results = self.client.geocode(address, region="fr")
            if not results:
                return None
            loc = results[0]["geometry"]["location"]
            return (loc["lat"], loc["lng"])
        except Exception as e:
            log.warning(f"Google Maps erreur pour '{address[:40]}...' : {e}")
            return None


class NominatimGeocoder:
    """Fallback OpenStreetMap — gratuit, rate-limité à ~1 req/s."""

    def __init__(self):
        from geopy.geocoders import Nominatim
        self.client = Nominatim(user_agent="sport_data_solution_v3")

    def geocode(self, address: str) -> Optional[Tuple[float, float]]:
        try:
            time.sleep(1.1)                             # respect du rate limit OSM
            loc = self.client.geocode(address, timeout=10)
            return (loc.latitude, loc.longitude) if loc else None
        except Exception as e:
            log.warning(f"Nominatim erreur : {e}")
            return None


def _haversine_km(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    """Distance à vol d'oiseau en km, formule de Haversine."""
    from math import radians, sin, cos, asin, sqrt
    lat1, lon1 = map(radians, a)
    lat2, lon2 = map(radians, b)
    dlat, dlon = lat2 - lat1, lon2 - lon1
    h = sin(dlat/2)**2 + cos(lat1)*cos(lat2)*sin(dlon/2)**2
    return 2 * 6371 * asin(sqrt(h))


def _pick_geocoder():
    """Choisit Google Maps si clé dispo, sinon Nominatim."""
    if GOOGLE_MAPS_API_KEY:
        log.info("Géocodeur : Google Maps API")
        return GoogleMapsGeocoder(GOOGLE_MAPS_API_KEY), "google"
    log.warning("GOOGLE_MAPS_API_KEY absent -> fallback Nominatim/OSM")
    return NominatimGeocoder(), "nominatim"


def _validate_row(row: pd.Series) -> dict:
    """Applique les seuils métier à une ligne."""
    mode, dist = row["transport_mode"], row.get("distance_km")
    seuil = SEUILS_KM.get(mode)

    if seuil is None:
        return {"geo_status": "N/A", "geo_reason": "Mode non contrôlé"}
    if pd.isna(dist):
        return {"geo_status": "INCONNU", "geo_reason": "Distance non calculable"}
    if dist <= seuil:
        return {"geo_status": "OK",     "geo_reason": f"{dist:.1f} km <= {seuil} km"}
    return {"geo_status": "ALERTE",
            "geo_reason": f"{dist:.1f} km > {seuil} km — déclaration suspecte"}


def validate_geo() -> pd.DataFrame:
    """Géocode, calcule la distance au siège, applique les seuils, persiste.

    Returns:
        DataFrame des salariés enrichi de distance_km, geo_status, geo_reason.
    """
    eng = get_engine()

    df = pd.read_sql("SELECT * FROM raw.employees", eng)
    log.info(f"{len(df)} salariés à géocoder")

    geocoder, name = _pick_geocoder()
    cache = _load_cache()

    distances = []
    for addr in df["address"].fillna(""):
        key = f"{name}::{addr}"
        if key in cache:
            coords = cache[key]
        else:
            coords = geocoder.geocode(addr)
            cache[key] = coords

        if coords:
            distances.append(round(_haversine_km(tuple(coords), COMPANY_COORDS), 2))
        else:
            distances.append(None)

    _save_cache(cache)

    df["distance_km"] = distances

    # Validation règles métier
    validations = df.apply(_validate_row, axis=1, result_type="expand")
    df[["geo_status", "geo_reason"]] = validations

    # Persistance en base via bulk UPDATE (executemany) plutôt qu'une boucle
    # ligne-par-ligne — gain ~x10 sur grosses volumétries.
    updates = df[["employee_id", "distance_km", "geo_status", "geo_reason"]].to_dict(
        orient="records"
    )
    if updates:
        with eng.begin() as conn:
            conn.execute(
                text("""
                    UPDATE raw.employees
                    SET distance_km = :distance_km,
                        geo_status  = :geo_status,
                        geo_reason  = :geo_reason
                    WHERE employee_id = :employee_id
                """),
                updates,
            )

    n_alertes = (df["geo_status"] == "ALERTE").sum()
    log.info(f"Validation géo terminée — {n_alertes} alertes suspectes")
    return df


if __name__ == "__main__":
    import argparse, sys
    logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(asctime)s | %(levelname)-8s | %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    try:
        df = validate_geo()
        n_alerts = int((df["geo_status"] == "ALERTE").sum())
        log.info(f"geo OK — {n_alerts} alertes")
        sys.exit(0)
    except Exception as e:
        log.critical(f"geo KO : {e}", exc_info=True)
        sys.exit(1)
