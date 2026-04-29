"""Configuration centrale — chargée depuis .env via python-dotenv."""
import os
from pathlib import Path
from dotenv import load_dotenv

# Chargement automatique du .env depuis la racine du projet
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

# ─── Chemins ─────────────────────────────────────────────────────────────
DATA_DIR       = BASE_DIR / "data"
OUTPUT_DIR     = Path(
    os.getenv("OUTPUT_DIR") or BASE_DIR / "output"
).expanduser().resolve()
MONITORING_DIR = BASE_DIR / "monitoring"
SQL_DIR        = BASE_DIR / "sql"
for d in (OUTPUT_DIR, MONITORING_DIR):
    d.mkdir(parents=True, exist_ok=True)


def validate_output_dir() -> None:
    """Vérifie qu'OUTPUT_DIR est utilisable AVANT de lancer un export.

    Sur un partage vmhgfs / SMB / cloud, le mount peut sauter silencieusement.
    On échoue tôt avec un message clair plutôt qu'un cryptique IOError 30 s plus tard.
    À appeler explicitement depuis load.py (pas à l'import, sinon les modules
    de monitoring qui doivent tourner en mode dégradé planteraient aussi).
    """
    if not OUTPUT_DIR.exists():
        raise RuntimeError(
            f"❌ OUTPUT_DIR introuvable : {OUTPUT_DIR}\n"
            f"   Vérifie le mount  : `mount | grep hgfs`\n"
            f"   Vérifie le parent : `ls -la {OUTPUT_DIR.parent}`"
        )
    probe = OUTPUT_DIR / ".sds_write_probe"
    try:
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
    except OSError as e:
        raise RuntimeError(
            f"❌ OUTPUT_DIR non inscriptible : {OUTPUT_DIR}\n"
            f"   Erreur : {e}\n"
            f"   Pistes : permissions UID/GID du mount, ou fichier .xlsx "
            f"actuellement ouvert dans Excel côté Windows."
        )

# ─── Base de données ─────────────────────────────────────────────────────
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg://sds:change_me_before_running@localhost:5432/sds_poc",
)

# ─── Clés API externes ───────────────────────────────────────────────────
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "").strip()
SLACK_WEBHOOK_URL   = os.getenv("SLACK_WEBHOOK_URL", "").strip()

# ─── Règles métier ───────────────────────────────────────────────────────
# NOTE: SEUILS_KM sont à vol d'oiseau (Haversine) dans le POC.
#       En prod, basculer sur Google Maps Distance Matrix (distance routière).
COMPANY_ADDRESS   = "1362 Avenue des Platanes, 34970 Lattes, France"
COMPANY_COORDS    = (43.5657, 3.9001)                 # fallback si géocodage KO

TAUX_PRIME        = 0.05
JOURS_BIENETRE    = 5
MIN_ACTIVITES_AN  = 15

MOYENS_ACTIFS     = {"Vélo/Trottinette/Autres", "Marche/running"}
SEUILS_KM         = {
    "Marche/running":          15.0,
    "Vélo/Trottinette/Autres": 25.0,
}

# ─── Qualité & exécution ─────────────────────────────────────────────────
PIPELINE_VERSION  = os.getenv("PIPELINE_VERSION", "3.1.0")


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        return int(raw)
    except ValueError:
        import warnings
        warnings.warn(f"{name}={raw!r} non entier, fallback sur {default}")
        return default


DQ_THRESHOLD      = _int_env("DQ_THRESHOLD", 80)
LOG_LEVEL         = os.getenv("LOG_LEVEL", "INFO")