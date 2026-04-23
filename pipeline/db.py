"""Connexion PostgreSQL via SQLAlchemy + helpers idempotents.

Singleton engine, context manager de transaction, ping et truncate.
"""
import logging
from contextlib import contextmanager
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config import DATABASE_URL

log = logging.getLogger("SDS.db")

_engine: Engine | None = None


def get_engine() -> Engine:
    """Singleton engine. Pool réutilisé entre appels."""
    global _engine
    if _engine is None:
        _engine = create_engine(
            DATABASE_URL,
            pool_size=5,
            pool_pre_ping=True,
            future=True,
        )
    return _engine


@contextmanager
def connection():
    """Transaction propre : commit auto / rollback sur exception."""
    eng = get_engine()
    with eng.begin() as conn:
        yield conn


def ping() -> bool:
    """Vérifie que la base est joignable. False si KO (pas d'exception)."""
    try:
        with connection() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as e:
        log.error(f"Connexion BDD impossible : {e}")
        return False


def truncate_raw_tables() -> None:
    """Vide les tables raw avant rechargement complet.

    TODO: quand on passera en mode incrémental, remplacer par un UPSERT
    avec ON CONFLICT DO UPDATE sur employee_id.
    """
    stmt = text("""
        TRUNCATE raw.activities, raw.employee_sports, raw.employees
        RESTART IDENTITY CASCADE
    """)
    with connection() as conn:
        conn.execute(stmt)
    log.info("Tables raw vidées (TRUNCATE CASCADE)")
