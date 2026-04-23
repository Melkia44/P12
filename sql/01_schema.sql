-- ═══════════════════════════════════════════════════════════════════════
--  Sport Data Solution — Schéma PostgreSQL
--  Architecture 3 couches RGPD :
--    raw          → données brutes (accès DBA uniquement)
--    analytics    → IDs pseudonymisés SHA-256 (accès analystes)
--    presentation → agrégats par BU (accès managers / PowerBI)
--
--  Ce fichier est joué automatiquement au premier `docker compose up`
--  via le montage /docker-entrypoint-initdb.d
-- ═══════════════════════════════════════════════════════════════════════

CREATE EXTENSION IF NOT EXISTS pgcrypto;   -- pour DIGEST(sha256)

-- ═══ Schémas ═══════════════════════════════════════════════════════════
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS presentation;
CREATE SCHEMA IF NOT EXISTS monitoring;

-- ═══ Rôles RGPD ════════════════════════════════════════════════════════
-- Créés si absents (idempotent)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'role_dba') THEN
        CREATE ROLE role_dba;
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'role_analyst') THEN
        CREATE ROLE role_analyst;
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'role_manager') THEN
        CREATE ROLE role_manager;
    END IF;
END $$;

-- Principe du moindre privilège
GRANT USAGE ON SCHEMA raw          TO role_dba;
GRANT USAGE ON SCHEMA analytics    TO role_dba, role_analyst;
GRANT USAGE ON SCHEMA presentation TO role_dba, role_analyst, role_manager;
GRANT USAGE ON SCHEMA monitoring   TO role_dba;

-- ═══ Tables brutes ═════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS raw.employees (
    employee_id       VARCHAR(32) PRIMARY KEY,
    last_name         VARCHAR(128) NOT NULL,
    first_name        VARCHAR(128) NOT NULL,
    birth_date        DATE,
    hire_date         DATE,
    address           TEXT,
    postal_code       VARCHAR(10),
    contract_type     VARCHAR(16),
    business_unit     VARCHAR(64),
    gross_salary      NUMERIC(10,2) CHECK (gross_salary > 0),
    transport_mode    VARCHAR(64),
    distance_km       NUMERIC(6,2),         -- calculée par validation_geo
    geo_status        VARCHAR(16),          -- OK / ALERTE / N/A / INCONNU
    geo_reason        TEXT,
    loaded_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.employee_sports (
    employee_id       VARCHAR(32) PRIMARY KEY
                      REFERENCES raw.employees(employee_id) ON DELETE CASCADE,
    declared_sport    VARCHAR(64),
    loaded_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Métadonnées d'une activité sportive :
-- ID, ID salarié, date début, type, distance, date fin, commentaire
CREATE TABLE IF NOT EXISTS raw.activities (
    activity_id       BIGINT PRIMARY KEY,
    employee_id       VARCHAR(32) REFERENCES raw.employees(employee_id) ON DELETE CASCADE,
    start_date        TIMESTAMP NOT NULL,
    end_date          TIMESTAMP NOT NULL,                     -- ← AJOUTÉ v3
    sport_type        VARCHAR(64),
    distance_m        INTEGER,                                -- vide si non pertinent (escalade, ping...)
    elapsed_seconds   INTEGER CHECK (elapsed_seconds >= 0),
    comment           TEXT,
    loaded_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CHECK (end_date >= start_date)
);

CREATE INDEX IF NOT EXISTS idx_activities_employee ON raw.activities(employee_id);
CREATE INDEX IF NOT EXISTS idx_activities_date     ON raw.activities(start_date);

-- Résultats d'un run (une ligne par salarié par run)
CREATE TABLE IF NOT EXISTS raw.rewards (
    run_id            VARCHAR(32) NOT NULL,
    employee_id       VARCHAR(32) NOT NULL REFERENCES raw.employees(employee_id) ON DELETE CASCADE,
    nb_activities     INTEGER DEFAULT 0,
    eligible_prime    BOOLEAN,
    prime_amount      NUMERIC(10,2) DEFAULT 0,
    eligible_wellness BOOLEAN,
    wellness_days     INTEGER DEFAULT 0,
    reward_category   VARCHAR(32),
    geo_alert_prime   BOOLEAN DEFAULT FALSE,
    row_hash          VARCHAR(64),                            -- MD5 idempotency
    pipeline_version  VARCHAR(16),
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id, employee_id)
);

CREATE INDEX IF NOT EXISTS idx_rewards_hash     ON raw.rewards(row_hash);
CREATE INDEX IF NOT EXISTS idx_rewards_run_date ON raw.rewards(run_id, created_at DESC);

-- ═══ Monitoring des runs ═══════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS monitoring.pipeline_runs (
    run_id            VARCHAR(32) PRIMARY KEY,
    run_date          TIMESTAMP NOT NULL,
    pipeline_version  VARCHAR(16) NOT NULL,
    status            VARCHAR(16),                            -- SUCCESS / FAILURE / PARTIAL
    dq_score          INTEGER,
    n_employees       INTEGER,
    n_prime           INTEGER,
    n_wellness        INTEGER,
    total_cost_eur    NUMERIC(12,2),
    n_geo_alerts      INTEGER,
    duration_seconds  NUMERIC(8,2),
    details_json      JSONB
);

-- ═══ Vue analytique — pseudonymisation SHA-256 ═════════════════════════
-- Principe RGPD : l'analyste ne voit JAMAIS nom/prénom/adresse/salaire brut
-- Il travaille sur un identifiant dérivé irréversible (sans table de correspondance accessible)
CREATE OR REPLACE VIEW analytics.employees_pseudo AS
SELECT
    SUBSTRING(ENCODE(DIGEST(employee_id, 'sha256'), 'hex'), 1, 16) AS pseudo_id,
    business_unit,
    contract_type,
    EXTRACT(YEAR FROM AGE(COALESCE(birth_date, CURRENT_DATE)))::INT   AS age,
    EXTRACT(YEAR FROM AGE(COALESCE(hire_date, CURRENT_DATE)))::INT    AS seniority_years,
    gross_salary,
    transport_mode,
    distance_km,
    geo_status
FROM raw.employees;

CREATE OR REPLACE VIEW analytics.rewards_pseudo AS
SELECT
    SUBSTRING(ENCODE(DIGEST(r.employee_id, 'sha256'), 'hex'), 1, 16) AS pseudo_id,
    r.run_id,
    e.business_unit,
    r.nb_activities,
    r.eligible_prime,
    r.prime_amount,
    r.eligible_wellness,
    r.wellness_days,
    r.reward_category,
    r.row_hash,
    r.pipeline_version,
    r.created_at
FROM raw.rewards r
JOIN raw.employees e ON e.employee_id = r.employee_id;

-- ═══ Vue présentation — agrégats par BU (aucun individu identifiable) ══
CREATE OR REPLACE VIEW presentation.kpi_by_bu AS
SELECT
    e.business_unit,
    COUNT(*)                                             AS n_employees,
    SUM(CASE WHEN r.eligible_prime    THEN 1 ELSE 0 END) AS n_eligible_prime,
    SUM(CASE WHEN r.eligible_wellness THEN 1 ELSE 0 END) AS n_eligible_wellness,
    ROUND(SUM(r.prime_amount)::NUMERIC, 2)               AS total_prime_cost,
    SUM(r.wellness_days)                                 AS total_wellness_days,
    r.run_id
FROM raw.rewards r
JOIN raw.employees e ON e.employee_id = r.employee_id
GROUP BY e.business_unit, r.run_id;

CREATE OR REPLACE VIEW presentation.kpi_global AS
SELECT
    r.run_id,
    MAX(m.run_date)                                      AS run_date,
    MAX(m.pipeline_version)                              AS pipeline_version,
    MAX(m.dq_score)                                      AS dq_score,
    COUNT(*)                                             AS total_employees,
    SUM(CASE WHEN r.eligible_prime THEN 1 ELSE 0 END)    AS n_eligible_prime,
    SUM(CASE WHEN r.eligible_wellness THEN 1 ELSE 0 END) AS n_eligible_wellness,
    SUM(CASE WHEN r.eligible_prime AND r.eligible_wellness THEN 1 ELSE 0 END) AS n_both,
    ROUND(SUM(r.prime_amount)::NUMERIC, 2)               AS total_annual_cost,
    SUM(CASE WHEN r.geo_alert_prime THEN 1 ELSE 0 END)   AS n_geo_alerts
FROM raw.rewards r
LEFT JOIN monitoring.pipeline_runs m USING (run_id)
GROUP BY r.run_id;

-- ═══ Permissions sur les vues ══════════════════════════════════════════
GRANT SELECT ON analytics.employees_pseudo TO role_analyst;
GRANT SELECT ON analytics.rewards_pseudo   TO role_analyst;
GRANT SELECT ON presentation.kpi_by_bu     TO role_manager, role_analyst;
GRANT SELECT ON presentation.kpi_global    TO role_manager, role_analyst;

-- L'analyste ne peut PAS lire raw.* directement (RGPD) :
-- aucun GRANT sur le schéma raw.

COMMENT ON SCHEMA raw IS          'Données brutes — accès DBA uniquement (RGPD)';
COMMENT ON SCHEMA analytics IS    'Données pseudonymisées — accès analystes';
COMMENT ON SCHEMA presentation IS 'Agrégats anonymes — accès managers / PowerBI';
