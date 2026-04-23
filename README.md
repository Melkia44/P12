# Sport Data Solution — Pipeline Data Engineering (v3.1.0)

> **POC Data Engineering** · Pipeline de bout en bout automatisant un système de récompenses sportives pour les salariés.
>
> **Stack** : Python 3.12 · PostgreSQL 16 · **Kestra** · Docker · Google Maps API · Great Expectations · Slack Block Kit · PowerBI

[![Pipeline](https://img.shields.io/badge/pipeline-v3.1.0-065A82)]()
[![Orchestration](https://img.shields.io/badge/orchestration-Kestra-1C7293)]()
[![DQ Score](https://img.shields.io/badge/DQ-100%2F100-22C55E)]()
[![RGPD](https://img.shields.io/badge/RGPD-3%20couches-8B5CF6)]()

---

## Nouveautés v3.1.0

L'orchestration est passée d'un simple `python run_pipeline.py` à un **flow Kestra** avec UI web et paramètres exposés. Concrètement :

- **UI Kestra** sur `http://localhost:8089` — lancement manuel, planification quotidienne, historique des runs, logs par tâche
- **Paramètres métier modifiables depuis l'UI** (le killer feature pour Juliette) :
  - `taux_prime` (défaut 0.05)
  - `seuil_activites` (défaut 15)
  - `dq_threshold` (défaut 80)
  - `individual_msgs` (défaut 3) — nombre de notifs Slack individuelles par run
- **Retries automatiques** sur les tâches susceptibles d'échec transient (Google Maps : 3 tentatives, 30s d'intervalle)
- **Isolation par tâche** : chaque étape tourne dans un conteneur Docker éphémère de l'image `sds-pipeline:latest`
- **Schedule** : exécution automatique chaque nuit à 02:00 Europe/Paris
- **Rétrocompatibilité** : `make run` fonctionne toujours pour la démo locale sans Kestra

---

## Architecture

```
        Sources                     KESTRA (orchestrateur)                   Destinations
  ──────────────────     ────────────────────────────────────────         ─────────────────
  DonneesRH.xlsx     ┐        ┌──────────────────────────────────┐
  DonneesSportive    ├──► ┌──►│ 1. extract    (Excel → raw.*)    │
  .env + API keys    ┘    │   │ 2. generate   (Strava-like 12m)  │
                          │   │ 3. validate_geo (Google Maps)    │       PostgreSQL
                          │   │ 4. data_quality (GE + scoring)   │──►    (source de vérité)
                          │   │ 5. transform  (règles métier)    │
                          │   │ 6. load       (Excel 8 feuilles) │──►    PowerBI Desktop
                          │   │ 7. notify     (Slack Block Kit)  │──►    Slack
                          │   │ 8. finalize   (KPIs + durée)     │──►    Monitoring JSON
                          │   └──────────────────────────────────┘
                          │        │                  │
                          │    Inputs UI         Retries · Schedule
                          │   (taux_prime,       (Google Maps × 3,
                          │    seuil, DQ)         cron 02:00)
                          │
                          └── {{ execution.id }} = run_id unifié partagé entre toutes les tâches
```

Chaque tâche Kestra tourne dans un conteneur Docker **éphémère** de l'image `sds-pipeline:latest`. Les tâches communiquent via PostgreSQL — aucun état n'est partagé en mémoire Python, ce qui permet les retries propres.

### Architecture BDD — 3 couches RGPD

```
raw.*          → DBA uniquement (données brutes : nom, prénom, salaire, adresse)
analytics.*    → analystes (IDs pseudonymisés SHA-256)
presentation.* → managers (agrégats par BU, aucun individu identifiable)
```

L'architecture (schémas + rôles GRANT + vues SHA-256) est définie dans [sql/01_schema.sql](sql/01_schema.sql). **Dans ce POC**, le pipeline opère avec un user DBA et l'Excel PowerBI est généré à partir de `raw.*` pour simplicité de démo ; **en production**, le flux PowerBI lirait `presentation.kpi_by_bu` via un user `role_manager`.

---

## Démarrage rapide

```bash
git clone <votre-repo>/sport-data-solution.git && cd sport-data-solution
cp .env.example .env                           # puis éditez (voir INSTALL.md)

make install                                    # pip install
make kestra-up                                  # builds + démarre postgres + pgadmin + kestra
make kestra-open                                # ouvre http://localhost:8089
```

Depuis l'UI Kestra : namespace `sds.poc` → flow `sds_pipeline` → *Execute* → ajustez les paramètres → valider. Le DAG s'allume vert étape par étape.

**Guide complet de setup** (Docker, Google Maps, Slack, PowerBI) : [INSTALL.md](./INSTALL.md)

---

## Structure du projet

```
sport-data-solution/
│
├── README.md                    ← vous êtes ici
├── INSTALL.md                   ← guide de setup pas-à-pas
├── Dockerfile                   ← image sds-pipeline (utilisée par Kestra)
├── docker-compose.yml           ← PostgreSQL + pgAdmin + Kestra + runner
├── Makefile                     ← commandes utiles (make help)
├── .env.example                 ← template secrets
├── requirements.txt             ← deps Python
│
├── config/
│   └── settings.py              ← CONFIG centrale depuis .env
│
├── sql/
│   └── 01_schema.sql            ← schémas + tables + vues + rôles RGPD
│
├── pipeline/
│   ├── db.py                    ← engine SQLAlchemy + helpers
│   ├── extract.py               ← Excel → raw.*               [CLI: __main__]
│   ├── generateur_strava.py     ← simulateur + end_date       [CLI: __main__]
│   ├── validation_geo.py        ← Google Maps + fallback OSM  [CLI: __main__]
│   ├── data_quality_ge.py       ← GE + scoring + persistance  [CLI: __main__]
│   ├── transform.py             ← règles métier + row_hash    [CLI: __main__]
│   ├── load.py                  ← Excel 8 feuilles            [CLI: __main__]
│   ├── slack_notifier.py        ← Block Kit individuel+récap  [CLI: __main__]
│   └── monitoring.py            ← finalize run + KPIs         [CLI: __main__]
│
├── orchestration/
│   └── kestra/
│       └── flows/
│           ├── sds_pipeline.yaml   ← flow principal (7 tâches + finalize)
│           └── sds_reset.yaml      ← flow de reset (démos)
│
├── data/                        ← sources Excel (incluses)
├── output/                      ← exports Excel horodatés
├── monitoring/                  ← logs + rapports JSON par run
│
└── run_pipeline.py              ← orchestrateur local (rétrocompat sans Kestra)
```

---

## Règles métier (note de cadrage)

| Avantage | Conditions cumulatives | Calcul |
|---|---|---|
| **Prime 5 %** | `transport_mode` ∈ {Vélo/Trottinette, Marche/running} + distance ≤ 15 km (marche) ou 25 km (vélo) vers 1362 Av. des Platanes, 34970 Lattes | `gross_salary × 0.05` |
| **5 jours bien-être** | `declared_sport` renseigné + ≥ 15 activités / an | 5 jours / an |

---

## Le mécanisme clé : reprocessing piloté depuis l'UI

**Demande initiale de Juliette** : *« permettre de relancer l'historique si une source est modifiée (taux ou données entrantes) »*.

### Avant v3.1 — reprocessing nécessitait une modification de code

```bash
# Éditer config/settings.py : TAUX_PRIME = 0.07
# Relancer : make run
```

### Depuis v3.1 — reprocessing piloté depuis Kestra UI

1. Ouvrir http://localhost:8089 → flow `sds_pipeline` → *Execute*
2. Modifier `taux_prime` de `0.05` à `0.07` dans le formulaire
3. Cliquer *Execute*
4. Le DAG tourne avec le nouveau paramètre — chaque tâche est visible dans le DAG visuel
5. Un nouveau `run_id` est créé (= Kestra `execution.id`), une nouvelle ligne par salarié est insérée dans `raw.rewards`, PowerBI lit la feuille `Historique_Runs` qui contient tous les runs

Aucun doublon — l'idempotency est garantie par la clé `(run_id, employee_id)` dans `raw.rewards`. Le `row_hash` MD5 détecte par ailleurs les changements de **données source** (salaire, mode, sport, nb activités). L'historique complet est consultable :
- Dans **Kestra UI** — timeline des runs, durée, état par tâche
- Dans **PostgreSQL** — table `monitoring.pipeline_runs` (SQL interrogeable)
- Dans **PowerBI** — feuille `Historique_Runs` de chaque Excel généré

---

## Data Quality — double couche

### 1. Great Expectations (fluent API, 3 suites)

Trois suites s'exécutent à chaque run sur les données en base :
- `employees` : unicité `employee_id`, salaires > 0, modes de transport dans un `value_set` fermé
- `activities` : unicité `activity_id`, `end_date` non nulle, durée ≤ 24 h, distance ∈ [0 ; 200 km]
- `employee_sports` : unicité `employee_id`

### 2. Scoring métier pondéré (/100, bloquant)

7 contrôles avec pénalités : -20 (critique) à -5 (warning). **Seuil bloquant** configurable via l'input Kestra `dq_threshold` (défaut 80). Si le score passe sous le seuil, la tâche `data_quality` exit 2 → Kestra marque la tâche FAILED → les tâches aval (`transform`, `load`, `notify`) sont automatiquement ignorées.

---

## Notifications Slack

**Message individuel** (Block Kit avec émoji par sport, 15 disciplines) :

> *Bravo Juliette Mendes ! Tu viens de courir 10,8 km en 46 min ! Quelle énergie ! 🏃 🔥*

**Récapitulatif global** à la fin de chaque run :

> *📅 Date · 👥 161 salariés · 🚴 68 prime · 🧘 69 bien-être · 💰 172 482 € / an*

---

## Conformité à la note de cadrage

| Exigence | Statut | Module |
|---|---|---|
| §1 Objectifs : faisabilité, données, impact financier | ✅ | Pipeline complet |
| §2 Prime 5 % + 5 jours bien-être | ✅ | `transform.py` |
| §3.1 Base de données sécurisée | ✅ | PostgreSQL 16 + scram-sha-256 + 3 rôles |
| §3.2 Création des données Strava-like | ✅ | `generateur_strava.py` |
| §3.3 Tests GreatExpectations | ✅ | `data_quality_ge.py` |
| **§3.4 Pipeline ETL + orchestration** | ✅ | **Kestra** + `run_pipeline.py` |
| §3.5 Monitoring volumétrie + état d'exécution | ✅ | Kestra UI + `monitoring.pipeline_runs` |
| §3.6 Restitution PowerBI | ✅ | Export Excel 8 feuilles |
| §4 Liberté outils | ✅ | 100 % open source / free tier |
| §4 Robustesse et sécurité RH | ✅ | RGPD 3 couches (DB) + SHA-256 + roadmap prod |
| §4 Repo GitHub + README | ✅ | Ce fichier |
| §5.1 Validation géo Google Maps | ✅ | `validation_geo.py` |
| §5.1 Seuils 15 km / 25 km | ✅ | `config.SEUILS_KM` |
| §5.2 Métadonnée *Date de fin* | ✅ | Ajoutée v3 |
| §5.3 Messages Slack par activité | ✅ | `slack_notifier.py` |
| Mail Juliette : **reprocessing UI** | ✅ | **Kestra inputs** + `row_hash` |

---

## Roadmap production

| Brique POC | Brique production |
|---|---|
| Fichiers Excel sources | API SIRH (Workday, Sage) |
| Simulateur Strava-like | Strava API réelle (OAuth 2.0) |
| Google Maps à vol d'oiseau | Distance Matrix API (distance routière) |
| Docker local | PostgreSQL managé (RDS, Cloud SQL) avec SSL forcé |
| Kestra standalone (H2) | Kestra cluster (JDBC Postgres backend, Elasticsearch) |
| Webhook Slack | Bot Slack avec OAuth + channels dynamiques |
| PowerBI Desktop | PowerBI Service avec refresh planifié + RLS |

---

## Licence

MIT — voir [LICENSE](./LICENSE).

## Auteur

Projet POC Data Engineering — Formation OpenClassrooms. Données fictives.
