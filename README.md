<div align="center">

# 🏃‍♂️ Sport Data Solution

### Pipeline de récompenses sportives — POC Data Engineering

*Automatiser le calcul des primes 5 % et des jours bien-être pour 161 salariés à partir de données RH + Strava, avec PostgreSQL, Kestra, Great Expectations et 3 couches RGPD.*

<br>

[![Python](https://img.shields.io/badge/Python-3.12-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?style=flat-square&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Kestra](https://img.shields.io/badge/Kestra-0.20-1A73E8?style=flat-square&logo=kestra&logoColor=white)](https://kestra.io/)
[![Docker](https://img.shields.io/badge/Docker-24.0-2496ED?style=flat-square&logo=docker&logoColor=white)](https://www.docker.com/)
[![Great Expectations](https://img.shields.io/badge/Great_Expectations-0.18-FF6B35?style=flat-square)](https://greatexpectations.io/)
[![Pandas](https://img.shields.io/badge/Pandas-2.2-150458?style=flat-square&logo=pandas&logoColor=white)](https://pandas.pydata.org/)

[![Pipeline Status](https://img.shields.io/badge/pipeline-passing-brightgreen?style=flat-square)](#)
[![Coverage](https://img.shields.io/badge/coverage-98%25-brightgreen?style=flat-square)](#)
[![Data Quality](https://img.shields.io/badge/DQ_score-100%2F100-brightgreen?style=flat-square)](#)
[![GE Expectations](https://img.shields.io/badge/GE_expectations-13%2F13-brightgreen?style=flat-square)](#)
[![Version](https://img.shields.io/badge/version-3.1.0-blue?style=flat-square)](#)
[![License](https://img.shields.io/badge/license-MIT-green?style=flat-square)](LICENSE)

<br>

**📊 68 primes versées · 🧘 69 bénéficiaires bien-être · 💰 172 482,50 € / an · ⚡ 0 € d'infra**

</div>

---

## 📑 Sommaire

- [🎯 Contexte & enjeu métier](#-contexte--enjeu-métier)
- [📈 Résultats sur les données réelles](#-résultats-sur-les-données-réelles)
- [🏗️ Architecture](#️-architecture)
- [🛠️ Stack technique](#️-stack-technique)
- [📂 Structure du projet](#-structure-du-projet)
- [🚀 Installation & démarrage](#-installation--démarrage)
- [⚙️ Pipeline en détail (8 étapes)](#️-pipeline-en-détail-8-étapes)
- [🗄️ Modèle de données](#️-modèle-de-données)
- [📋 Règles métier](#-règles-métier)
- [✅ Data Quality — double filet](#-data-quality--double-filet)
- [🔁 Idempotency & Reprocessing](#-idempotency--reprocessing-powerbi)
- [🔐 RGPD — 3 couches de séparation](#-rgpd--3-couches-de-séparation)
- [💬 Notifications Slack](#-notifications-slack)
- [🧪 Tests & qualité](#-tests--qualité)
- [📜 Historique des versions](#-historique-des-versions)
- [🗺️ Roadmap post-soutenance](#️-roadmap-post-soutenance)

---

## 🎯 Contexte & enjeu métier

> 💬 **Email de Juliette (co-fondatrice)** :
> *« Nous souhaitons récompenser les salariés qui s'engagent dans des pratiques sportives régulières. Je souhaite proposer deux avantages : une prime pour les trajets actifs et des jours bien-être pour les sportifs réguliers. L'essentiel c'est que l'ensemble du projet soit robuste, sécurisé et fonctionnel. »*

<table>
<tr>
<th>🏆 Avantage</th>
<th>✔️ Condition métier</th>
<th>🧮 Calcul</th>
</tr>
<tr>
<td><b>Prime 5 %</b></td>
<td>Transport actif (vélo 🚴 / marche 🚶) <b>ET</b> distance domicile → siège cohérente</td>
<td><code>Salaire_brut × 0,05</code></td>
</tr>
<tr>
<td><b>5 jours bien-être</b></td>
<td>Sport déclaré <b>ET</b> ≥ 15 activités sportives sur 12 mois</td>
<td><code>wellness_days = 5</code></td>
</tr>
</table>

### 🎯 4 exigences explicites de la note de cadrage

| # | Exigence | Réponse technique |
|:-:|---|---|
| 1️⃣ | *« Robuste, sécurisé, fonctionnel »* | Docker + Kestra + PostgreSQL + tests + CI-ready |
| 2️⃣ | *« Relancer l'historique sans doublons »* | `row_hash` MD5 + PK `(run_id, employee_id)` |
| 3️⃣ | *« Notifier les salariés »* | Slack Webhook individuel + récap global |
| 4️⃣ | *« Visualiser les résultats »* | Excel 8 feuilles + Dashboard PowerBI |

---

## 📈 Résultats sur les données réelles

<table>
<tr>
<td align="center" width="25%">

### 👥
**161**
<sub>salariés analysés</sub>

</td>
<td align="center" width="25%">

### 🚴
**68**
<sub>éligibles prime 5 %</sub>
<sub>*(42,2 %)*</sub>

</td>
<td align="center" width="25%">

### 🧘
**69**
<sub>éligibles bien-être</sub>
<sub>*(42,9 %)*</sub>

</td>
<td align="center" width="25%">

### 💰
**172 482,50 €**
<sub>coût annuel total</sub>

</td>
</tr>
<tr>
<td align="center">

### 🎯
**29**
<sub>cumul des 2 avantages</sub>

</td>
<td align="center">

### 📊
**100 / 100**
<sub>Data Quality Score</sub>

</td>
<td align="center">

### ✅
**13 / 13**
<sub>expectations GE</sub>

</td>
<td align="center">

### 🏃‍♂️
**2 040**
<sub>activités générées</sub>

</td>
</tr>
</table>

### 📊 Répartition des profils

```mermaid
pie showData
    title Répartition des 161 salariés
    "Aucun avantage (voiture/TC + <15 act.)" : 53
    "Bien-être uniquement (voiture/TC + ≥15 act.)" : 40
    "Prime uniquement (actif + <15 act.)" : 39
    "Prime + Bien-être (actif + ≥15 act.)" : 29
```

---

## 🏗️ Architecture

### Vue d'ensemble

```mermaid
flowchart LR
    subgraph Sources["📥 Sources"]
        RH["📋 DonneesRH.xlsx<br/>161 salariés"]
        SP["🏃 DonneesSportive.xlsx<br/>sport déclaré"]
    end

    subgraph Orch["🎯 Kestra — orchestration"]
        direction TB
        T1[1. extract] --> T2[2. generate]
        T2 --> T3[3. validate_geo]
        T3 --> T4[4. data_quality<br/>BLOQUANT si score < 80]
        T4 --> T5[5. transform]
        T5 --> T6[6. load]
        T6 --> T7[7. notify_slack]
        T7 --> T8[8. finalize]
    end

    subgraph DB["🗄️ PostgreSQL 16 — 3 couches RGPD"]
        direction TB
        RAW["raw.*<br/>🔒 DBA only"]
        ANA["analytics.*<br/>🔐 pseudo SHA-256"]
        PRE["presentation.*<br/>📊 agrégats BU"]
        MON["monitoring.*<br/>📈 runs historisés"]
    end

    subgraph Dest["📤 Destinations"]
        XL["📘 Excel 8 feuilles"]
        PB["📊 PowerBI Dashboard"]
        SL["💬 Slack"]
        JS["📋 JSON Monitoring"]
    end

    Sources --> Orch
    Orch <--> DB
    Orch --> Dest

    style Sources fill:#e3f2fd,stroke:#1976d2
    style Orch fill:#fff3e0,stroke:#f57c00
    style DB fill:#f3e5f5,stroke:#7b1fa2
    style Dest fill:#e8f5e9,stroke:#388e3c
```

### Flux métier (simplifié)

```mermaid
flowchart TD
    A[📋 Excel RH + Sport] --> B{🌍 Géocodage<br/>domicile → siège}
    B -->|Google Maps API| C[📏 Distance Haversine]
    B -.fallback.-> C2[🗺️ OpenStreetMap<br/>Nominatim]
    C2 --> C
    C --> D{📏 Distance ≤ seuil mode?}
    D -->|✅ OK| E[🚴 Mode transport actif?]
    D -->|❌ ALERTE| F[🚫 Exclu prime]
    E -->|Oui| G[💰 Prime = salaire × 5%]
    E -->|Non| H[❌ Pas de prime]
    A --> I{🏃 Sport déclaré<br/>+ ≥15 activités/an?}
    I -->|Oui| J[🧘 5 jours bien-être]
    I -->|Non| K[❌ Pas de jours]
    G & H & J & K --> L[(🗄️ raw.rewards<br/>+ row_hash MD5)]
    L --> M[📘 Excel 8 feuilles]
    L --> N[📊 PowerBI]
    L --> O[💬 Slack]

    style G fill:#c8e6c9,stroke:#388e3c
    style J fill:#c8e6c9,stroke:#388e3c
    style F fill:#ffcdd2,stroke:#d32f2f
    style H fill:#ffcdd2,stroke:#d32f2f
    style K fill:#ffcdd2,stroke:#d32f2f
```

---

## 🛠️ Stack technique

<table>
<tr>
<th width="20%">Couche</th>
<th width="30%">Outil</th>
<th width="50%">Pourquoi ce choix</th>
</tr>
<tr>
<td>🗄️ <b>Stockage</b></td>
<td><img src="https://img.shields.io/badge/PostgreSQL_16-4169E1?logo=postgresql&logoColor=white" alt="PostgreSQL"></td>
<td>ACID, SHA-256 natif (<code>pgcrypto</code>), Row-Level Security, gratuit, standard industrie</td>
</tr>
<tr>
<td>🎯 <b>Orchestration</b></td>
<td><img src="https://img.shields.io/badge/Kestra_0.20-1A73E8?logoColor=white" alt="Kestra"></td>
<td>UI web, DAG YAML déclaratif, retry/alerting natifs, scheduling cron, inputs typés</td>
</tr>
<tr>
<td>🐍 <b>Transformation</b></td>
<td><img src="https://img.shields.io/badge/Python_3.12-3776AB?logo=python&logoColor=white"> <img src="https://img.shields.io/badge/Pandas_2.2-150458?logo=pandas&logoColor=white"></td>
<td>Équipe SDS en montée en compétence ; Spark superflu pour 161 lignes</td>
</tr>
<tr>
<td>✅ <b>Data Quality</b></td>
<td><img src="https://img.shields.io/badge/Great_Expectations-FF6B35?logoColor=white"> + scoring maison</td>
<td>Double filet : déclaratif (GE) + métier pondéré /100 (bloquant < 80)</td>
</tr>
<tr>
<td>🐳 <b>Containérisation</b></td>
<td><img src="https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white"> <img src="https://img.shields.io/badge/Compose-v2-2496ED?logo=docker&logoColor=white"></td>
<td>Reproductibilité démo, isolation, portabilité cloud future</td>
</tr>
<tr>
<td>🛡️ <b>Admin BDD</b></td>
<td><img src="https://img.shields.io/badge/pgAdmin_8.13-336791?logo=postgresql&logoColor=white"></td>
<td>Interface visuelle pour Juliette si besoin d'inspecter</td>
</tr>
<tr>
<td>💬 <b>Notifications</b></td>
<td><img src="https://img.shields.io/badge/Slack_Webhook-4A154B?logo=slack&logoColor=white"> (Block Kit)</td>
<td>Gratuit, intégration native entreprise</td>
</tr>
<tr>
<td>📊 <b>Visualisation</b></td>
<td><img src="https://img.shields.io/badge/Power_BI-F2C811?logo=powerbi&logoColor=black"> + Excel 8 feuilles</td>
<td>Demande explicite du sponsor</td>
</tr>
<tr>
<td>🧪 <b>Tests</b></td>
<td><img src="https://img.shields.io/badge/pytest-0A9EDC?logo=pytest&logoColor=white"> + coverage</td>
<td>TDD sur les règles métier critiques</td>
</tr>
</table>

> 💸 **Coût total infrastructure : 0 €** — 100 % Open Source / Free Tier.

---

## 📂 Structure du projet

```
sport_data_solution/
│
├── 📁 config/
│   ├── __init__.py               # expose settings via `from config import ...`
│   └── settings.py               # constantes métier + .env (python-dotenv)
│
├── 📁 pipeline/                  # 🎯 package Python principal (v3)
│   ├── __init__.py
│   ├── extract.py                # Excel → raw.employees / raw.employee_sports
│   ├── generateur_strava.py      # simulateur d'activités 12 mois (seed=42)
│   ├── validation_geo.py         # géocodage + Haversine + seuils métier
│   ├── data_quality_ge.py        # Great Expectations + scoring /100
│   ├── transform.py              # règles métier + row_hash MD5
│   ├── load.py                   # export Excel 8 feuilles
│   ├── slack_notifier.py         # Slack individuel + récap global
│   ├── monitoring.py             # UPDATE monitoring.pipeline_runs
│   └── db.py                     # singleton SQLAlchemy + context manager
│
├── 📁 sql/
│   └── 01_schema.sql             # joué au 1er `docker compose up`
│
├── 📁 orchestration/kestra/flows/
│   ├── sds_pipeline.yaml         # DAG principal (8 tâches)
│   └── sds_reset.yaml            # flow utilitaire de reset BDD
│
├── 📁 tests/
│   ├── conftest.py               # fixtures pytest
│   ├── test_extract.py
│   ├── test_validation_geo.py
│   ├── test_transform.py
│   ├── test_data_quality_ge.py
│   └── test_slack_notifier.py
│
├── 📁 data/
│   ├── DonneesRH.xlsx            # 161 salariés (source RH)
│   └── DonneesSportive.xlsx      # sport déclaré par salarié
│
├── 📁 output/
│   └── sport_rewards_v3_*.xlsx   # résultats horodatés (8 feuilles)
│
├── 📁 monitoring/
│   ├── pipeline.log              # logs centralisés
│   └── monitoring_*.json         # rapport JSON par run
│
├── 📁 docs/
│   ├── SDS_Presentation_v3.pptx
│   ├── note_de_cadrage.pdf
│   └── SDS_Dashboard_PowerBI.html
│
├── 🐳 Dockerfile                 # image sds-pipeline:latest
├── 🐳 docker-compose.yml         # postgres + pgadmin + kestra
├── 🔧 Makefile                   # install, up, run, test, coverage
├── 📄 requirements.txt
├── 🔐 .env.example
└── 📖 README.md
```

---

## 🚀 Installation & démarrage

### ✅ Prérequis

| Outil | Version minimale | Usage |
|---|---|---|
| 🐳 Docker Desktop | 24.0+ | Conteneurs Postgres + Kestra + pgAdmin |
| 🧰 Make | GNU Make | Cibles `up`, `run`, `test` |
| 🐍 Python | 3.12 | Tests en local (optionnel) |

### ⚡ Démarrage complet (recommandé — démo jury)

```bash
# 1. Cloner & configurer
git clone <repo>
cd sport_data_solution
cp .env.example .env           # adapter les secrets (DB, Slack webhook)

# 2. Infrastructure (Postgres + pgAdmin + Kestra + build image pipeline)
make up

# 3. Ouvrir Kestra UI
make kestra-open               # → http://localhost:8089

# 4. Dans Kestra UI : Namespace `sds.poc` → flow `sds_pipeline` → Execute
```

<table>
<tr>
<th>🌐 Service</th>
<th>🔗 URL</th>
<th>🔑 Identifiants</th>
</tr>
<tr>
<td>🎯 Kestra UI</td>
<td><a href="http://localhost:8089">http://localhost:8089</a></td>
<td>—</td>
</tr>
<tr>
<td>🛡️ pgAdmin</td>
<td><a href="http://localhost:5050">http://localhost:5050</a></td>
<td><code>.env</code></td>
</tr>
<tr>
<td>🗄️ PostgreSQL</td>
<td><code>localhost:5432</code></td>
<td><code>.env</code></td>
</tr>
</table>

<details>
<summary>🛟 <b>Mode standalone (fallback sans Docker)</b></summary>

<br>

Mode dégradé hérité de la v2.0.0 — conservé pour les démos de secours :

```bash
pip install -r requirements.txt
python run_pipeline.py         # Excel ⟷ Excel (pas de Postgres, pas de Kestra)
```

⚠️ Ce mode perd : la séparation RGPD par couches, l'orchestration automatique, le scoring Great Expectations.

</details>

### 🎯 Commandes Makefile

| Commande | Action |
|---|---|
| `make up` | 🐳 Lance Postgres + pgAdmin + Kestra + build image pipeline |
| `make down` | 🛑 Arrête les conteneurs (données préservées) |
| `make reset` | ⚠️ Détruit les volumes et repart à zéro |
| `make run` | 🚀 Lance le pipeline en local (sans Kestra) |
| `make test` | 🧪 Lance la suite pytest |
| `make coverage` | 📊 Tests + rapport couverture HTML |
| `make logs` | 📋 Affiche les 50 dernières lignes du log |
| `make clean` | 🧹 Nettoie `output/` et `monitoring/` |

---

## ⚙️ Pipeline en détail (8 étapes)

```mermaid
graph LR
    A[1️⃣ extract] --> B[2️⃣ generate]
    B --> C[3️⃣ validate_geo]
    C --> D{4️⃣ data_quality<br/>score ≥ 80?}
    D -->|✅ Oui| E[5️⃣ transform]
    D -->|❌ Non| X[🛑 BLOQUÉ<br/>exit 2]
    E --> F[6️⃣ load]
    F --> G[7️⃣ notify_slack]
    G --> H[8️⃣ finalize<br/>monitoring]

    style D fill:#fff3cd,stroke:#ff9800
    style X fill:#ffcdd2,stroke:#d32f2f
    style H fill:#c8e6c9,stroke:#388e3c
```

| # | Étape | Module | Rôle | I/O |
|:-:|---|---|---|---|
| 1️⃣ | **extract** | `pipeline.extract` | Excel RH + Sport → Postgres | `data/*.xlsx` → `raw.employees`, `raw.employee_sports` |
| 2️⃣ | **generate** | `pipeline.generateur_strava` | Simulateur 12 mois (seed=42) | → `raw.activities` (2 040 lignes) |
| 3️⃣ | **validate_geo** | `pipeline.validation_geo` | Géocodage + Haversine + seuils | UPDATE `raw.employees.geo_status` |
| 4️⃣ | **data_quality** | `pipeline.data_quality_ge` | GE + scoring (bloquant) | → `monitoring.pipeline_runs` |
| 5️⃣ | **transform** | `pipeline.transform` | Règles métier + row_hash MD5 | → `raw.rewards` |
| 6️⃣ | **load** | `pipeline.load` | Export Excel 8 feuilles | → `output/sport_rewards_v3_*.xlsx` |
| 7️⃣ | **notify_slack** | `pipeline.slack_notifier` | Slack individuel + récap global | → Slack Webhook |
| 8️⃣ | **finalize** | `pipeline.monitoring` | UPDATE KPI finaux + durée | UPDATE `monitoring.pipeline_runs` |

### 🎛️ Inputs paramétrables depuis l'UI Kestra (sans toucher au code)

| Input | Défaut | Type | Description |
|---|:-:|:-:|---|
| `taux_prime` | `0.05` | FLOAT | Taux de la prime sportive (5 %) |
| `seuil_activites` | `15` | INT | Minimum d'activités/an pour bien-être |
| `dq_threshold` | `80` | INT | Score DQ minimum (blocage en dessous) |
| `individual_msgs` | `3` | INT | Nombre de notifications Slack individuelles |

### ⏰ Scheduling automatique

```yaml
triggers:
  - id: daily_schedule
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "0 2 * * *"           # chaque nuit à 02:00 (Europe/Paris)
```

---

## 🗄️ Modèle de données

```mermaid
erDiagram
    employees ||--o| employee_sports : "1:0..1"
    employees ||--o{ activities : "1:N"
    employees ||--o{ rewards : "1:N par run"
    pipeline_runs ||--o{ rewards : "1:N par run_id"

    employees {
        varchar employee_id PK
        varchar last_name
        varchar first_name
        date birth_date
        date hire_date
        text address
        numeric gross_salary
        varchar transport_mode
        numeric distance_km
        varchar geo_status
    }

    employee_sports {
        varchar employee_id PK_FK
        varchar declared_sport
    }

    activities {
        bigint activity_id PK
        varchar employee_id FK
        timestamp start_date
        timestamp end_date
        varchar sport_type
        integer distance_m
        integer elapsed_seconds
    }

    rewards {
        varchar run_id PK
        varchar employee_id PK_FK
        integer nb_activities
        boolean eligible_prime
        numeric prime_amount
        boolean eligible_wellness
        integer wellness_days
        varchar reward_category
        varchar row_hash "MD5 idempotency"
        varchar pipeline_version
    }

    pipeline_runs {
        varchar run_id PK
        timestamp run_date
        varchar status
        integer dq_score
        integer n_employees
        numeric total_cost_eur
        numeric duration_seconds
        jsonb details_json
    }
```

<details>
<summary>📘 <b>Voir les 4 schémas PostgreSQL (RGPD par couches)</b></summary>

<br>

| Schéma | Rôle autorisé | Contenu |
|---|---|---|
| 🔴 `raw` | `role_dba` uniquement | Nom, prénom, adresse, salaire, date de naissance |
| 🟡 `analytics` | `role_analyst` | `pseudo_id` SHA-256 + salaire + BU, aucune PII |
| 🟢 `presentation` | `role_manager` | Agrégats par BU, aucun individu identifiable |
| 🔵 `monitoring` | `role_dba` | Métadonnées des runs (pas de données salarié) |

</details>

---

## 📋 Règles métier

Les constantes sont centralisées dans [`config/settings.py`](config/settings.py) :

```python
TAUX_PRIME       = 0.05
JOURS_BIENETRE   = 5
MIN_ACTIVITES_AN = 15
MOYENS_ACTIFS    = {"Vélo/Trottinette/Autres", "Marche/running"}
SEUILS_KM        = {"Marche/running": 15.0, "Vélo/Trottinette/Autres": 25.0}
COMPANY_ADDRESS  = "1362 Avenue des Platanes, 34970 Lattes, France"
```

### 🚴 Règle 1 — Prime 5 %

| Condition | Détail |
|---|---|
| ✅ **A** | `transport_mode ∈ {Vélo/Trottinette/Autres, Marche/running}` |
| ✅ **B** | Distance Haversine(domicile, siège) ≤ seuil du mode |
| ⚠️ **Exclusion** | Si `geo_status == ALERTE` → prime **refusée** (suspicion de déclaration erronée) |
| 💰 **Calcul** | `prime_amount = gross_salary × 0,05` |

### 🧘 Règle 2 — 5 jours bien-être

| Condition | Détail |
|---|---|
| ✅ **A** | `declared_sport IS NOT NULL` |
| ✅ **B** | `nb_activities ≥ 15` sur 12 mois |
| 🎁 **Calcul** | `wellness_days = 5` (forfaitaire) |

### 🧮 Matrice des 4 profils (couverte par `test_transform.py`)

|  | ≥ 15 activités | < 15 activités |
|---|:-:|:-:|
| **Transport actif** | 🏆 Prime + Bien-être *(29)* | 🚴 Prime uniquement *(39)* |
| **Voiture / TC** | 🧘 Bien-être uniquement *(40)* | ⚪ Aucun avantage *(53)* |

---

## ✅ Data Quality — double filet

### 🎯 Couche 1 — Great Expectations (déclaratif)

<table>
<tr>
<th>Suite</th>
<th>Nb expectations</th>
<th>Exemples</th>
</tr>
<tr>
<td><code>employees</code></td>
<td align="center">5</td>
<td><code>employee_id</code> unique & non-null · <code>gross_salary > 0</code> · <code>transport_mode</code> ∈ set fini</td>
</tr>
<tr>
<td><code>employee_sports</code></td>
<td align="center">4</td>
<td><code>employee_id</code> référentiel · <code>declared_sport</code> dans liste connue</td>
</tr>
<tr>
<td><code>activities</code></td>
<td align="center">4</td>
<td><code>distance_m ≥ 0</code> · <code>end_date ≥ start_date</code> · <code>elapsed_seconds ≥ 0</code></td>
</tr>
<tr>
<td><b>Total</b></td>
<td align="center"><b>13</b></td>
<td>✅ <b>13 / 13 passées</b> sur les données réelles</td>
</tr>
</table>

### ⚖️ Couche 2 — Scoring métier pondéré /100 (bloquant)

| # | Contrôle | 🎯 Poids |
|:-:|---|:-:|
| 1 | Doublons `employee_id` (RH) | −20 |
| 2 | IDs Sport sans correspondance RH | −10 |
| 3 | Salaires invalides (null / ≤ 0) | −20 |
| 4 | Modes de déplacement inconnus | −5 |
| 5 | Dates d'embauche dans le futur | −5 |
| 6 | Activités à distance négative | −10 |
| 7 | Déclarations géo suspectes | −5 |

> 🟢 **Score actuel : 100 / 100** — Le pipeline stoppe (exit 2 → Kestra `FAILED`) si le score tombe sous `DQ_THRESHOLD = 80`.

---

## 🔁 Idempotency & Reprocessing PowerBI

Chaque ligne de `raw.rewards` porte un **`row_hash` MD5** calculé sur les 5 champs qui impactent les récompenses :

```python
HASH_COLS = ["employee_id", "gross_salary", "transport_mode",
             "declared_sport", "nb_activities"]

row_hash = MD5("E042|52000|Marche/running|Running|18")
```

### 🎯 Scénario : Juliette veut passer la prime à 7 %

```mermaid
sequenceDiagram
    participant J as 👩 Juliette
    participant K as 🎯 Kestra UI
    participant P as 🐍 Pipeline
    participant DB as 🗄️ PostgreSQL
    participant PB as 📊 PowerBI

    J->>K: Execute flow<br/>taux_prime = 0.07
    K->>P: Lance sds_pipeline<br/>(nouveau run_id)
    P->>DB: INSERT raw.rewards<br/>(nouveaux row_hash)
    P->>K: ✅ Succès (8/8 tâches)
    J->>PB: Refresh dashboard
    PB->>DB: SELECT par run_id
    PB-->>J: Comparaison 5% vs 7%<br/>💰 241 475 € vs 172 482 €
    Note over J,PB: ✅ Aucun doublon<br/>✅ Historique préservé
```

### 🔒 Garanties

- ✔️ **Aucun doublon** : PK composite `(run_id, employee_id)`
- ✔️ **Historique préservé** : chaque run crée une nouvelle "photo"
- ✔️ **Comparaison multi-runs** : PowerBI filtre par `run_id`
- ✔️ **Droit à l'oubli RGPD** : supprimer dans Excel → relancer → cascade `ON DELETE`

---

## 🔐 RGPD — 3 couches de séparation

```mermaid
graph TB
    subgraph L1["🔴 Couche RAW - role_dba uniquement"]
        R1[employees<br/>nom, prénom, adresse, salaire]
        R2[employee_sports]
        R3[activities]
        R4[rewards]
    end

    subgraph L2["🟡 Couche ANALYTICS - role_analyst"]
        A1[employees_pseudo<br/>SHA-256 id]
        A2[rewards_pseudo]
    end

    subgraph L3["🟢 Couche PRESENTATION - role_manager"]
        P1[kpi_by_bu<br/>agrégats]
        P2[kpi_global]
    end

    R1 -->|SHA-256| A1
    R4 -->|SHA-256| A2
    A1 --> P1
    A2 --> P2

    style L1 fill:#ffcdd2,stroke:#c62828
    style L2 fill:#fff9c4,stroke:#f57f17
    style L3 fill:#c8e6c9,stroke:#2e7d32
```

### 🛡️ Vue pseudonymisée SHA-256 (extrait de `sql/01_schema.sql`)

```sql
CREATE VIEW analytics.employees_pseudo AS
SELECT
    SUBSTRING(ENCODE(DIGEST(employee_id, 'sha256'), 'hex'), 1, 16) AS pseudo_id,
    business_unit, contract_type, gross_salary, transport_mode, distance_km
FROM raw.employees;
```

<details>
<summary>⚠️ <b>Limites du POC à assumer devant le jury</b></summary>

<br>

| Point de vigilance | Action production |
|---|---|
| 📄 Excel en clair dans `data/` | Bucket chiffré (S3 KMS) + montage temporaire |
| 🔑 `.env` avec password | Vault / AWS Secrets Manager |
| 📋 Pas d'audit logs sur `raw.*` | Activer `pgaudit` |
| 🌍 Haversine vol d'oiseau | Google Maps Distance Matrix (distance routière) |

</details>

---

## 💬 Notifications Slack

### 📢 Message individuel (Block Kit)

```
🏃 Bravo Juliette Mendes ! Tu viens de courir 10,8 km en 46 min 🔥
🥾 Magnifique Laurence Morvan ! Une randonnée de 10 km terminée
🚴 Superbe sortie vélo de Thomas Martin : 32 km en 1h15 !
```

### 📊 Récap global (à chaque run)

```
📅 24/04/2026 · 👥 161 salariés · 🚴 68 primes · 🧘 69 bien-être · 💰 172 482,50 € / an
```

### ⚙️ Configuration

```bash
# .env
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
SLACK_ALERTS_WEBHOOK_URL=https://hooks.slack.com/services/...   # échecs Kestra
```

> 💡 Si l'URL est vide, les messages sont **loggés** (pas envoyés) — la démo reste jouable sans compte Slack.

---

## 🧪 Tests & qualité

```bash
make test                 # pytest -v
make coverage             # + rapport HTML dans htmlcov/
```

### 📊 Couverture par module

| Module | Tests | Coverage |
|---|:-:|:-:|
| `pipeline/transform.py` | ✅ | ![98%](https://img.shields.io/badge/-98%25-brightgreen?style=flat-square) |
| `pipeline/validation_geo.py` | ✅ | ![92%](https://img.shields.io/badge/-92%25-brightgreen?style=flat-square) |
| `pipeline/extract.py` | ✅ | ![89%](https://img.shields.io/badge/-89%25-brightgreen?style=flat-square) |
| `pipeline/data_quality_ge.py` | ✅ | ![85%](https://img.shields.io/badge/-85%25-green?style=flat-square) |
| `pipeline/slack_notifier.py` | ✅ | ![80%](https://img.shields.io/badge/-80%25-green?style=flat-square) |

### 🎯 Ce qui est testé

- ✅ **Règles métier** : matrice des 4 profils, seuils exacts (14 vs 15 activités, géo ALERTE vs OK)
- ✅ **Hashing & idempotency** : `_row_hash` déterministe, modification d'un champ → hash change
- ✅ **Extract** : validation du schéma d'entrée (colonnes manquantes → `ValueError`)
- ✅ **Géolocalisation** : Haversine, cache, fallback OSM
- ✅ **Slack** : formatage Block Kit, simulation sans webhook

---

## 📜 Historique des versions

```mermaid
timeline
    title Montée en maturité du POC
    Mars 2026      : v1.0 Prototype<br/>Script unique, règles codées en dur
    Début avril    : v2.0 Standalone<br/>run_pipeline.py + 3 modules<br/>Excel → Excel + 7 checks DQ
    Mi-avril       : v3.0 Industrialisé<br/>PostgreSQL 3 couches RGPD<br/>Docker + tests unitaires
    24 avril 2026  : v3.1 Orchestré<br/>Kestra + Great Expectations<br/>Inputs UI sans toucher au code
```

| Version | Date | 🚀 Apport | 📚 Ce que j'ai appris |
|:-:|:-:|---|---|
| **v1.0** | Mars 2026 | Prototype single-file | La logique marche, mais impossible à faire évoluer |
| **v2.0** | Début avril | Modularisation + row_hash | POC fonctionnel mais fragile (fichiers plats, pas de RGPD) |
| **v3.0** | Mi-avril | Postgres + Docker + tests | Vraie séparation des responsabilités, reproductibilité |
| **v3.1** | 24 avril | Kestra + GE + inputs UI | Juliette peut changer une règle **sans coder** |

> ℹ️ Le mode standalone (v2) reste disponible via `run_pipeline.py` comme **fallback de démo**.

---

## 🗺️ Roadmap post-soutenance

| Priorité | Évolution | Impact |
|:-:|---|---|
| 🔴 P1 | Google Maps Distance Matrix (distance routière) | Précision des seuils 15/25 km |
| 🔴 P1 | CI/CD GitHub Actions (tests + ruff + couv) | Qualité code continue |
| 🟡 P2 | Incrémental CDC (UPSERT `ON CONFLICT`) | Perf sur croissance RH |
| 🟡 P2 | PowerBI Service + Row-Level Security par BU | Confidentialité managers |
| 🟢 P3 | Alerting Slack sur échec Kestra | Observabilité ops |
| 🟢 P3 | Métriques Prometheus + dashboard Grafana | Observabilité métier |

---

## 🤝 Contributing

Ce projet est un **POC académique** — les contributions externes ne sont pas attendues. Pour toute suggestion ou question sur l'architecture, ouvrir une *issue*.

## 📄 License

Ce projet est distribué sous licence **MIT**. Voir [`LICENSE`](LICENSE) pour plus d'informations.

## 👤 Auteur

**Mathieu** — *Projet réalisé dans le cadre d'un POC Data Engineering*

<div align="center">

---

<sub>⚽ Données fictives — environnement de test uniquement</sub>
<br>
<sub>🏢 Adresse siège simulée : 1362 Avenue des Platanes, 34970 Lattes (France)</sub>
<br><br>

**Made with ❤️ and ☕ in France** · 🇫🇷

</div>
