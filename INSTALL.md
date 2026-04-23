# Guide d'installation — Sport Data Solution v3.1.0

> **Objectif** : faire tourner le pipeline complet avec orchestration Kestra en **~40 minutes** (compter 10 min de plus qu'en v3.0 pour la partie orchestration).

---

## Vue d'ensemble

| # | Étape | Temps | Obligatoire ? |
|---|-------|-------|---------------|
| 1 | Prérequis système (Python, Docker, Git) | 10 min | ✅ |
| 2 | Clone + install Python | 5 min | ✅ |
| 3 | Configuration `.env` | 3 min | ✅ |
| 4 | Clé API Google Maps | 10 min | ⚠️ Recommandé |
| 5 | Webhook Slack | 5 min | ⚠️ Recommandé |
| 6 | Build image + démarrage infra (Postgres + pgAdmin + Kestra) | 5 min | ✅ |
| 7 | **Exécution via Kestra UI** | 2 min | ✅ |
| 8 | Connexion PowerBI | 5 min | ✅ |

> Si vous ne configurez ni Google Maps ni Slack, le pipeline tourne quand même avec Nominatim (géocodage OSM gratuit) et les messages Slack sont simulés dans les logs.

---

## 1. Prérequis

### macOS
```bash
brew install python@3.12 git
brew install --cask docker
```
Lancez ensuite **Docker Desktop** (icône baleine dans la barre de menus).

### Windows 10/11
Installez depuis les sites officiels :
- [Python 3.12](https://www.python.org/downloads/) — cochez *« Add Python to PATH »*
- [Git](https://git-scm.com/download/win)
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) — accepte WSL 2

### Linux (Ubuntu/Debian)
```bash
sudo apt update && sudo apt install -y python3 python3-pip python3-venv git
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker $USER  # puis déconnexion/reconnexion
```

### Vérification
```bash
python --version     # >= 3.10
git --version
docker --version
docker compose version
```

---

## 2. Clone + installation Python

```bash
git clone <URL-du-repo>/sport-data-solution.git
cd sport-data-solution

python -m venv .venv
source .venv/bin/activate            # macOS/Linux
# ou .venv\Scripts\Activate.ps1      # Windows PowerShell

pip install --upgrade pip
pip install -r requirements.txt
```

---

## 3. Configuration `.env`

```bash
cp .env.example .env
```

Éditez `.env` et remplissez au minimum :

```ini
POSTGRES_PASSWORD=un_mot_de_passe_fort
DATABASE_URL=postgresql+psycopg://sds:un_mot_de_passe_fort@localhost:5432/sds_poc

# Pour Kestra (les tâches tournent DANS le réseau Docker, donc host = "postgres")
KESTRA_DATABASE_URL=postgresql+psycopg://sds:un_mot_de_passe_fort@postgres:5432/sds_poc
KESTRA_DATABASE_URL_JDBC=jdbc:postgresql://postgres:5432/sds_poc

# Chemin ABSOLU de votre projet sur le disque (pour le volume output)
PROJECT_PATH=/chemin/absolu/vers/sport-data-solution
```

Le mot de passe doit être **identique** dans `POSTGRES_PASSWORD`, `DATABASE_URL` et `KESTRA_DATABASE_URL`.

> **Trouver votre `PROJECT_PATH`** :
> - macOS/Linux : `pwd` dans le répertoire du projet
> - Windows WSL : `pwd` → `/mnt/c/Users/...`
> - Windows natif : évitez, utilisez WSL

---

## 4. Clé API Google Maps (optionnelle mais recommandée)

La note de cadrage §5.1 cite explicitement Google Maps. Voici comment obtenir une clé gratuite.

1. [console.cloud.google.com](https://console.cloud.google.com) → *Nouveau projet* → `sport-data-solution-poc`
2. *APIs & Services → Library* → activer **Geocoding API**
3. *APIs & Services → Credentials* → *Create API key* → copier la clé `AIzaSy...`
4. *Edit API key* → sous *API restrictions*, restreindre à **Geocoding API** uniquement
5. Configurer la facturation (Billing → Link billing account). Google exige une carte même pour le free tier mais **vous ne serez pas débité** — quota gratuit 200 $/mois (~40 000 appels). Notre POC fait < 200 appels par run, avec un cache local.
6. Coller dans `.env` : `GOOGLE_MAPS_API_KEY=AIzaSy...`

Sans clé, le pipeline bascule automatiquement sur Nominatim/OSM.

---

## 5. Webhook Slack (optionnel)

1. [api.slack.com/apps](https://api.slack.com/apps) → *Create New App → From scratch*
2. Nom `SDS Notifier`, workspace au choix
3. *Incoming Webhooks* → activer → *Add New Webhook to Workspace*
4. Choisir channel `#sport-data-solution` (à créer si besoin)
5. Copier l'URL `https://hooks.slack.com/services/T.../B.../...`
6. Coller dans `.env` : `SLACK_WEBHOOK_URL=https://hooks.slack.com/...`

Sans webhook, les messages sont simulés dans les logs Kestra (aucune erreur).

---

## 6. Build de l'image + démarrage infra

```bash
make kestra-up
```

Cette commande fait tout :
1. Build de l'image `sds-pipeline:latest` (copie du code + installation des deps dans une image Docker) — ~3 min la première fois
2. Téléchargement des images Postgres 16, pgAdmin, Kestra — ~2 min la première fois
3. Démarrage des 3 services
4. Initialisation automatique du schéma SQL (`sql/01_schema.sql` joué au premier démarrage de Postgres)
5. Attente que Postgres soit prêt

Vérifiez :
- http://localhost:5050 → pgAdmin (admin@sds.local / admin)
- http://localhost:8089 → **Kestra** (pas d'authentification en local)
- `make check` → doit afficher *« ✅ Connexion BDD OK »*

---

## 7. Exécution via Kestra UI — la vraie bascule v3.1

### Première exécution

1. Ouvrir http://localhost:8089
2. Menu latéral : *Flows* → namespace `sds.poc` → `sds_pipeline`
3. Vous voyez le DAG avec les 8 tâches : `extract → generate → validate_geo → data_quality → transform → load → notify_slack → finalize_monitoring`
4. Onglet *Execute* (en haut à droite)
5. Formulaire d'inputs avec valeurs par défaut :
   - `taux_prime` = 0.05
   - `seuil_activites` = 15
   - `dq_threshold` = 80
   - `individual_msgs` = 3
6. Cliquer *Execute* en bas
7. Le DAG s'anime : chaque tâche devient bleue (running) puis verte (success)
8. Durée totale : ~30-60 secondes selon la présence de la clé Google Maps
9. Onglet *Gantt* pour voir la timeline d'exécution
10. Onglet *Logs* pour voir la sortie de chaque tâche

### Démo du reprocessing demandé par Juliette

1. Retour sur le flow `sds_pipeline` → *Execute*
2. Changer `taux_prime` de `0.05` à `0.07`
3. Cliquer *Execute*
4. Nouveau run, nouveau DAG, nouveaux montants dans `raw.rewards`
5. PowerBI → *Actualiser* → les données sont à jour, et l'historique des deux runs est préservé dans la feuille `Historique_Runs`

### Schedule automatique

Le flow est configuré pour se déclencher chaque jour à **02:00 Europe/Paris**. Pour le désactiver temporairement : onglet *Triggers* → désactiver `daily_schedule`.

### Exécution locale sans Kestra

```bash
make run    # équivalent à python run_pipeline.py
```

Fonctionne toujours et reste utile pour le debug ou les runs ad-hoc.

---

## 8. Connexion PowerBI

### Option A — Lecture Excel (plus simple pour la démo)

1. Télécharger [PowerBI Desktop](https://www.microsoft.com/fr-fr/download/details.aspx?id=58494) (Windows)
2. *Obtenir les données → Excel*
3. Sélectionner le fichier le plus récent dans `output/sport_rewards_v3_*.xlsx`
4. Charger les 8 feuilles

### Option B — Connexion PostgreSQL (trajectoire prod)

> **Note POC** : dans la configuration actuelle, le pipeline écrit dans `raw.*` et l'Excel est généré à partir de cette couche. L'option B ci-dessous est la trajectoire visée en production (PowerBI lit `presentation.*` via un user `role_manager`). Pour une démo POC, utiliser l'Option A.

1. *Obtenir les données → PostgreSQL*
2. Serveur `localhost`, Base `sds_poc`, user `sds` + mot de passe du `.env`
3. Dans le navigateur, sélectionner uniquement les **vues du schéma `presentation`** (`kpi_by_bu`, `kpi_global`) — jamais `raw.*` (RGPD)

---

## Dépannage

### Kestra ne démarre pas — logs à consulter
```bash
docker compose logs kestra --tail 50
```

### Kestra UI s'ouvre mais les flows n'apparaissent pas
Les flows sont montés en lecture depuis `orchestration/kestra/flows/`. Redémarrer Kestra :
```bash
docker compose restart kestra
```
Les flows YAML sont rechargés au démarrage.

### Une tâche Kestra échoue avec "Cannot connect to postgres"
Vérifier que `KESTRA_DATABASE_URL` dans `.env` utilise bien `postgres` (hostname Docker) et **pas** `localhost` — les tâches tournent dans le réseau Docker `sds_network`, où Postgres est joignable via son nom de service.

### "image sds-pipeline:latest not found" dans les logs Kestra
L'image n'a pas été buildée. Lancer :
```bash
make build
```

### Google Maps REQUEST_DENIED
- API Geocoding bien activée ? (pas seulement Maps JavaScript)
- Facturation liée au projet ? (requis même pour free tier)
- Clé sans restriction IP bloquante

### Slack ne reçoit rien mais pas d'erreur
Webhook URL correcte dans `.env` ? Si vide, les messages vont dans les logs Kestra (chercher `[SIMULATION Slack]`).

### Pipeline bloqué sur "score DQ < seuil"
Comportement attendu — garde-fou de production. Pour passer outre temporairement, baisser `dq_threshold` à 60 dans l'input du flow Kestra. Pour diagnostiquer : consulter la feuille `Data_Quality` du dernier Excel ou la table `monitoring.pipeline_runs`.

---

## Reset complet

Pour repartir totalement de zéro (entre deux démos en soutenance par exemple) :

```bash
make reset                                       # détruit TOUS les volumes
rm -f data/geocoding_cache.json                  # force un re-géocodage
make kestra-up                                   # redémarre tout
```

Ou, plus rapide, depuis Kestra UI : exécuter le flow `sds.poc.sds_reset` qui vide juste les tables `raw.rewards` et `monitoring.pipeline_runs` sans toucher au schéma ni aux RH.

---

## Annexe A — Passage en production

Les sections précédentes couvrent l'installation POC. Cette annexe liste les durcissements à appliquer avant tout déploiement en production. Les valeurs par défaut du projet sont calibrées pour une démo, pas pour la prod.

## 1. Activer l'authentification Kestra

Par défaut (`docker-compose.yml` section `kestra`), la `basic-auth` est désactivée :

```yaml
server:
  basic-auth:
    enabled: false   # désactivé en local
```

En production, activer :

```yaml
server:
  basic-auth:
    enabled: true
    username: ${KESTRA_ADMIN_USER}
    password: ${KESTRA_ADMIN_PASSWORD}
    realm: "Sport Data Solution"
```

Ajouter dans `.env` :

```ini
KESTRA_ADMIN_USER=admin_sds
KESTRA_ADMIN_PASSWORD=<mot_de_passe_fort_32_char_min>
```

Rotation des secrets : tous les 90 jours via un rappel calendaire + mise à jour de `.env` et redémarrage du service Kestra.

## 2. Bascule Kestra H2 → PostgreSQL (mode cluster-ready)

Le backend H2 embarqué tient pour un POC mais limite à un worker unique et ne survit pas à un redémarrage non-propre.

Configuration production :

```yaml
KESTRA_CONFIGURATION: |
  kestra:
    queue:      { type: postgres }
    repository: { type: postgres }
    storage:
      type: local                          # en prod, bascule S3 compatible
      local: { base-path: /app/storage }
  datasources:
    postgres:
      url: jdbc:postgresql://postgres:5432/kestra_prod
      driverClassName: org.postgresql.Driver
      username: kestra
      password: ${KESTRA_DB_PASSWORD}
```

Créer la base `kestra_prod` dédiée :

```sql
CREATE DATABASE kestra_prod OWNER kestra;
GRANT ALL PRIVILEGES ON DATABASE kestra_prod TO kestra;
```

## 3. PostgreSQL : TLS obligatoire + backups

Dans toutes les URL de connexion, ajouter `sslmode=require` :

```
DATABASE_URL=postgresql+psycopg://sds:xxx@host:5432/sds_prod?sslmode=require
```

Backup quotidien via `pg_dump` (cron externe ou Kestra) :

```bash
pg_dump -h $HOST -U sds sds_prod | gzip > /backups/sds_$(date +%Y%m%d).sql.gz
```

Retention : 30 jours glissants sur stockage chiffré.

## 4. Monitoring & alerting

### Alerting Slack en cas d'échec de run

**À implémenter pour la prod** : ajouter un bloc `errors:` dans [`orchestration/kestra/flows/sds_pipeline.yaml`](orchestration/kestra/flows/sds_pipeline.yaml) qui déclenche un webhook Slack dédié (`SLACK_ALERTS_WEBHOOK_URL`, distinct du canal de récap). L'env var globale `slack_alerts_webhook_url` est déjà exposée dans le docker-compose.yml — il suffit de la consommer depuis le flow.

### Rétention table `monitoring.pipeline_runs`

À 1 run / heure × 24 h × 365 j = 8 760 lignes / an. Négligeable. Mais prévoir une politique :

```sql
-- Purge des runs de plus de 3 ans
DELETE FROM monitoring.pipeline_runs
WHERE run_date < NOW() - INTERVAL '3 years';
```

Planifier via pg_cron ou un Kestra cleanup flow hebdomadaire.

## 5. Rotation des clés API externes

| Clé | Rotation recommandée | Action |
|---|---|---|
| `GOOGLE_MAPS_API_KEY` | 90 jours | Nouvelle clé + restriction Geocoding API uniquement + restriction IP |
| `SLACK_WEBHOOK_URL` | Au besoin | Régénérer via https://api.slack.com/apps |
| `POSTGRES_PASSWORD` | 60 jours | Outil de gestion secrets (Vault, AWS Secrets Manager) |

## 6. Audit log PostgreSQL

```sql
ALTER SYSTEM SET log_statement = 'mod';         -- log INSERT/UPDATE/DELETE
ALTER SYSTEM SET log_connections = on;
ALTER SYSTEM SET log_disconnections = on;
SELECT pg_reload_conf();
```

Centraliser dans un SIEM (Elasticsearch, Splunk) avec rétention 1 an.

## 7. RGPD — droits des personnes

Le pipeline doit pouvoir traiter les 3 droits prévus par les articles 15/16/17 du RGPD :

| Droit | Action | Impact pipeline |
|---|---|---|
| Accès (art. 15) | Sortie CSV des données d'un salarié | Nouveau script `scripts/export_personal_data.py` |
| Rectification (art. 16) | UPDATE en base + reprocess | Pipeline déjà idempotent |
| Effacement (art. 17) | DELETE CASCADE sur `raw.employees` | Vérifier les FK : OK (`ON DELETE CASCADE`) |

Documenter le délai de réponse (< 1 mois réglementaire).

## 8. Certification RGPD par un DPO

Le POC a été conçu selon les principes du RGPD mais n'a pas été audité par un DPO. En production, prévoir :

- Analyse d'impact (PIA) si la volumétrie ou l'usage évolue
- Inscription au registre des traitements
- Revue annuelle par le DPO

## 9. Checklist de bascule prod (résumé exécutable)

- [ ] Basic-auth Kestra activée
- [ ] Kestra backend Postgres (JDBC) configuré
- [ ] PostgreSQL `sslmode=require` sur toutes les connexions
- [ ] Backups quotidiens automatisés
- [ ] Rotation des clés API planifiée
- [ ] Alerting Slack en cas d'échec configuré
- [ ] Audit log PostgreSQL activé
- [ ] Tests de bout en bout sur un environnement de staging
- [ ] DPO informé et pipeline validé
- [ ] Runbook d'incident rédigé (qui appeler, comment rollback)
