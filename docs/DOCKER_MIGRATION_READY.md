# Docker Migration Readiness

This document is the operational checklist for moving the Dockerized OIAT portal from a test host to a production host such as `oiat-srv-01`.

## What must move

The application code can be pulled again from Git, but the live runtime state is not in Git.

The state that must be preserved lives in the Docker volume `code-scripts_app-data` and includes:

- Django database: `db.sqlite3`
- QBO token store: `code_scripts/qbo_tokens.sqlite`
- uploaded/archive directories
- generated reports
- scheduler/runtime logs

The Caddy certificate state lives in Docker volumes too:

- `code-scripts_caddy_data`
- `code-scripts_caddy_config`

Those Caddy volumes do not strictly need to be migrated because Caddy can obtain a fresh certificate on the new host as long as DNS and the Cloudflare token are correct.

## What does not need to move

- the built Docker images
- the old `caddy-root.crt` file from the internal-CA test path
- `.venv`, local editor settings, and any other workstation-only files

## Production prerequisites on `oiat-srv-01`

1. Docker Desktop or Docker Engine with Compose support installed
2. Tailscale installed and joined to the correct tailnet
3. A stable Tailscale IP for the server
4. The repo checked out on the `docker-build` branch
5. A valid `.env` file on the server
6. Cloudflare DNS record for `portal.oiatsolutions.com`
7. Cloudflare API token in `.env`
8. Tailscale grants/ACLs ready to restrict `tcp:443`

## Recommended migration model

Use a fresh host deployment plus a one-time data import.

That means:

1. Pull the repo on `oiat-srv-01`
2. Put the final `.env` on the server
3. Copy the current state files from the old host into the checked-out repo paths on the new host
4. Run the one-time `bootstrap` service
5. Start `caddy`, `web`, and `scheduler`

This is cleaner than trying to copy raw Docker volumes between hosts unless you already have a consistent volume-backup process.

## State paths to copy onto the new server before bootstrap

Copy these from the current server into the repo working tree on `oiat-srv-01`:

- `db.sqlite3`
- `code_scripts/qbo_tokens.sqlite`
- `code_scripts/Uploaded/`
- `code_scripts/uploads/`
- `code_scripts/logs/`
- `code_scripts/reports/`
- `code_scripts/outputs/`

Then run:

```bash
docker compose build
docker compose run --rm --profile bootstrap bootstrap
docker compose up -d caddy web scheduler
```

## Fresh-host deployment commands

```bash
git fetch origin
git switch docker-build
git pull origin docker-build
docker compose build
docker compose run --rm --profile bootstrap bootstrap
docker compose up -d caddy web scheduler
docker compose logs -f caddy web scheduler
```

## Post-migration smoke tests

```bash
docker compose ps
docker compose logs --tail=100 caddy web scheduler
nslookup portal.oiatsolutions.com
curl -I https://portal.oiatsolutions.com/login/
docker compose exec web python manage.py shell -c "from django.contrib.auth import get_user_model; print(list(get_user_model().objects.values_list('username', flat=True)))"
docker compose exec web python manage.py shell -c "from apps.epos_qbo.models import CompanyConfigRecord; print(list(CompanyConfigRecord.objects.values_list('company_key', flat=True)))"
docker compose exec web python store_tokens.py --list
docker compose exec web python code_scripts/scripts/qbo_queries/qbo_query.py --company company_a query \"select Id, Name from Item maxresults 1\"
```

## Tailscale access control checklist

Before exposing the production server URL to end users:

1. Restrict `tcp:443` on `oiat-srv-01` to the intended Tailscale users or groups
2. Verify unauthorized tailnet users cannot connect
3. Verify authorized users still land on the Django login page

## Backup recommendation

Before migration, back up the current host state:

- the `.env` file
- `db.sqlite3`
- `code_scripts/qbo_tokens.sqlite`
- `code_scripts/Uploaded/`
- `code_scripts/uploads/`
- `code_scripts/logs/`
- `code_scripts/reports/`
- `code_scripts/outputs/`

After migration, back up the Docker volume-backed state on `oiat-srv-01` regularly.

## Common mistakes

- starting `web` and `scheduler` on the new host before seeding state
- forgetting that `bootstrap` is a one-time import step, not a normal update step
- forgetting to seed `code_scripts/companies/*.json` on the volume after migration — the portal reads from the DB, but `run_all_companies.py` / `run_pipeline.py` read the JSON files. If the volume's companies directory is empty, scheduled and Quick Sync runs fail with "No runnable companies found." Fix: `docker compose exec web python manage.py sync_companies_to_json` once after bootstrap
- using a Cloudflare proxied record instead of `DNS only`
- assuming Cloudflare is acting as a WAF here when it is not
- leaving Tailscale access too broad
