# Running alacati-hava on a Windows laptop as a server

Phase 2 runs on Numan's Windows laptop with Docker Desktop (WSL2 backend, x86_64).
`docker-compose.yml` at the repo root starts:

| Service                 | What it does                                                  | Memory limit |
|-------------------------|---------------------------------------------------------------|--------------|
| `postgres`              | Airflow metadata DB (official `postgres:17.11-bookworm`)      | 512 MB       |
| `airflow-init`          | One-shot: checks `.env`, `airflow db migrate`, admin user     | 1 GB (exits) |
| `airflow-api-server`    | Airflow UI + REST/Execution API, `http://localhost:8080` only | 1 GB         |
| `airflow-scheduler`     | Scheduler + LocalExecutor: runs ingest, dbt, morning summary  | 2 GB         |
| `airflow-dag-processor` | Parses `orchestration/dags/` (required in Airflow 3)          | 768 MB       |
| `telegram-bot`          | `alacati-bot`: long-polling Telegram bot                      | 384 MB       |

That's about 4.7 GB in the worst case, which fits on a 15 GB laptop. Honest scale note: at
17 HTTP calls per hour, Airflow and Postgres are not needed. They are here to
demonstrate the architecture.

DAGs (`orchestration/dags/`, times in UTC unless stated):

- `weather_hourly`: runs at minute 5 of every hour. `ingest` → `dbt_build` → `dbt_source_freshness`.
  Ingest exit code 2 (partial) logs a warning and dbt still runs. Exit code 1 fails the task.
  Freshness only warns and never fails the run.
- `morning_summary`: runs at 08:00 Europe/Istanbul. It sends the Telegram morning summary,
  which is idempotent through `alacati_ops.summary_log`. Scheduled runs are skipped after
  `summary.stale_after_local_hour` in `config/bot.yaml` (12:00 local time), and also when
  the run slot belongs to an earlier day. Exit code 3 of `alacati-bot-summary` ("sent, but
  the summary_log write failed") counts as success with a warning and is never retried,
  because a retry would send the message twice.
- `airflow_maintenance`: Sundays 02:30 UTC. `airflow db clean` for metadata older than
  30 days (no archive tables) and deletes task log files older than 30 days.

Every DAG has a failure callback: when a run fails (after its retries), a short Telegram
message goes to `TELEGRAM_CHAT_ID`, see §6.

## 1. One-time setup

### 1.1 Docker Desktop

1. Install Docker Desktop with the **WSL2** backend (it installs WSL2 if needed).
2. Docker Desktop → **Settings → General**:
   - **Start Docker Desktop when you sign in to your computer**: **on**
   - **Open Docker Dashboard when Docker Desktop starts**: **off**
3. Optional: cap the memory WSL2 may take with `%UserProfile%\.wslconfig`:

   ```ini
   [wsl2]
   memory=8GB
   ```

   Then run `wsl --shutdown` and restart Docker Desktop.

Docker Desktop starts only **after a Windows sign-in**. After a reboot (for example a
Windows Update restart), nothing runs until someone signs in. If you want unattended
restarts, enable automatic sign-in (for example Sysinternals *Autologon*). This is a
security trade-off: anyone with physical access gets your session.

### 1.2 Never sleep on AC power

The laptop must not sleep or hibernate while it's plugged in. Screen-off is fine.

- **Settings → System → Power & battery → Screen, sleep & hibernate timeouts**:
  set "When plugged in, put my device to sleep after" to **Never**, and set
  hibernate (if shown) to **Never** as well.
- Or from an elevated PowerShell / cmd:

  ```powershell
  powercfg /change standby-timeout-ac 0     # never sleep on AC
  powercfg /change hibernate-timeout-ac 0   # never hibernate on AC
  powercfg /change monitor-timeout-ac 10    # screen off after 10 min is fine
  ```

- **Lid-close action on AC**: Control Panel → Hardware and Sound → Power Options →
  *Choose what closing the lid does* → "When I close the lid" / **Plugged in: Do nothing**.
  The same from the command line:

  ```powershell
  powercfg /setacvalueindex SCHEME_CURRENT SUB_BUTTONS LIDACTION 0
  powercfg /setactive SCHEME_CURRENT
  ```

- To check what the laptop supports, run `powercfg /a`. To see why it woke or slept, run
  `powercfg /lastwake` and `powercfg /sleepstudy`.
- Settings → Windows Update → Advanced options → set **Active hours**, so that
  update restarts don't land in the middle of the day.

On battery the laptop may still sleep. That's fine, because the system recovers (see §3).

### 1.3 Google Cloud credentials (ADC, no key file)

```powershell
gcloud auth application-default login
gcloud auth application-default set-quota-project <your-gcp-project-id>
```

This writes `C:\Users\<you>\AppData\Roaming\gcloud\application_default_credentials.json`.
Compose mounts the **folder** (`GCP_ADC_DIR`) read-only at `/opt/gcp` in the scheduler and
bot containers and sets `GOOGLE_APPLICATION_CREDENTIALS=/opt/gcp/application_default_credentials.json`
and `GOOGLE_CLOUD_PROJECT=<GCP_PROJECT_ID>`. The other Airflow containers get no GCP access.

Why the folder and not the file: a re-login replaces the JSON file. A single-file bind
mount would keep showing the old (revoked) file until the containers are recreated; a
folder mount shows the new file at once. Trade-off: the rest of the gcloud folder is
visible read-only inside those two containers too.

**Re-login.** When tasks fail with `invalid_grant` / `Reauthentication is needed` /
`Token has been expired or revoked` (user ADC can expire after a password change, long
inactivity or a session policy):

```powershell
gcloud auth application-default login
gcloud auth application-default set-quota-project <your-gcp-project-id>
```

No restart is needed: the next task run reads the new file through the folder mount.
Clear the failed task in the UI (or wait for the next hourly run). The long-running bot
reads credentials again on its next BigQuery client refresh; if its commands keep failing,
run `docker compose restart telegram-bot`.

Production alternative: create a dedicated service account with only the BigQuery roles
it needs (for example *BigQuery Job User* on the project, plus *Data Editor* on the
`alacati_*` datasets) and mount its key the same way, or better, use workload identity
on a cloud host. Personal user credentials on a laptop are fine for this portfolio project.

### 1.4 `.env`

Copy `.env.example` to `.env` (it's git-ignored) and fill in every value. Phase 2 needs:

- `GCP_PROJECT_ID`, `BQ_*`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`
- `GCP_ADC_DIR`: host **folder** that holds the ADC file from §1.3, with forward slashes
  and no trailing slash, e.g. `C:/Users/numan/AppData/Roaming/gcloud`
- `GOOGLE_APPLICATION_CREDENTIALS`: leave it unset. Local runs use gcloud ADC directly,
  and Compose sets the in-container path itself.
- `AIRFLOW_POSTGRES_USER/PASSWORD/DB`: use letters and digits only for the password
  (it goes into a connection URL).
- `AIRFLOW_ADMIN_USER`, `AIRFLOW_ADMIN_PASSWORD`: the UI login.
- `AIRFLOW_FERNET_KEY`, `AIRFLOW_JWT_SECRET`, `AIRFLOW_API_SECRET_KEY`. Generate them with:

  ```powershell
  uv run --no-project --with cryptography python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
  python -c "import secrets; print(secrets.token_urlsafe(32))"   # run twice: JWT secret, API secret key
  ```

`airflow-init` refuses to start when these still hold their `.env.example` placeholders.
Compose itself stops with a clear message when a required variable is missing.

There is no `env_file:` in `docker-compose.yml`. Each service receives only the variables
it needs through `environment:`: the scheduler (runs the ingest/dbt/summary tasks) gets the
BigQuery and Telegram settings, the dag-processor only the Telegram ones (it runs the
failure callbacks), the API server and `airflow-init` only Airflow's own settings. After
adding a new variable to `.env`, also add it to the right service in the compose file.

## 2. Start, logs, stop

All commands run from the repo root:

```powershell
docker compose up -d --build        # first start, and after any code change
docker compose ps                   # all services "running (healthy)"; airflow-init "exited (0)"
docker compose logs -f airflow-scheduler telegram-bot
docker compose logs airflow-init    # if the stack does not come up
docker compose stop                 # stop, keep containers + data
docker compose start                # resume
docker compose down                 # remove containers, KEEP the Postgres + log volumes
# docker compose down -v            # also deletes Airflow's DB and logs (run history!)
```

- Changes in `config/*.yaml` or `contracts/` **need no rebuild**: they're bind-mounted
  read-only and read again on every task run. The long-running bot may cache its
  config, so run `docker compose restart telegram-bot` after a change to be safe.
- Changes in `orchestration/dags/` are picked up by the DAG processor within about 5 minutes.
- Changes in `ingestion/`, `dbt/`, or `apps/telegram_bot/` are baked into the images, so run
  `docker compose up -d --build`.
- To change the admin password later (the init keeps an existing user as it is):
  `docker compose exec airflow-api-server airflow users reset-password --username <user> --password <new>`

## 3. How it comes back after a reboot or sleep

1. **Docker Desktop** starts at sign-in (§1.1). Every long-running service has
   `restart: unless-stopped`, so Docker restarts postgres, the Airflow services and the bot
   on its own. `airflow-init` is not rerun, and it doesn't need to be. A service you stopped
   with `docker compose stop` stays stopped.
2. **Airflow** uses `catchup=False`. It doesn't replay every missed hour. After a wake-up it
   starts the **most recent** missed slot, and at most one older one from before the
   sleep. `max_active_runs=1` keeps those runs one after the other. Task retries
   (1 × 2 min for weather, 3 × 5 min for the summary) cover the first minutes, while
   the network or the API server is still coming up.
3. **Ingestion** fills the gap itself. `past_hours` becomes the hours since the last
   successful run, capped at `max_catchup_hours` in `config/ingestion.yaml`. One run
   re-fetches the missed hours, and Open-Meteo still serves them.
4. **Morning summary**: a late run before `summary.stale_after_local_hour` (12:00
   Istanbul time) still sends today's summary
   (once, thanks to `summary_log`). After 12:00, or for a slot from an earlier day, the
   scheduled run is skipped as stale. A manual trigger from the UI always sends.
5. **Clock drift after sleep (WSL2).** After the laptop wakes up, the WSL2 VM clock can
   lag behind Windows by minutes or hours. Symptoms: runs start late or show odd times,
   Google calls fail with `invalid_grant` or "token used too early", HTTPS calls fail with
   "certificate is not yet valid", and Airflow logs show JWT / heartbeat errors.
   Check with `docker run --rm alpine date -u` and compare with the Windows clock (UTC).
   Fix:

   ```powershell
   docker compose stop
   wsl --shutdown
   # then restart Docker Desktop (tray icon -> Restart, or quit and reopen it)
   docker compose start
   ```

   The ingestion fills the gap afterwards (point 3).

### Time budget per run

`dagrun_timeout` always covers the worst case of all task attempts (`execution_timeout`
× attempts + retry delays), so a run is never cut off in the middle of a legitimate retry.
Each DAG file checks this at parse time and shows an import error if someone breaks it.

| DAG                   | Task limits (attempts), retry delay                               | Worst case | `dagrun_timeout` |
|-----------------------|-------------------------------------------------------------------|------------|------------------|
| `weather_hourly`      | ingest 8 min (2), dbt build 12 min (2), freshness 4 min (1); 2 min | 48 min     | 55 min (before the next slot) |
| `morning_summary`     | send 5 min (4); 5 min                                              | 35 min     | 45 min           |
| `airflow_maintenance` | db clean 20 min (1), log prune 10 min (1)                          | 30 min     | 45 min           |

## 4. How to verify

1. `docker compose ps`: every service is `healthy`.
2. Open **http://localhost:8080** and sign in with `AIRFLOW_ADMIN_USER` /
   `AIRFLOW_ADMIN_PASSWORD`. `weather_hourly` and `morning_summary` should be listed and
   unpaused, and the latest `weather_hourly` run should be green. Open a task to see its log
   (the ingest log ends with the run summary: `status: complete`).
3. DAG import problems: `docker compose exec airflow-scheduler airflow dags list-import-errors`
4. Smoke test without writing to BigQuery:
   `docker compose exec airflow-scheduler /opt/venvs/ingestion/bin/alacati-ingest run --dry-run`
5. Last ingestion runs in BigQuery (BigQuery console or `bq`):

   ```sql
   SELECT run_id, trigger, started_at, finished_at, past_hours,
          succeeded_messages, expected_messages, status
   FROM `alacati_ops.ingest_runs`
   ORDER BY started_at DESC
   LIMIT 5;
   ```

   The newest row should be less than about 1 hour old with `status = 'complete'`. After a
   downtime you'll see one row with a larger `past_hours`.
6. Bot: send `/uyari` to the bot in Telegram. `docker compose logs telegram-bot` shows
   the polling loop.

## 5. Housekeeping

- Container stdout logs are rotated (`local` driver, 3 × 10 MB per service).
- Airflow task logs live in the `airflow-logs` volume (about 70 small files a day). The
  weekly `airflow_maintenance` DAG deletes log files older than 30 days and runs
  `airflow db clean` for metadata rows older than 30 days. Check the size now and then
  with `docker system df -v`.
- Images: after many rebuilds, run `docker image prune` to free disk.

## 6. Failure message in Telegram

When a DAG run fails after its retries (or hits `dagrun_timeout`), the bot chat gets:

```
⚠️ alacati-hava: weather_hourly.ingest başarısız (2026-11-05 16:35 UTC)
```

It is sent once per failed run by the DAG's `on_failure_callback`, from the
`airflow-dag-processor` container (Airflow 3 runs DAG-level callbacks there), with a plain
Bot API call. A partial ingest (exit code 2), exit code 3 of the summary and a skipped
stale summary are **not** failures and send nothing. If Telegram is unreachable, the
callback only logs a warning; the run is still marked failed in the UI.

What to do:

1. Open http://localhost:8080 → the DAG → the failed run → the named task → **Logs**.
2. Typical causes:
   - `invalid_grant`, `Reauthentication is needed`, `RefreshError`: re-login (§1.3).
   - "not yet valid" / token-time errors right after a wake-up: clock drift (§3, point 5).
   - Open-Meteo HTTP errors or timeouts: usually transient; the next hourly run widens
     `past_hours` and fills the gap on its own.
   - `dbt build` test failures: a data issue; the log names the failing test.
   - `morning_summary`: the summary was **not** sent. Trigger the DAG manually if it is
     still morning.
3. Re-run with **Clear** on the failed task, or **Trigger** the DAG. For
   `weather_hourly` you can also just wait: the next run catches up.

No message at all while something is clearly broken means the dag-processor is down or
Telegram is unreachable: check `docker compose ps` and
`docker compose logs airflow-dag-processor`.
