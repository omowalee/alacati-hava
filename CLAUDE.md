# alacati-hava — Çeşme Peninsula Weather Platform

Personal, non-commercial portfolio project by Numan (Alaçatı). Timebox: 3 weeks.

## Communication
- Talk to Numan in **Turkish**.
- Code, code comments, commit messages, README: **English**.
- Bot and dashboard user-facing text: **Turkish**.

## What it does
Pulls Open-Meteo Forecast + Marine data hourly for ~15 beaches and Alaçatı surf/kite
spots on the Çeşme peninsula and offers:
1. **Severe weather alerts (main feature)** — thunderstorm, heavy rain, strong wind,
   high waves; levels yellow / orange / red; lifecycle new → updated → ended, no repeats.
2. **Beach recommendation** — ranks beaches by shelter from the current wind.
3. **Surf/kite conditions** — when Alaçatı wind enters the rideable range.

Channels: Telegram bot (08:00 morning summary, 21:30 night summary + instant alerts, commands
`/uyari`, `/plaj`, `/ruzgar`) and a Streamlit dashboard on Community Cloud.

Out of scope: sea temperature, sunset score, event risk report, forecast verification.
Optional (only if time is left): 30-year Open-Meteo Historical archive for 4–6 points,
processed with PySpark in Docker. Do not build it now, but don't design it out.

## Architecture
```
Open-Meteo (forecast + marine, hourly)
   └─ ingestion/ producer ──► Kafka topic weather.raw  (Phase 3; Phase 1–2: direct BigQuery load)
                                 ├─ streaming/ bq consumer ──► BigQuery alacati_raw (load jobs / Storage Write API)
                                 └─ streaming/ alert engine ──► Kafka topic weather.alerts ──► Telegram bot
BigQuery (EU): alacati_raw → dbt (alacati_staging → alacati_marts), alacati_ops (call log, alert state)
Airflow (LocalExecutor + Postgres): hourly ingest, dbt build, morning + night summary
Streamlit: reads only small mart tables, with caching
```
- Kafka: single broker, KRaft, official `apache/kafka` image, heap 512 MB.
- Airflow: official `apache/airflow` image; Postgres: official `postgres` image.
- **No Bitnami images.**
- Python 3.12, one uv environment per component.
- BigQuery: no legacy streaming inserts. Tables partitioned by date, clustered by location.
- Time: everything stored in **UTC**; displayed in **Europe/Istanbul** (UTC+3, no DST).
- Server = Numan's Windows laptop (Docker Desktop + WSL2). All services
  `restart: unless-stopped`. After downtime, ingestion re-fetches the last hours (`past_hours`).
- Alert rules and beach score live in SQL/dbt as much as possible.

## Folder layout
```
contracts/      data contracts — READ-ONLY, ask Numan before any change
config/         locations, alert thresholds, surf thresholds, beach score weights (YAML)
ingestion/      Open-Meteo clients, loader / Kafka producer
streaming/      Kafka consumers, alert engine
orchestration/  Airflow DAGs
dbt/            dbt project (staging → marts, tests)
apps/telegram_bot/, apps/streamlit/
tests/
```

## Non-negotiable rules
1. **Contracts first.** Nothing in `contracts/` changes without Numan's approval. If a
   component needs a schema change: stop and ask.
2. **No secrets in the repo.** Secrets live in `.env` (git-ignored); `.env.example` has
   placeholders. gitleaks runs as a pre-commit hook — never bypass it (`--no-verify` is forbidden).
3. **Thresholds and parameters come from `config/`**, never hard-coded.
4. **Never invent beach shore orientations.** Anything not confirmed by Numan is `verified: false`.
5. **Open-Meteo terms:** stay far below 10,000 calls/day; count and log every call
   (`alacati_ops.api_call_log`); CC-BY 4.0 attribution in the dashboard and the bot.
6. Commit at every meaningful step; push at the end of each phase.
7. **Stop at the end of each phase**: summarize what was done, how to test it, what's next; wait for approval.
8. **Be honest about scale:** README must state Kafka and Airflow are not required at this
   data size — they are there to demonstrate the architecture.
9. Every alert message and the dashboard carry the disclaimer: personal project based on
   model forecasts; for official warnings see MGM (Meteoroloji Genel Müdürlüğü).
   Exceptions (Numan, 2026-11-05): `/sorf` and the Streamlit dashboard show only the CC BY
   attribution line, no disclaimer. Alert messages and the morning and night summaries keep it.

## Agents
- `data-engineer`, `debugger` → opus; `python-pro`, `sql-pro`, `code-reviewer` → sonnet.
- `code-reviewer` reviews every phase's full diff at the end of the phase (read-only).
  Never self-approve your own code.

## Phases
- **Week 1 — Phase 0 + 1:** setup, contracts, config; Open-Meteo client + BigQuery load
  script; dbt staging + alert events + beach score + surf marts with tests; basic Streamlit.
- **Week 2 — Phase 2:** Airflow + Postgres in Compose; DAGs (hourly ingest, dbt build,
  morning summary); Telegram bot; auto-recovery after laptop reboot.
- **Week 3 — Phase 3:** Kafka, producer, BigQuery consumer, alert engine, instant alerts;
  README; Streamlit Community Cloud. If Kafka slips, ship README + Streamlit first.

## Tooling notes (this machine)
- No winget. uv and pre-commit installed via `pip install --user`; Python 3.12 via `uv python install 3.12`.
- Docker Desktop (per-user install) + WSL2 are installed; the full stack runs via `docker compose up -d --build`.
- Do NOT push to GitHub until Numan explicitly says so (remote origin is set).
