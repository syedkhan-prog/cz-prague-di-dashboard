# Prague DI Dashboard - Tourist / Local / Expat

Single-page DI overview for **Bolt Food, Prague (city_id 271)**.
Splits delivered orders into 3 cohorts based on phone prefix + home_city, and
shows Bolt-funded discount intensity (DI%) for previous week, last week, and MTD.

> **Live dashboard (full, with working Refresh button):** https://cz-prague-di-dashboard.onrender.com/
>
> **Static fallback (GitHub Pages, refresh opens Actions UI):** https://syedkhan-prog.github.io/cz-prague-di-dashboard/

## Cohorts

| Segment   | Definition |
|-----------|------------|
| Local     | `phone_anonymized = '420'`                                              |
| Expat     | `phone_anonymized != '420'` AND `home_city_id = 271`                    |
| Tourist   | `phone_anonymized != '420'` AND (`home_city_id != 271` OR NULL)         |
| Unknown   | NULL phone                                                              |

DI% = `SUM(campaign_spend_bolt_eur) / SUM(order_gmv_eur)` on delivered orders
in Prague (excludes provider co-fund).

## Architecture

```
                                                    +------------------+
   anyone in browser ---> https://cz-prague-di-     |  Render web      |
   clicks "Refresh Now"   dashboard.onrender.com -> |  service (free)  |
                                                    |                  |
                                                    |  cz_dashboard.py |
                                                    |  --serve         |
                                                    +--------+---------+
                                                             | DATABRICKS_TOKEN
                                                             v
                                                       +-----------+
                                                       | Databricks |
                                                       +-----------+

   GitHub Pages mirror (static, daily-refreshed by GH Actions cron):
   https://syedkhan-prog.github.io/cz-prague-di-dashboard/
```

The "Refresh Now" button on the **Render** site re-queries Databricks and
reloads the dashboard within ~10-30 seconds.

The "Refresh Now" button on the **GitHub Pages** mirror has no backend, so it
falls back to opening the GitHub Actions "Run workflow" page (3-min refresh).

## Deploy to Render (one-time setup, ~5 min)

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/syedkhan-prog/cz-prague-di-dashboard)

1. Click the button above. Sign in / sign up for Render (free, no credit card needed).
2. Render reads `render.yaml` and shows the service settings. Leave them as-is.
3. **Set the `DATABRICKS_TOKEN` env var** to your Databricks Personal Access Token
   (the one that starts with `dapi...`). Render marks it `sync: false` so it
   stays out of git.
4. Click **Apply / Create**.
5. First build takes ~3-5 min (installing pandas, etc.). Subsequent deploys are
   ~30 seconds.
6. Note the URL Render assigns you (e.g.
   `https://cz-prague-di-dashboard-XXXX.onrender.com/`).

### After the first deploy: prevent cold starts

Render's free tier puts the service to sleep after 15 min of inactivity. To
prevent that, the repo includes a GitHub Actions workflow that pings
`/health` every 10 min. Tell it your Render URL:

```bash
gh variable set RENDER_URL --body 'https://YOUR-RENDER-URL.onrender.com'
```

(or in the GitHub UI: **Settings -> Secrets and variables -> Actions -> Variables -> New repository variable**, name `RENDER_URL`).

The keep-alive workflow uses 0 GH Actions credits since this is a public repo.

## How refresh works (3 paths)

| Where you click Refresh                  | What happens                                                          | Latency  |
|------------------------------------------|-----------------------------------------------------------------------|----------|
| **cz-prague-di-dashboard.onrender.com**  | POST `/refresh` -> Render queries Databricks -> page reloads          | ~10-30 s |
| **localhost** (`python cz_dashboard.py --serve`) | POST `/refresh` -> queries Databricks via your local OAuth -> reloads | ~10-30 s |
| **syedkhan-prog.github.io** (static)     | Opens GitHub Actions UI, you click "Run workflow", wait, reload       | ~3 min   |

The daily 06:00 UTC GitHub Action also runs unconditionally so the static
mirror never goes more than 24 hours stale.

## Local development

```bash
pip install -r requirements.txt

# Serve cached data, just open the page locally
python cz_dashboard.py --serve --no-query

# Force a fresh query (OAuth browser sign-in to Databricks)
python cz_dashboard.py --serve --refresh

# Just rebuild static HTML+JSON without serving (this is what CI does)
python cz_dashboard.py --build --refresh --no-browser

# Backfill / pretend "today" is some other date
python cz_dashboard.py --serve --refresh --date 2026-04-26
```

Auth is auto-detected:
- env `DATABRICKS_TOKEN` set -> PAT (used by Render and GitHub Actions)
- otherwise -> OAuth browser flow (used locally)

Bind: defaults to `127.0.0.1:8765` locally; switches to `0.0.0.0:$PORT`
automatically when the `PORT` env var is set (Render injects this).

## Repo layout

```
.
├── cz_dashboard.py              # main: CLI + queries + server + HTML template
├── dbx.py                       # Databricks connection (auto-detects PAT vs OAuth)
├── requirements.txt
├── render.yaml                  # Render Blueprint -> "Deploy to Render" button
├── docs/                        # served by GitHub Pages
│   ├── index.html               # generated by --build
│   └── data.json                # generated by --build
└── .github/workflows/
    ├── refresh.yml              # daily DB query + commit fresh data to /docs
    └── keepalive.yml            # ping Render /health every 10 min
```

## Source tables

- `ng_delivery_spark.dim_order_delivery` - order facts (`country_code='cz'`, `city_id=271`, `order_state='delivered'`)
- `ng_public_spark.user_user` - user lookup for `phone_anonymized` + `home_city_id`
