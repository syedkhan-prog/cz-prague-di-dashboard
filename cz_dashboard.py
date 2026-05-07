"""
CZ Prague DI Dashboard - Tourist / Local / Expat
=================================================

Single-page DI Overview dashboard for Prague (city_id=271, country=cz).

Modes:
    python3 cz_dashboard.py --build          # Just generate docs/index.html + docs/data.json
                                              # (used by GitHub Actions)
    python3 cz_dashboard.py --serve          # Start local HTTP server with live /refresh
                                              # endpoint and auto-open the dashboard
    python3 cz_dashboard.py --serve --refresh   # Force a fresh query before serving
    python3 cz_dashboard.py --serve --no-query  # Serve cached docs/data.json only
    python3 cz_dashboard.py --serve --date 2026-04-26   # Backfill: pretend "today" = this date

Auth: env var DATABRICKS_TOKEN -> PAT (CI); else OAuth browser flow (local).

Periods (DI Overview):
    Previous week  (Mon-Sun, the week before "last week")
    Last week      (most recent completed Mon-Sun)
    Month-to-Date  (1st of current month -> yesterday)

Period date ranges are recomputed from `today` on every refresh, so windows
shift forward automatically.
"""

from __future__ import annotations

import argparse
import http.server
import json
import os
import socketserver
import sys
import threading
import traceback
import webbrowser
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

DOCS = ROOT / "docs"
DATA_FILE = DOCS / "data.json"
HTML_FILE = DOCS / "index.html"

PORT = 8765

# Prague city + CZ phone prefix
CZ_COUNTRY = "cz"
PRAGUE_CITY_ID = 271
CZ_PHONE_PREFIX = "420"


# ---------------------------------------------------------------------------
# Periods (computed dynamically from today)
# ---------------------------------------------------------------------------
def _fmt_range(s, e):
    s_d = date.fromisoformat(s) if isinstance(s, str) else s
    e_d = date.fromisoformat(e) if isinstance(e, str) else e
    if s_d.month == e_d.month and s_d.year == e_d.year:
        return f"{s_d.strftime('%b %-d')} - {e_d.strftime('%-d')}"
    return f"{s_d.strftime('%b %-d')} - {e_d.strftime('%b %-d')}"


def _week_anchors(today):
    this_mon = today - timedelta(days=today.weekday())
    last_mon = this_mon - timedelta(days=7)
    last_sun = this_mon - timedelta(days=1)
    prev_mon = last_mon - timedelta(days=7)
    prev_sun = last_mon - timedelta(days=1)
    return this_mon, last_mon, last_sun, prev_mon, prev_sun


def get_periods(today=None):
    today = today or date.today()
    yesterday = today - timedelta(days=1)
    _, last_mon, last_sun, prev_mon, prev_sun = _week_anchors(today)
    month_start = today.replace(day=1)

    return [
        {
            "key": "prev_week",
            "label": f"Previous week ({_fmt_range(prev_mon, prev_sun)})",
            "short": "Previous week",
            "start": prev_mon.isoformat(),
            "end": prev_sun.isoformat(),
        },
        {
            "key": "last_week",
            "label": f"Last week ({_fmt_range(last_mon, last_sun)})",
            "short": "Last week",
            "start": last_mon.isoformat(),
            "end": last_sun.isoformat(),
        },
        {
            "key": "mtd",
            "label": f"Month-to-Date ({_fmt_range(month_start, yesterday)})",
            "short": "MTD",
            "start": month_start.isoformat(),
            "end": yesterday.isoformat(),
        },
    ]


# ---------------------------------------------------------------------------
# Query layer
# ---------------------------------------------------------------------------
def _to_native(v):
    if v is None:
        return None
    try:
        import pandas as pd
        if pd.isna(v):
            return None
    except Exception:
        pass
    try:
        from decimal import Decimal
        if isinstance(v, Decimal):
            return float(v)
    except Exception:
        pass
    if hasattr(v, "isoformat"):
        return v.isoformat()
    if isinstance(v, (int, float, str, bool)):
        return v
    return str(v)


def _df_records(df):
    return [{k: _to_native(v) for k, v in row.items()} for row in df.to_dict("records")]


USER_SEG_CTE = f"""
user_seg AS (
    SELECT
        id AS user_id,
        CASE
            WHEN phone_anonymized = '{CZ_PHONE_PREFIX}' THEN 'local'
            WHEN phone_anonymized IS NULL OR phone_anonymized = '' THEN 'unknown'
            WHEN home_city_id = {PRAGUE_CITY_ID} THEN 'expat'
            ELSE 'tourist'
        END AS segment
    FROM ng_public_spark.user_user
)"""


def fetch_di_breakdown(dbx, start, end):
    q = f"""
    WITH {USER_SEG_CTE}
    SELECT
        COALESCE(s.segment, 'unmapped') AS segment,
        COUNT(DISTINCT d.order_id)              AS orders,
        COUNT(DISTINCT d.user_id)               AS active_users,
        ROUND(SUM(d.order_gmv_eur), 0)          AS gmv_eur,
        ROUND(SUM(d.campaign_spend_bolt_eur),0) AS bolt_spend_eur,
        ROUND(AVG(d.order_gmv_eur), 2)          AS aov_eur,
        ROUND(SUM(d.campaign_spend_bolt_eur)*100.0/NULLIF(SUM(d.order_gmv_eur),0), 2) AS di_pct
    FROM ng_delivery_spark.dim_order_delivery d
    LEFT JOIN user_seg s ON d.user_id = s.user_id
    WHERE d.country_code = '{CZ_COUNTRY}'
      AND d.city_id = {PRAGUE_CITY_ID}
      AND d.order_state = 'delivered'
      AND d.order_created_date BETWEEN '{start}' AND '{end}'
    GROUP BY ROLLUP(COALESCE(s.segment, 'unmapped'))
    ORDER BY segment NULLS FIRST
    """
    df = dbx.query(q)
    df["segment"] = df["segment"].fillna("TOTAL_PRAGUE")
    for col in ["gmv_eur", "bolt_spend_eur", "aov_eur", "di_pct"]:
        df[col] = df[col].astype(float)
    df["orders"] = df["orders"].astype(int)
    df["active_users"] = df["active_users"].astype(int)

    total_bolt = float(df.loc[df["segment"] == "TOTAL_PRAGUE", "bolt_spend_eur"].iloc[0]) or 1.0
    total_gmv = float(df.loc[df["segment"] == "TOTAL_PRAGUE", "gmv_eur"].iloc[0]) or 1.0
    df["pct_of_bolt_spend"] = (df["bolt_spend_eur"] / total_bolt * 100).round(2)
    df["pct_of_gmv"] = (df["gmv_eur"] / total_gmv * 100).round(2)
    return _df_records(df)


def run_all_queries(today=None):
    from dbx import DBX

    today = today or date.today()
    periods = get_periods(today)

    print(f"[refresh] reference date: {today.isoformat()}", flush=True)
    print("[refresh] connecting to Databricks ...", flush=True)
    dbx = DBX()
    print(f"[refresh]   auth mode: {dbx.auth_mode}", flush=True)

    di = {}
    for p in periods:
        print(f"[refresh] DI breakdown - {p['short']} ({p['start']} -> {p['end']})", flush=True)
        di[p["key"]] = {
            "label": p["label"],
            "short": p["short"],
            "start": p["start"],
            "end": p["end"],
            "rows": fetch_di_breakdown(dbx, p["start"], p["end"]),
        }

    dbx.close()
    print("[refresh] done.", flush=True)

    return {
        "last_refreshed": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "reference_date": today.isoformat(),
        "city": {"id": PRAGUE_CITY_ID, "name": "Prague", "country": CZ_COUNTRY.upper()},
        "phone_prefix": CZ_PHONE_PREFIX,
        "periods": periods,
        "di_breakdown": di,
    }


def load_or_fetch(force=False, today=None):
    if force or not DATA_FILE.exists():
        data = run_all_queries(today=today)
        DOCS.mkdir(parents=True, exist_ok=True)
        with open(DATA_FILE, "w") as f:
            json.dump(data, f, default=str)
        return data
    with open(DATA_FILE) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Dashboard HTML
# ---------------------------------------------------------------------------
HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Prague DI Dashboard - Tourist / Local / Expat</title>
<style>
:root{
  --bg:#0b1220; --panel:#111a2e; --panel-2:#19243d; --border:#27314c;
  --text:#e6ecff; --muted:#94a3b8;
  --accent:#60a5fa; --accent-2:#7c3aed;
  --local:#3b82f6; --expat:#f97316; --tourist:#22c55e;
  --total:#fbbf24; --unknown:#6b7280;
  --good:#10b981; --warn:#f59e0b; --bad:#ef4444;
}
*{box-sizing:border-box}
html,body{margin:0;padding:0;font-family:-apple-system,Segoe UI,Inter,sans-serif;background:var(--bg);color:var(--text);}
header{
  position:sticky;top:0;z-index:10;
  background:linear-gradient(135deg,#0f172a,#1e1b4b);
  border-bottom:1px solid var(--border);padding:18px 28px;
  display:flex;align-items:center;justify-content:space-between;gap:24px;flex-wrap:wrap;
}
header h1{margin:0;font-size:22px;letter-spacing:.3px}
header .meta{display:flex;align-items:center;gap:14px;color:var(--muted);font-size:13px;flex-wrap:wrap}
button.refresh{
  background:linear-gradient(90deg,#3b82f6,#7c3aed);color:#fff;border:0;
  padding:9px 18px;border-radius:8px;font-weight:600;cursor:pointer;font-size:13px;
  display:flex;align-items:center;gap:6px;
  transition:transform .12s ease,box-shadow .12s ease;
  box-shadow:0 4px 12px rgba(59,130,246,.25);
}
button.refresh:hover{transform:translateY(-1px);box-shadow:0 6px 16px rgba(59,130,246,.35)}
button.refresh:disabled{opacity:.6;cursor:wait}
main{padding:24px 28px 80px;max-width:1400px;margin:0 auto}
.kpi-row{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:14px;margin-bottom:22px}
.kpi{background:var(--panel);border:1px solid var(--border);border-radius:10px;padding:14px 16px}
.kpi .label{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.7px}
.kpi .value{font-size:22px;font-weight:700;margin-top:4px}
.kpi .sub{font-size:12px;color:var(--muted);margin-top:2px}
h2{margin:18px 0 10px;font-size:18px}
.card{background:var(--panel);border:1px solid var(--border);border-radius:12px;padding:18px;margin-bottom:18px}
.card-title{font-size:15px;font-weight:600;margin:0 0 4px;color:#cdd6ff}
.card-sub{font-size:12px;color:var(--muted);margin:0 0 12px}
table{width:100%;border-collapse:collapse;font-size:13px}
thead th{
  text-align:right;padding:10px 12px;background:var(--panel-2);border-bottom:1px solid var(--border);
  font-weight:600;color:#cdd6ff;text-transform:uppercase;font-size:11px;letter-spacing:.5px;
}
thead th:first-child{text-align:left}
tbody td{padding:9px 12px;border-bottom:1px solid var(--border);text-align:right;color:#dde6ff}
tbody td:first-child{text-align:left;font-weight:500}
tbody tr:hover{background:rgba(255,255,255,.03)}
tbody tr.total td{background:rgba(251,191,36,.08);font-weight:700;color:var(--total)}
.seg-pill{display:inline-block;padding:2px 9px;border-radius:999px;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.4px}
.seg-local{background:rgba(59,130,246,.15);color:var(--local)}
.seg-expat{background:rgba(249,115,22,.15);color:var(--expat)}
.seg-tourist{background:rgba(34,197,94,.15);color:var(--tourist)}
.seg-unknown{background:rgba(107,114,128,.18);color:#cbd5e1}
.seg-total{background:rgba(251,191,36,.18);color:var(--total)}
.muted{color:var(--muted)}
.toast{
  position:fixed;bottom:24px;right:24px;background:#1f2937;border:1px solid var(--border);
  padding:14px 18px;border-radius:10px;color:var(--text);font-size:13px;
  box-shadow:0 6px 18px rgba(0,0,0,.45);z-index:100;display:none;max-width:420px;
}
.toast.show{display:block}
.toast.error{border-color:#ef4444;color:#fca5a5}
.toast.info{border-color:#60a5fa;color:#bfdbfe}
.toast a{color:#fff;text-decoration:underline;font-weight:600}
code{background:#1e293b;padding:2px 6px;border-radius:4px;font-size:12px}
.note{
  background:rgba(96,165,250,.07);border-left:3px solid var(--accent);
  padding:10px 14px;border-radius:6px;font-size:12.5px;color:#cbd5e1;margin-bottom:14px;
}
.callout{
  background:rgba(34,197,94,.08);border-left:3px solid var(--good);
  padding:10px 14px;border-radius:6px;font-size:13px;color:#bbf7d0;margin:8px 0 16px;
}
</style>
</head>
<body>
<header>
  <div>
    <h1>Prague DI Dashboard <span class="muted" style="font-size:13px;font-weight:400;">- Tourist / Local / Expat</span></h1>
    <div class="meta" style="margin-top:4px">
      <span>As-of: <strong id="ref-date">-</strong></span>
      <span class="muted">|</span>
      <span>Last refreshed: <strong id="ts">-</strong></span>
    </div>
  </div>
  <button class="refresh" id="refresh-btn" onclick="doRefresh()">
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="M3 12a9 9 0 0 1 15.5-6.4L21 8"/><path d="M21 3v5h-5"/><path d="M21 12a9 9 0 0 1-15.5 6.4L3 16"/><path d="M3 21v-5h5"/></svg>
    Refresh Now
  </button>
</header>

<main>
  <div class="note">
    <strong>Definitions:</strong>
    <span class="seg-pill seg-local">Local</span> phone prefix <code>+420</code> ·
    <span class="seg-pill seg-expat">Expat</span> non-CZ prefix &amp; <code>home_city_id = 271</code> (Prague) ·
    <span class="seg-pill seg-tourist">Tourist</span> non-CZ prefix &amp; <code>home_city_id != 271</code> (or NULL) ·
    <span class="seg-pill seg-unknown">Unknown</span> NULL phone.
    DI% = Bolt-funded campaign spend / GMV (Bolt-only, excludes provider co-fund). Filtered to delivered orders in Prague (city_id 271).
  </div>
  <div class="callout" id="callout-wow"></div>
  <div id="di-cards"></div>
</main>

<div id="toast" class="toast"></div>

<script>
const DATA = __DATA_PLACEHOLDER__;
const REFRESH_CONFIG = __REFRESH_CONFIG__;

const segMeta = {
  local:        {label:"Local",   cls:"seg-local"},
  tourist:      {label:"Tourist", cls:"seg-tourist"},
  expat:        {label:"Expat",   cls:"seg-expat"},
  unknown:      {label:"Unknown", cls:"seg-unknown"},
  unmapped:     {label:"Unmapped",cls:"seg-unknown"},
  TOTAL_PRAGUE: {label:"TOTAL Prague", cls:"seg-total"},
};
const segOrder = ["TOTAL_PRAGUE","local","tourist","expat","unknown","unmapped"];

function fmtInt(n){ if(n===null||n===undefined||isNaN(n)) return "-"; return Math.round(n).toLocaleString(); }
function fmtEUR(n){ if(n===null||n===undefined||isNaN(n)) return "-"; return "EUR "+Math.round(n).toLocaleString(); }
function fmtEUR2(n){ if(n===null||n===undefined||isNaN(n)) return "-"; return "EUR "+Number(n).toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2}); }
function fmtPct(n,d=2){ if(n===null||n===undefined||isNaN(n)) return "-"; return Number(n).toFixed(d)+"%"; }

function toast(msg, kind="info", durationMs=5500){
  const t = document.getElementById("toast");
  t.innerHTML = msg;
  t.className = "toast show " + kind;
  if(durationMs > 0) setTimeout(()=>{ t.className = "toast"; }, durationMs);
}

function renderDI(){
  const container = document.getElementById("di-cards");
  container.innerHTML = "";

  const co = document.getElementById("callout-wow");
  const lw = DATA.di_breakdown.last_week;
  const pw = DATA.di_breakdown.prev_week;
  if(lw && pw){
    const lt = lw.rows.find(r => r.segment === "TOTAL_PRAGUE");
    const pt = pw.rows.find(r => r.segment === "TOTAL_PRAGUE");
    if(lt && pt){
      const dDi    = lt.di_pct          - pt.di_pct;
      const dGmv   = pt.gmv_eur          ? (lt.gmv_eur          - pt.gmv_eur)          / pt.gmv_eur          * 100 : 0;
      const dSpend = pt.bolt_spend_eur   ? (lt.bolt_spend_eur   - pt.bolt_spend_eur)   / pt.bolt_spend_eur   * 100 : 0;
      const dOrd   = pt.orders           ? (lt.orders           - pt.orders)           / pt.orders           * 100 : 0;
      const arr = (n) => n > 0.005 ? "▲" : n < -0.005 ? "▼" : "—";
      const sgn = (n, d=1) => (n > 0 ? "+" : "") + Number(n).toFixed(d);
      const lwRange = `${lw.start.slice(5)} → ${lw.end.slice(5)}`;
      const pwRange = `${pw.start.slice(5)} → ${pw.end.slice(5)}`;
      co.innerHTML =
        `<strong>WoW change</strong> · last week (${lwRange}) vs previous week (${pwRange}): ` +
        `Orders ${arr(dOrd)} <strong>${sgn(dOrd)}%</strong> · ` +
        `GMV ${arr(dGmv)} <strong>${sgn(dGmv)}%</strong> · ` +
        `Bolt spend ${arr(dSpend)} <strong>${sgn(dSpend)}%</strong> · ` +
        `DI ${arr(dDi)} <strong>${sgn(dDi, 2)} pp</strong>`;
      co.style.display = "";
    } else {
      co.style.display = "none";
    }
  } else {
    co.style.display = "none";
  }

  for(const period of DATA.periods){
    const block = DATA.di_breakdown[period.key];
    if(!block) continue;
    const total = block.rows.find(r => r.segment === "TOTAL_PRAGUE") || {};
    const card = document.createElement("div");
    card.className = "card";
    card.innerHTML = `
      <div class="card-title">${block.label}</div>
      <div class="card-sub">${block.start} → ${block.end} · Prague delivered orders</div>
      <div class="kpi-row" style="margin-bottom:14px">
        <div class="kpi"><div class="label">Orders</div><div class="value">${fmtInt(total.orders)}</div></div>
        <div class="kpi"><div class="label">GMV</div><div class="value">${fmtEUR(total.gmv_eur)}</div></div>
        <div class="kpi"><div class="label">Bolt spend</div><div class="value">${fmtEUR(total.bolt_spend_eur)}</div></div>
        <div class="kpi"><div class="label">Blended DI%</div><div class="value">${fmtPct(total.di_pct)}</div></div>
      </div>
      <table>
        <thead>
          <tr>
            <th>Segment</th>
            <th>Orders</th>
            <th>Active users</th>
            <th>GMV</th>
            <th>% GMV</th>
            <th>Bolt spend</th>
            <th>% spend</th>
            <th>AOV</th>
            <th>DI%</th>
          </tr>
        </thead>
        <tbody>
          ${[...block.rows]
            .sort((a,b)=> segOrder.indexOf(a.segment) - segOrder.indexOf(b.segment))
            .map(r=>{
              const m = segMeta[r.segment] || {label:r.segment, cls:"seg-unknown"};
              const isTotal = r.segment === "TOTAL_PRAGUE";
              return `<tr class="${isTotal?'total':''}">
                <td><span class="seg-pill ${m.cls}">${m.label}</span></td>
                <td>${fmtInt(r.orders)}</td>
                <td>${fmtInt(r.active_users)}</td>
                <td>${fmtEUR(r.gmv_eur)}</td>
                <td>${fmtPct(r.pct_of_gmv,1)}</td>
                <td>${fmtEUR(r.bolt_spend_eur)}</td>
                <td>${fmtPct(r.pct_of_bolt_spend,1)}</td>
                <td>${fmtEUR2(r.aov_eur)}</td>
                <td><strong>${fmtPct(r.di_pct)}</strong></td>
              </tr>`;
            }).join("")}
        </tbody>
      </table>
    `;
    container.appendChild(card);
  }
}

function isLocalhost(){
  const h = window.location.hostname;
  return h === "localhost" || h === "127.0.0.1" || h === "0.0.0.0" || h === "";
}

async function doRefresh(){
  const btn = document.getElementById("refresh-btn");
  if(isLocalhost() && REFRESH_CONFIG.local_endpoint){
    btn.disabled = true;
    const orig = btn.innerHTML;
    btn.innerHTML = "Refreshing… (~2-3 min)";
    toast("Re-querying Databricks. This usually takes 2-3 minutes…");
    try{
      const res = await fetch(REFRESH_CONFIG.local_endpoint, {method:"POST"});
      if(!res.ok) throw new Error("HTTP " + res.status);
      toast("Refresh complete. Reloading…");
      setTimeout(()=>location.reload(), 800);
    }catch(e){
      btn.disabled = false;
      btn.innerHTML = orig;
      toast("Refresh failed: " + e.message + ". Make sure cz_dashboard.py is still running.", "error");
    }
    return;
  }

  // GitHub Pages: open the workflow_dispatch UI in a new tab
  const url = REFRESH_CONFIG.actions_url;
  if(url){
    window.open(url, "_blank", "noopener");
    toast(
      `Opening GitHub Actions in a new tab. Click <strong>Run workflow → Run workflow</strong> to refresh. ` +
      `Then come back here and reload the page in ~3 minutes.`,
      "info",
      9000
    );
  } else {
    toast("No refresh endpoint configured. See README.md.", "error");
  }
}

function renderAll(){
  document.getElementById("ts").textContent = DATA.last_refreshed || "(unknown)";
  document.getElementById("ref-date").textContent = DATA.reference_date || "(unknown)";
  renderDI();
}

renderAll();
</script>
</body>
</html>
"""


def render_html(data, refresh_config: dict):
    payload = json.dumps(data, default=str)
    cfg = json.dumps(refresh_config)
    return (
        HTML_TEMPLATE
        .replace("__DATA_PLACEHOLDER__", payload)
        .replace("__REFRESH_CONFIG__", cfg)
    )


def write_html(data, refresh_config: dict):
    DOCS.mkdir(parents=True, exist_ok=True)
    HTML_FILE.write_text(render_html(data, refresh_config))


# ---------------------------------------------------------------------------
# HTTP server (local mode only)
# ---------------------------------------------------------------------------
class Handler(http.server.BaseHTTPRequestHandler):
    refresh_config: dict = {}

    def log_message(self, fmt, *args):
        pass

    def _send(self, code, body, content_type="text/html; charset=utf-8"):
        body_bytes = body.encode("utf-8") if isinstance(body, str) else body
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body_bytes)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body_bytes)

    def do_GET(self):
        path = urlparse(self.path).path
        if path in ("/", "/index.html", "/dashboard"):
            with open(DATA_FILE) as f:
                data = json.load(f)
            self._send(200, render_html(data, self.refresh_config))
            return
        if path == "/data":
            with open(DATA_FILE) as f:
                self._send(200, f.read(), "application/json")
            return
        if path == "/health":
            self._send(200, "ok", "text/plain")
            return
        self._send(404, "not found", "text/plain")

    def do_POST(self):
        path = urlparse(self.path).path
        if path == "/refresh":
            try:
                data = run_all_queries()
                with open(DATA_FILE, "w") as f:
                    json.dump(data, f, default=str)
                write_html(data, self.refresh_config)
                self._send(
                    200,
                    json.dumps({"ok": True, "last_refreshed": data["last_refreshed"]}),
                    "application/json",
                )
            except Exception as e:
                traceback.print_exc()
                self._send(500, json.dumps({"ok": False, "error": str(e)}), "application/json")
            return
        self._send(404, "not found", "text/plain")


class ReusableServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    allow_reuse_address = True
    daemon_threads = True


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def _read_actions_url() -> str:
    """Return the GitHub Actions workflow URL for the 'Refresh' button on the
    deployed site. Reads from env GH_ACTIONS_URL or defaults to the repo we
    push this dashboard to."""
    return os.environ.get(
        "GH_ACTIONS_URL",
        "https://github.com/syedkhan-prog/cz-prague-di-dashboard/actions/workflows/refresh.yml",
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--build", action="store_true", help="Just build static HTML+JSON to docs/")
    parser.add_argument("--serve", action="store_true", help="Run local HTTP server on :%d" % PORT)
    parser.add_argument("--refresh", action="store_true", help="Force a fresh query before serving/building")
    parser.add_argument("--no-query", action="store_true", help="Use cached docs/data.json, do not hit Databricks")
    parser.add_argument("--no-browser", action="store_true", help="Do not auto-open the browser")
    parser.add_argument("--date", help="Reference date YYYY-MM-DD (backfill)")
    args = parser.parse_args()

    if not (args.build or args.serve):
        args.serve = True

    ref_date = date.fromisoformat(args.date) if args.date else None

    refresh_config = {
        "local_endpoint": "/refresh",
        "actions_url": _read_actions_url(),
    }

    if args.no_query:
        if not DATA_FILE.exists():
            print(f"ERROR: --no-query but no cached data at {DATA_FILE}", file=sys.stderr)
            sys.exit(1)
        with open(DATA_FILE) as f:
            data = json.load(f)
        print(f"[init] using cached data ({data.get('last_refreshed')})")
    else:
        if ref_date:
            print(f"[init] reference date overridden via --date: {ref_date.isoformat()}")
        data = load_or_fetch(force=args.refresh, today=ref_date)

    write_html(data, refresh_config)
    print(f"[init] wrote {HTML_FILE}")
    print(f"[init] wrote {DATA_FILE}")

    if args.build:
        print("[build] done.")
        return

    Handler.refresh_config = refresh_config
    httpd = ReusableServer(("127.0.0.1", PORT), Handler)
    url = f"http://localhost:{PORT}/"
    print()
    print("=" * 70)
    print(f"  Prague DI Dashboard running at  {url}")
    print(f"  Static HTML written to          {HTML_FILE}")
    print(f"  Cached data:                    {DATA_FILE}")
    print(f"  Last refreshed:                 {data.get('last_refreshed')}")
    print("  Press Ctrl-C to stop.")
    print("=" * 70)
    print()

    if not args.no_browser:
        threading.Timer(0.6, lambda: webbrowser.open(url)).start()

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n[shutdown] bye.")
        httpd.server_close()


if __name__ == "__main__":
    main()
