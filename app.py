"""
PostgreSQL Multi-Instance Dashboard
Flask backend — dynamic instance list with add/remove API.
"""

import csv
import decimal
import io
import json
import os
import re
import sqlite3
import time
import threading
import traceback
import urllib.request
import uuid
from collections import deque
from datetime import date, datetime, time as dt_time, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
from flask import Flask, jsonify, request, send_from_directory

app = Flask(__name__, static_folder="static")

TIMEOUT = 5  # connection timeout in seconds
INSTANCES_FILE = Path(__file__).parent / "instances.json"

# ---------------------------------------------------------------------------
# Instance registry  (mutable, persisted to instances.json)
# ---------------------------------------------------------------------------
_instances_lock = threading.Lock()

DEFAULT_INSTANCES = [
    {"id": "pg1", "label": "Instance 1", "host": "localhost", "port": 5414,
     "user": "postgres", "password": "postgres", "dbname": "postgres"},
    {"id": "pg2", "label": "Instance 2", "host": "localhost", "port": 5415,
     "user": "postgres", "password": "postgres", "dbname": "postgres"},
    {"id": "pg3", "label": "Instance 3", "host": "localhost", "port": 5416,
     "user": "postgres", "password": "postgres", "dbname": "postgres"},
    {"id": "pg4", "label": "Instance 4", "host": "localhost", "port": 5417,
     "user": "postgres", "password": "postgres", "dbname": "postgres"},
    {"id": "pg5", "label": "Instance 5", "host": "localhost", "port": 5418,
     "user": "postgres", "password": "postgres", "dbname": "postgres"},
]


def load_instances():
    if INSTANCES_FILE.exists():
        try:
            return json.loads(INSTANCES_FILE.read_text())
        except Exception:
            pass
    return list(DEFAULT_INSTANCES)


def save_instances(instances):
    INSTANCES_FILE.write_text(json.dumps(instances, indent=2))


# In-memory instance list — protected by _instances_lock
_instances = load_instances()


def get_instances():
    with _instances_lock:
        return list(_instances)


# ---------------------------------------------------------------------------
# Metric cache  (refreshed every 10 s in background)
# ---------------------------------------------------------------------------
_cache = {}
_cache_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Trend history  (server-side, so it keeps accumulating for every instance
# regardless of which one the browser currently has open — the background
# refresh thread polls all of them every 10s either way)
# ---------------------------------------------------------------------------
HISTORY_MAXLEN = 250
_history = {}       # instance_id -> {"ts": deque, "tps": deque, "conn": deque, "cache": deque}
_prev_xact = {}      # instance_id -> (monotonic_time, cumulative xact_commit+xact_rollback)
_history_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Trend history — on-disk backing store (SQLite)
#
# The in-memory deques above are just a hot cache capped at HISTORY_MAXLEN
# samples (~42 min at the 10s poll interval) and lost on restart. Every
# sample is also persisted here so trends survive a restart and so the UI
# can request a much longer look-back (hours) without bloating the payload
# of every /api/metrics poll.
# ---------------------------------------------------------------------------
TREND_DB_PATH = Path(__file__).parent / "trend_history.sqlite3"
TREND_RETENTION_SAMPLES = 8640  # 24h at a 10s poll interval, per instance
_trend_db_lock = threading.Lock()


def _trend_db_connect():
    conn = sqlite3.connect(TREND_DB_PATH, timeout=5)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_trend_db():
    with _trend_db_lock:
        conn = _trend_db_connect()
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS trend_samples (
                    instance_id TEXT NOT NULL,
                    ts          TEXT NOT NULL,
                    tps         REAL,
                    conn        INTEGER,
                    cache       REAL
                )
            """)
            # rowid itself can't appear in an index's column list (it's
            # already implicit in every index) — indexing instance_id alone
            # is enough since ORDER BY rowid / DELETE ... NOT IN (...) still
            # use SQLite's native rowid ordering.
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_trend_samples_inst ON trend_samples(instance_id)"
            )
            conn.commit()
        finally:
            conn.close()


def record_trend_sample(inst_id, ts_iso, tps, conn_count, cache_hit):
    with _trend_db_lock:
        conn = _trend_db_connect()
        try:
            conn.execute(
                "INSERT INTO trend_samples (instance_id, ts, tps, conn, cache) VALUES (?, ?, ?, ?, ?)",
                (inst_id, ts_iso, tps, conn_count, cache_hit),
            )
            # Keep only the most recent TREND_RETENTION_SAMPLES rows per instance.
            conn.execute(
                "DELETE FROM trend_samples WHERE instance_id = ? AND rowid NOT IN "
                "(SELECT rowid FROM trend_samples WHERE instance_id = ? ORDER BY rowid DESC LIMIT ?)",
                (inst_id, inst_id, TREND_RETENTION_SAMPLES),
            )
            conn.commit()
        finally:
            conn.close()


def load_trend_history(inst_id, samples):
    """Most recent `samples` rows for one instance, oldest first."""
    with _trend_db_lock:
        conn = _trend_db_connect()
        try:
            cur = conn.execute(
                "SELECT ts, tps, conn, cache FROM trend_samples "
                "WHERE instance_id = ? ORDER BY rowid DESC LIMIT ?",
                (inst_id, samples),
            )
            rows = cur.fetchall()
        finally:
            conn.close()
    return list(reversed(rows))


def delete_trend_history(inst_id):
    with _trend_db_lock:
        conn = _trend_db_connect()
        try:
            conn.execute("DELETE FROM trend_samples WHERE instance_id = ?", (inst_id,))
            conn.commit()
        finally:
            conn.close()


def _short_time(ts_iso):
    try:
        return datetime.fromisoformat(ts_iso).strftime("%H:%M:%S")
    except Exception:
        return ts_iso


# ---------------------------------------------------------------------------
# Collection helpers
# ---------------------------------------------------------------------------

def make_dsn(inst):
    return (
        f"host={inst['host']} port={inst['port']} dbname={inst['dbname']} "
        f"user={inst['user']} password={inst['password']} connect_timeout={TIMEOUT}"
    )


def query(conn, sql, params=None):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]


def scalar(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return row[0] if row else None


# ---------------------------------------------------------------------------
# PostgreSQL release catalog (for "a newer version is available" hints)
# ---------------------------------------------------------------------------
_pg_version_catalog = {"data": None, "fetched_at": 0}
_pg_version_catalog_lock = threading.Lock()
PG_VERSION_CATALOG_TTL = 6 * 3600  # refetch at most every 6 hours


def get_pg_version_catalog():
    """Latest published patch release per PostgreSQL major version, keyed by
    major version string (e.g. "17" -> {"latest": "17.10", ...}).

    Cached in memory since this rarely changes; a fetch failure just means we
    skip the "update available" hint until the next successful refresh.
    """
    now = time.time()
    with _pg_version_catalog_lock:
        if _pg_version_catalog["data"] is not None and now - _pg_version_catalog["fetched_at"] < PG_VERSION_CATALOG_TTL:
            return _pg_version_catalog["data"]
    try:
        with urllib.request.urlopen("https://endoflife.date/api/postgresql.json", timeout=5) as resp:
            cycles = json.load(resp)
        catalog = {c["cycle"]: c for c in cycles}
        with _pg_version_catalog_lock:
            _pg_version_catalog["data"] = catalog
            _pg_version_catalog["fetched_at"] = now
        return catalog
    except Exception:
        with _pg_version_catalog_lock:
            return _pg_version_catalog["data"]


def find_newer_pg_version(version_short):
    """Return the latest published patch version if it's newer than
    `version_short` (e.g. "14.23 (Debian ...)"), else None."""
    m = re.match(r'(\d+)\.(\d+)', version_short or "")
    if not m:
        return None
    major, minor = m.group(1), int(m.group(2))
    catalog = get_pg_version_catalog()
    cycle = catalog.get(major) if catalog else None
    if not cycle:
        return None
    latest = cycle.get("latest") or ""
    lm = re.search(r'\.(\d+)$', latest)
    if lm and int(lm.group(1)) > minor:
        return latest
    return None


def _parse_pgaudit_message(message):
    """pgaudit's log line is CSV-ish: AUDIT_TYPE,STATEMENT_ID,SUBSTATEMENT_ID,
    CLASS,COMMAND,OBJECT_TYPE,OBJECT_NAME,STATEMENT,PARAMETER — with CSV
    quoting for any field (usually STATEMENT) containing a comma or newline,
    prefixed with "AUDIT: ". Returns None if it doesn't match that shape, so
    callers can fall back to showing the raw message untouched.
    """
    prefix = "AUDIT: "
    if not message or not message.startswith(prefix):
        return None
    try:
        fields = next(csv.reader(io.StringIO(message[len(prefix):])))
    except Exception:
        return None
    if len(fields) < 9:
        return None
    return {
        "class":       fields[3],
        "command":     fields[4],
        "object_type": fields[5],
        "object":      fields[6],
        "statement":   fields[7],
        "parameter":   fields[8],
    }


def collect_instance(inst):
    result = {
        "id":           inst["id"],
        "label":        inst["label"],
        "host":         inst["host"],
        "port":         inst["port"],
        "dbname":       inst["dbname"],
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "error":        None,
    }
    try:
        conn = psycopg2.connect(make_dsn(inst))
        conn.autocommit = True

        # --- Server version ------------------------------------------------
        result["version"]          = scalar(conn, "SELECT version()")
        result["pg_version_num"]   = scalar(conn, "SELECT current_setting('server_version_num')::int")
        result["pg_version_short"] = scalar(conn, "SHOW server_version")
        result["pg_latest_version"] = find_newer_pg_version(result["pg_version_short"])

        # --- Uptime --------------------------------------------------------
        result["server_start"]  = str(scalar(conn, "SELECT pg_postmaster_start_time()"))
        uptime_s = scalar(conn, "SELECT EXTRACT(EPOCH FROM (now() - pg_postmaster_start_time()))::bigint")
        result["uptime_seconds"] = int(uptime_s) if uptime_s else 0

        # --- Connections ---------------------------------------------------
        conn_rows = query(conn, """
            SELECT count(*) FILTER (WHERE state = 'active')              AS active,
                   count(*) FILTER (WHERE state = 'idle')                AS idle,
                   count(*) FILTER (WHERE state = 'idle in transaction') AS idle_in_txn,
                   count(*) FILTER (WHERE wait_event_type IS NOT NULL)   AS waiting,
                   count(*)                                              AS total,
                   current_setting('max_connections')::int               AS max_connections
            FROM pg_stat_activity
            WHERE backend_type = 'client backend'
        """)
        result["connections"] = conn_rows[0] if conn_rows else {}

        # --- Database stats ------------------------------------------------
        result["databases"] = query(conn, """
            SELECT datname,
                   pg_size_pretty(pg_database_size(datname)) AS size_pretty,
                   pg_database_size(datname)                 AS size_bytes,
                   numbackends,
                   xact_commit,
                   xact_rollback,
                   blks_read,
                   blks_hit,
                   CASE WHEN blks_read + blks_hit > 0
                        THEN ROUND(100.0 * blks_hit / (blks_read + blks_hit), 2)
                        ELSE NULL END                         AS cache_hit_ratio,
                   tup_inserted, tup_updated, tup_deleted,
                   deadlocks,
                   temp_files,
                   pg_size_pretty(temp_bytes) AS temp_bytes_pretty
            FROM pg_stat_database
            WHERE datname NOT IN ('template0','template1')
            ORDER BY size_bytes DESC
        """)

        # --- TPS -------------------------------------------------------------
        # A live rate (delta cumulative xacts / delta wall time between
        # polls), computed in collect_and_record — NOT "cumulative xacts /
        # time since stats_reset". stats_reset is NULL on some replicas,
        # which silently collapsed that division to a divide-by-1-second and
        # reported the raw, ever-growing cumulative transaction count as
        # "TPS" (a number that can only ever climb).
        result["xact_total"] = sum(
            (db.get("xact_commit") or 0) + (db.get("xact_rollback") or 0)
            for db in result["databases"]
        )
        result["tps"] = 0.0  # overwritten by collect_and_record once a previous sample exists

        # --- Bgwriter / checkpointer (PG 17+ split) ------------------------
        pgver = result.get("pg_version_num", 0)
        if pgver and pgver >= 170000:
            bgw = query(conn, """
                SELECT c.num_timed       AS checkpoints_timed,
                       c.num_requested   AS checkpoints_req,
                       c.buffers_written AS buffers_checkpoint,
                       b.buffers_clean,
                       c.buffers_written + b.buffers_clean + b.buffers_alloc AS buffers_backend,
                       b.buffers_alloc,
                       pg_size_pretty(c.buffers_written *
                           current_setting('block_size')::bigint)  AS chk_written,
                       ROUND(100.0 * b.buffers_clean /
                           NULLIF(c.buffers_written + b.buffers_clean, 0), 2) AS clean_ratio
                FROM pg_stat_bgwriter b, pg_stat_checkpointer c
            """)
        else:
            bgw = query(conn, """
                SELECT checkpoints_timed,
                       checkpoints_req,
                       buffers_checkpoint,
                       buffers_clean,
                       buffers_backend,
                       buffers_alloc,
                       pg_size_pretty(buffers_checkpoint *
                           current_setting('block_size')::bigint)  AS chk_written,
                       ROUND(100.0 * buffers_clean /
                           NULLIF(buffers_checkpoint + buffers_clean + buffers_backend, 0), 2) AS clean_ratio
                FROM pg_stat_bgwriter
            """)
        result["bgwriter"] = bgw[0] if bgw else {}

        # --- Replication ---------------------------------------------------
        result["replication"] = query(conn, """
            SELECT client_addr, state, sent_lsn, write_lsn, flush_lsn, replay_lsn,
                   pg_wal_lsn_diff(sent_lsn, replay_lsn) AS replay_lag_bytes
            FROM pg_stat_replication
        """)

        # --- Bloat / vacuum ------------------------------------------------
        result["bloat_tables"] = query(conn, """
            SELECT schemaname, relname,
                   n_live_tup, n_dead_tup,
                   CASE WHEN n_live_tup > 0
                        THEN ROUND(100.0 * n_dead_tup / n_live_tup, 1)
                        ELSE 0 END AS dead_ratio,
                   last_vacuum, last_autovacuum, last_analyze, last_autoanalyze
            FROM pg_stat_user_tables
            ORDER BY n_dead_tup DESC LIMIT 10
        """)

        # --- Long-running queries ------------------------------------------
        result["long_queries"] = query(conn, """
            SELECT pid, usename, datname, state,
                   EXTRACT(EPOCH FROM (now() - query_start))::int AS duration_s,
                   LEFT(query, 200) AS query,
                   wait_event_type, wait_event
            FROM pg_stat_activity
            WHERE state != 'idle'
              AND query_start IS NOT NULL
              AND EXTRACT(EPOCH FROM (now() - query_start)) > 1
            ORDER BY duration_s DESC LIMIT 20
        """)

        # --- Locks ---------------------------------------------------------
        result["locks"] = query(conn, """
            SELECT l.mode, l.locktype, l.granted, count(*) AS cnt
            FROM pg_locks l
            GROUP BY l.mode, l.locktype, l.granted
            ORDER BY cnt DESC LIMIT 20
        """)

        # --- WAL -------------------------------------------------------------
        # Actual on-disk size of the pg_wal directory (not the cumulative LSN
        # offset, which only ever grows and doesn't reflect recycled segments).
        try:
            wal = scalar(conn, "SELECT COALESCE(SUM(size), 0) FROM pg_ls_waldir()")
            result["wal_bytes_on_disk"] = int(wal)
        except Exception:
            result["wal_bytes_on_disk"] = None

        # --- pg_stat_statements -------------------------------------------
        try:
            result["top_queries"] = query(conn, """
                SELECT queryid::text AS queryid,
                       LEFT(query, 150) AS query,
                       calls,
                       ROUND(total_exec_time::numeric, 2)  AS total_ms,
                       ROUND(mean_exec_time::numeric,  2)  AS mean_ms,
                       ROUND(stddev_exec_time::numeric, 2) AS stddev_ms,
                       rows
                FROM pg_stat_statements
                ORDER BY total_exec_time DESC LIMIT 10
            """)
        except Exception:
            result["top_queries"] = []

        # --- EDB extensions (graceful degradation, [] = installed+empty) --
        for name, sql in [
            ("stat_monitor", "SELECT queryid::text AS queryid,MIN(LEFT(query,100)) AS query,"
             "datname AS dbname,username,"
             "string_agg(DISTINCT host(client_ip), ', ') AS client_ips,"
             "SUM(calls) AS calls,"
             "ROUND(SUM(total_exec_time)::numeric,2) AS total_ms,"
             "ROUND((SUM(total_exec_time)/NULLIF(SUM(calls),0))::numeric,2) AS mean_ms,"
             "SUM(cpu_user_time) AS cpu_user_time,SUM(cpu_sys_time) AS cpu_sys_time,"
             "MAX(bucket_start_time) AS last_seen "
             "FROM edb_stat_monitor GROUP BY queryid,datname,username "
             "ORDER BY total_ms DESC LIMIT 10"),
            ("wait_states",  "SELECT wait_event_type AS wait_type, wait_event, count(*) AS cnt "
             "FROM edb_wait_states_data() GROUP BY wait_event_type, wait_event ORDER BY cnt DESC LIMIT 20"),
            ("index_advisor","SELECT index AS recommendation, estimated_size_in_bytes, "
             "ROUND(estimated_pct_cost_reduction::numeric,1) AS est_cost_reduction_pct, "
             "ROUND(abs_benefit::numeric,2) AS abs_benefit, "
             "COALESCE(array_length(benefited_queryids,1),0) AS benefited_queries, "
             # Cast to text[] — these are bigint queryids (~1e18), and round-tripping
             # them through JSON as numbers silently corrupts them once a browser's
             # JS parses them (doubles only carry 53 bits of integer precision).
             "benefited_queryids::text[] AS benefited_queryids "
             "FROM query_advisor_index_recommendations() "
             "ORDER BY estimated_pct_cost_reduction DESC LIMIT 10"),
        ]:
            try:
                result[name] = query(conn, sql)
            except Exception:
                result[name] = []

        # --- system_stats extension ----------------------------------------
        # None = extension absent; dict = data collected
        ext_installed = {
            r["extname"]
            for r in query(conn, "SELECT extname FROM pg_extension "
                                  "WHERE extname IN ('system_stats','edb_pg_tuner')")
        }

        if "system_stats" in ext_installed:
            ss = {}
            def _ss_query(sql, single=False):
                """Run a system_stats function; return {} / [] on any error."""
                try:
                    rows = query(conn, sql)
                    return rows[0] if single else rows
                except Exception:
                    return {} if single else []
            # system_stats objects are FUNCTIONS — must be called with ()
            ss["os"]       = _ss_query("SELECT * FROM pg_sys_os_info()",        single=True)
            ss["cpu_info"] = _ss_query("SELECT * FROM pg_sys_cpu_info()")
            ss["cpu_usage"]= _ss_query("SELECT * FROM pg_sys_cpu_usage_info()",  single=True)
            ss["memory"]   = _ss_query("SELECT * FROM pg_sys_memory_info()",     single=True)
            ss["load_avg"] = _ss_query("SELECT * FROM pg_sys_load_avg_info()",   single=True)
            ss["io"]       = _ss_query("SELECT * FROM pg_sys_io_analysis_info()")
            ss["disk"]     = _ss_query("SELECT * FROM pg_sys_disk_info()")
            ss["network"]  = _ss_query("SELECT * FROM pg_sys_network_info()")
            result["system_stats"] = ss
        else:
            result["system_stats"] = None   # extension not installed

        # --- edb_pg_tuner extension -----------------------------------------
        # edb_pg_tuner_recommendations() returns SETOF text, one "param = value"
        # string per row (conf format) or "ALTER SYSTEM SET …;" (sql format).
        if "edb_pg_tuner" in ext_installed:
            try:
                # conf format: "param = value"  — parse into structured rows
                rows = query(conn,
                    "SELECT recommendation FROM edb_pg_tuner_recommendations() "
                    "ORDER BY 1")
                recs = []
                for r in rows:
                    raw = r.get("recommendation", "")
                    if "=" in raw:
                        param, _, val = raw.partition("=")
                        recs.append({"param": param.strip(), "recommended": val.strip()})
                    else:
                        recs.append({"param": raw.strip(), "recommended": ""})
                # Fetch current values for each param
                for rec in recs:
                    try:
                        rec["current"] = scalar(conn,
                            "SELECT current_setting(%s)", (rec["param"],))
                    except Exception:
                        rec["current"] = None
                # sql format: ready-to-run ALTER SYSTEM statements
                sql_rows = query(conn,
                    "SELECT recommendation FROM edb_pg_tuner_recommendations('sql') "
                    "ORDER BY 1")
                result["pg_tuner"] = {
                    "recommendations": recs,
                    "alter_sql": [r["recommendation"] for r in sql_rows],
                }
            except Exception as e:
                result["pg_tuner"] = {"error": str(e)}
        else:
            result["pg_tuner"] = None       # extension not installed

        # --- pgaudit (Tab 5: Audit Logs) ------------------------------------
        # pgaudit's GUCs only exist in pg_settings once it's in
        # shared_preload_libraries — their presence is how we tell "preloaded"
        # apart from merely "available to install". Community pgaudit writes
        # to the server log, not a table; EDB Advanced/Extended Server also
        # mirrors entries into edb_internals.pgaudit_log, which is queried
        # separately below.
        try:
            avail     = scalar(conn, "SELECT 1 FROM pg_available_extensions WHERE name = 'pgaudit'")
            installed = scalar(conn, "SELECT 1 FROM pg_extension WHERE extname = 'pgaudit'")
            setting_rows = query(conn,
                "SELECT name, setting FROM pg_settings WHERE name LIKE 'pgaudit.%' ORDER BY name")
            settings = {r["name"]: r["setting"] for r in setting_rows}
            log_setting = (settings.get("pgaudit.log") or "").strip().lower()
            result["pgaudit"] = {
                "available": bool(avail),
                "installed": bool(installed),
                "preloaded": bool(setting_rows),
                "enabled":   bool(log_setting) and log_setting != "none",
                "settings":  settings,
            }
            # EDB Postgres Advanced/Extended Server audits to a real table
            # (edb_internals.pgaudit_log) rather than the server log — pull
            # recent entries when it's present. Nested try so its absence on
            # community Postgres doesn't blank out the settings above.
            try:
                log_rows = query(conn, """
                    SELECT log_time::text AS log_time, user_name, command_tag, message
                    FROM edb_internals.pgaudit_log
                    ORDER BY log_time DESC
                    LIMIT 500
                """)
                for r in log_rows:
                    r["parsed"] = _parse_pgaudit_message(r["message"])
                result["pgaudit"]["log_entries"] = log_rows
            except Exception:
                result["pgaudit"]["log_entries"] = None
        except Exception as e:
            result["pgaudit"] = {"error": str(e)}

        # --- Tablespaces ---------------------------------------------------
        result["tablespaces"] = query(conn, """
            SELECT spcname,
                   pg_size_pretty(pg_tablespace_size(spcname)) AS size_pretty,
                   pg_tablespace_size(spcname)                  AS size_bytes
            FROM pg_tablespace
            ORDER BY size_bytes DESC NULLS LAST
        """)

        # --- Key settings --------------------------------------------------
        result["settings"] = query(conn, """
            SELECT name, setting, unit, source
            FROM pg_settings
            WHERE name IN (
                'max_connections','shared_buffers','work_mem','maintenance_work_mem',
                'effective_cache_size','checkpoint_completion_target',
                'wal_level','max_wal_size','autovacuum','log_min_duration_statement',
                'track_activity_query_size','default_statistics_target',
                'random_page_cost','effective_io_concurrency','wal_buffers',
                'wal_compression','archive_mode','synchronous_commit',
                'max_worker_processes','max_parallel_workers'
            )
            ORDER BY name
        """)

        conn.close()
    except Exception as e:
        result["error"]     = str(e)
        result["traceback"] = traceback.format_exc()

    return result


def _avg_cache_hit(databases):
    vals = [float(db["cache_hit_ratio"]) for db in databases if db.get("cache_hit_ratio") is not None]
    return round(sum(vals) / len(vals), 2) if vals else 0


def collect_and_record(inst):
    """collect_instance(), plus append this sample to the instance's rolling
    trend history and attach that history to the result. A failed collection
    (result["error"] set) doesn't add a sample — it just carries the existing
    history forward so the charts don't get a false zero."""
    result = collect_instance(inst)
    inst_id = inst["id"]
    with _history_lock:
        h = _history.get(inst_id)
        if h is None:
            # First time this instance is seen in this process — prime the
            # in-memory cache from disk so a restart doesn't blank the charts.
            h = {
                "ts":    deque(maxlen=HISTORY_MAXLEN),
                "tps":   deque(maxlen=HISTORY_MAXLEN),
                "conn":  deque(maxlen=HISTORY_MAXLEN),
                "cache": deque(maxlen=HISTORY_MAXLEN),
            }
            for ts_iso, tps_v, conn_v, cache_v in load_trend_history(inst_id, HISTORY_MAXLEN):
                h["ts"].append(_short_time(ts_iso))
                h["tps"].append(tps_v)
                h["conn"].append(conn_v)
                h["cache"].append(cache_v)
            _history[inst_id] = h
        if not result.get("error"):
            # TPS as a live rate: delta cumulative xacts / delta wall time
            # since the previous poll. Falls back to 0 on the first sample
            # (no baseline yet) or if the counter went backwards (stats
            # reset / failover between polls).
            now_m = time.monotonic()
            xact_total = result.get("xact_total") or 0
            prev = _prev_xact.get(inst_id)
            tps = 0.0
            if prev is not None:
                prev_time, prev_total = prev
                elapsed = now_m - prev_time
                if elapsed > 0 and xact_total >= prev_total:
                    tps = round((xact_total - prev_total) / elapsed, 2)
            _prev_xact[inst_id] = (now_m, xact_total)
            result["tps"] = tps

            now_dt = datetime.now(timezone.utc)
            conn_total = (result.get("connections") or {}).get("total") or 0
            cache_hit = _avg_cache_hit(result.get("databases") or [])
            h["ts"].append(now_dt.strftime("%H:%M:%S"))
            h["tps"].append(tps)
            h["conn"].append(conn_total)
            h["cache"].append(cache_hit)
            record_trend_sample(inst_id, now_dt.isoformat(), tps, conn_total, cache_hit)
        result["history"] = {k: list(v) for k, v in h.items()}
    return result


# ---------------------------------------------------------------------------
# Background refresh
# ---------------------------------------------------------------------------

def refresh_all():
    current = get_instances()
    results = {}
    threads = []

    def worker(inst):
        results[inst["id"]] = collect_and_record(inst)

    for inst in current:
        t = threading.Thread(target=worker, args=(inst,), daemon=True)
        threads.append(t)
        t.start()
    for t in threads:
        t.join()

    with _cache_lock:
        # Remove stale entries for deleted instances
        active_ids = {i["id"] for i in current}
        stale = [k for k in _cache if not k.startswith("_") and k not in active_ids]
        for k in stale:
            del _cache[k]
        _cache.update(results)
        _cache["_refreshed"] = datetime.now(timezone.utc).isoformat()

    with _history_lock:
        for k in [k for k in _history if k not in active_ids]:
            del _history[k]
        for k in [k for k in _prev_xact if k not in active_ids]:
            del _prev_xact[k]


def background_refresh():
    while True:
        try:
            refresh_all()
        except Exception:
            pass
        time.sleep(10)


# ---------------------------------------------------------------------------
# API — instances CRUD
# ---------------------------------------------------------------------------

@app.route("/api/instances", methods=["GET"])
def api_get_instances():
    return jsonify(get_instances())


@app.route("/api/instances", methods=["POST"])
def api_add_instance():
    body = request.get_json(force=True)
    required = ("host", "port", "user", "password", "dbname")
    missing = [f for f in required if not body.get(f)]
    if missing:
        return jsonify({"error": f"Missing fields: {', '.join(missing)}"}), 400

    # Groups exist only implicitly: any non-empty string is a valid group,
    # and groups with no remaining member instances simply cease to exist.
    group = (body.get("group") or "").strip() or None

    new_id = "pg-" + uuid.uuid4().hex[:8]
    inst = {
        "id":       new_id,
        "label":    body.get("label") or f"{body['host']}:{body['port']}",
        "host":     body["host"],
        "port":     int(body["port"]),
        "user":     body["user"],
        "password": body["password"],
        "dbname":   body["dbname"],
        "group":    group,
    }

    with _instances_lock:
        _instances.append(inst)
        save_instances(_instances)

    # Collect immediately in background so the UI gets data quickly
    threading.Thread(target=lambda: _cache.__setitem__(new_id, collect_and_record(inst))
                                     or None, daemon=True).start()

    return jsonify(inst), 201


@app.route("/api/instances/<instance_id>", methods=["DELETE"])
def api_remove_instance(instance_id):
    with _instances_lock:
        before = len(_instances)
        remaining = [i for i in _instances if i["id"] != instance_id]
        if len(remaining) == before:
            return jsonify({"error": "not found"}), 404
        _instances[:] = remaining
        save_instances(_instances)

    with _cache_lock:
        _cache.pop(instance_id, None)
    with _history_lock:
        _history.pop(instance_id, None)
        _prev_xact.pop(instance_id, None)
    delete_trend_history(instance_id)

    return jsonify({"ok": True})


@app.route("/api/instances/<instance_id>", methods=["PUT"])
def api_update_instance(instance_id):
    body = request.get_json(force=True)

    with _instances_lock:
        inst = next((i for i in _instances if i["id"] == instance_id), None)
        if not inst:
            return jsonify({"error": "not found"}), 404
        if "label"    in body: inst["label"]    = body["label"]
        if "host"     in body: inst["host"]      = body["host"]
        if "port"     in body: inst["port"]      = int(body["port"])
        if "user"     in body: inst["user"]      = body["user"]
        if "password" in body: inst["password"]  = body["password"]
        if "dbname"   in body: inst["dbname"]    = body["dbname"]
        if "group"    in body: inst["group"]     = (body["group"] or "").strip() or None
        save_instances(_instances)
        updated = dict(inst)

    # Re-collect immediately so the UI refreshes with the new connection
    threading.Thread(
        target=lambda: _cache.__setitem__(instance_id, collect_and_record(updated)),
        daemon=True
    ).start()
    return jsonify(updated)


@app.route("/api/instances/<instance_id>/refresh", methods=["POST"])
def api_refresh_instance(instance_id):
    inst = next((i for i in get_instances() if i["id"] == instance_id), None)
    if not inst:
        return jsonify({"error": "not found"}), 404
    data = collect_and_record(inst)
    with _cache_lock:
        _cache[instance_id] = data
        _cache["_refreshed"] = datetime.now(timezone.utc).isoformat()
    return jsonify(data)


@app.route("/api/instances/<instance_id>/explain", methods=["POST"])
def api_explain(instance_id):
    """Look up a query by queryid (from pg_stat_statements or edb_stat_monitor)
    and EXPLAIN it.

    Query text from either source is normalized ($1, $2, …) — there are no
    real parameter values to substitute. On PG16+ we use EXPLAIN's
    GENERIC_PLAN option, built for exactly this case. On older versions we
    fall back to substituting NULL for each placeholder, which yields a
    structurally correct but row-estimate-inaccurate plan.
    """
    body = request.get_json(force=True) or {}
    queryid = body.get("queryid")
    source = body.get("source", "pg_stat_statements")
    if not queryid:
        return jsonify({"error": "missing queryid"}), 400
    if source not in ("pg_stat_statements", "stat_monitor"):
        return jsonify({"error": "invalid source"}), 400
    source_table = "edb_stat_monitor" if source == "stat_monitor" else "pg_stat_statements"

    inst = next((i for i in get_instances() if i["id"] == instance_id), None)
    if not inst:
        return jsonify({"error": "unknown instance"}), 404

    try:
        conn = psycopg2.connect(make_dsn(inst))
        conn.autocommit = True
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    # pg_stat_statements/edb_stat_monitor are cluster-wide — a query logged
    # against a different database than the instance's configured default
    # (e.g. "app" vs. "postgres") is still visible here, but EXPLAINing it
    # over this connection would fail with "relation does not exist" since a
    # connection only sees objects in its own database. Look up which
    # database it actually ran against so we can reconnect to that one below.
    try:
        if source_table == "pg_stat_statements":
            row = query(conn, """
                SELECT s.query AS query, d.datname AS dbname
                FROM pg_stat_statements s
                JOIN pg_database d ON d.oid = s.dbid
                WHERE s.queryid = %s::bigint
                LIMIT 1
            """, (queryid,))
        else:
            row = query(conn, """
                SELECT query, datname AS dbname
                FROM edb_stat_monitor
                WHERE queryid = %s::bigint
                LIMIT 1
            """, (queryid,))
        if not row:
            return jsonify({"error": f"query not found — it may have aged out of {source_table}"}), 404
        full_query, query_dbname = row[0]["query"], row[0]["dbname"]
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

    target = dict(inst, dbname=query_dbname) if query_dbname else inst
    try:
        conn = psycopg2.connect(make_dsn(target))
        conn.autocommit = True
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    try:
        pgver = scalar(conn, "SELECT current_setting('server_version_num')::int")
        try:
            if pgver and pgver >= 160000:
                rows = query(conn, f"EXPLAIN (GENERIC_PLAN, FORMAT TEXT) {full_query}")
                mode = "generic_plan"
            else:
                approx_query = re.sub(r'\$\d+', 'NULL', full_query)
                rows = query(conn, f"EXPLAIN (FORMAT TEXT) {approx_query}")
                mode = "approximate"
            plan_text = "\n".join(r["QUERY PLAN"] for r in rows)
            return jsonify({"plan": plan_text, "mode": mode, "query": full_query, "dbname": query_dbname})
        except Exception as e:
            return jsonify({"error": str(e), "query": full_query}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/instances/<instance_id>/index-recommendation-queries", methods=["POST"])
def api_index_recommendation_queries(instance_id):
    """Resolve the pg_stat_statements queryids an index recommendation benefits."""
    body = request.get_json(force=True) or {}
    queryids = body.get("queryids")
    if not queryids or not isinstance(queryids, list):
        return jsonify({"error": "missing queryids"}), 400

    inst = next((i for i in get_instances() if i["id"] == instance_id), None)
    if not inst:
        return jsonify({"error": "unknown instance"}), 404

    try:
        conn = psycopg2.connect(make_dsn(inst))
        conn.autocommit = True
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    try:
        rows = query(
            conn,
            "SELECT queryid::text AS queryid, query, calls, "
            "ROUND(total_exec_time::numeric,2) AS total_ms, "
            "ROUND(mean_exec_time::numeric,2) AS mean_ms "
            "FROM pg_stat_statements WHERE queryid = ANY(%s::bigint[]) "
            "ORDER BY total_exec_time DESC",
            (queryids,),
        )
        return jsonify({"queries": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/instances/<instance_id>/database-schema", methods=["POST"])
def api_database_schema(instance_id):
    """Schemas/tables/columns/indexes for one database on this instance.

    The instance's stored connection targets one specific dbname, but the
    Databases table lists every database on the server — so this opens a
    fresh connection with dbname swapped to whichever one was clicked.
    """
    body = request.get_json(force=True) or {}
    dbname = body.get("dbname")
    if not dbname:
        return jsonify({"error": "missing dbname"}), 400

    inst = next((i for i in get_instances() if i["id"] == instance_id), None)
    if not inst:
        return jsonify({"error": "unknown instance"}), 404

    target = dict(inst, dbname=dbname)
    try:
        conn = psycopg2.connect(make_dsn(target))
        conn.autocommit = True
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    try:
        schemas = [r["nspname"] for r in query(conn, """
            SELECT nspname FROM pg_namespace
            WHERE nspname NOT IN ('pg_catalog','information_schema','pg_toast')
              AND nspname NOT LIKE 'pg_temp%%' AND nspname NOT LIKE 'pg_toast_temp%%'
            ORDER BY nspname
        """)]

        tables = query(conn, """
            SELECT n.nspname AS schema, c.relname AS name,
                   pg_size_pretty(pg_total_relation_size(c.oid)) AS size_pretty,
                   GREATEST(c.reltuples::bigint, 0) AS row_estimate
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'r' AND n.nspname = ANY(%s)
            ORDER BY n.nspname, c.relname
        """, (schemas,))

        columns = query(conn, """
            SELECT table_schema AS schema, table_name, column_name AS name,
                   data_type AS type, is_nullable = 'YES' AS nullable, column_default AS default
            FROM information_schema.columns
            WHERE table_schema = ANY(%s)
            ORDER BY table_schema, table_name, ordinal_position
        """, (schemas,))

        indexes = query(conn, """
            SELECT n.nspname AS schema, t.relname AS table_name, i.relname AS name,
                   pg_get_indexdef(ix.indexrelid) AS def,
                   pg_size_pretty(pg_relation_size(ix.indexrelid)) AS size_pretty
            FROM pg_index ix
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_class t ON t.oid = ix.indrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname = ANY(%s)
            ORDER BY n.nspname, t.relname, i.relname
        """, (schemas,))

        cols_by_table = {}
        for c in columns:
            cols_by_table.setdefault((c["schema"], c["table_name"]), []).append(c)
        idx_by_table = {}
        for i in indexes:
            idx_by_table.setdefault((i["schema"], i["table_name"]), []).append(i)

        for t in tables:
            key = (t["schema"], t["name"])
            t["columns"] = cols_by_table.get(key, [])
            t["indexes"] = idx_by_table.get(key, [])

        return jsonify({"schemas": schemas, "tables": tables})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


SQL_CONSOLE_ROW_LIMIT = 1000


def _jsonable(v):
    """Coerce a psycopg2 result value into something jsonify can serialize
    without mangling it — dates/times as ISO text, Decimal as float, raw
    binary as hex, rather than relying on Flask's generic fallback."""
    if isinstance(v, (datetime, date, dt_time)):
        return v.isoformat()
    if isinstance(v, decimal.Decimal):
        return float(v)
    if isinstance(v, (bytes, bytearray, memoryview)):
        return bytes(v).hex()
    if isinstance(v, uuid.UUID):
        return str(v)
    return v


@app.route("/api/instances/<instance_id>/sql", methods=["POST"])
def api_run_sql(instance_id):
    """Lightweight SQL console — run arbitrary SQL against one database on
    this instance (like a browser-based psql) and return either the result
    set or a rowcount/status. One connection per request, autocommit, no
    session state across calls — same trust model as the EXPLAIN/schema
    endpoints above, which already run admin-supplied SQL text.
    """
    body = request.get_json(force=True) or {}
    sql = (body.get("sql") or "").strip()
    dbname = body.get("dbname")
    if not sql:
        return jsonify({"error": "missing sql"}), 400

    inst = next((i for i in get_instances() if i["id"] == instance_id), None)
    if not inst:
        return jsonify({"error": "unknown instance"}), 404

    target = dict(inst, dbname=dbname) if dbname else inst
    try:
        conn = psycopg2.connect(make_dsn(target))
        conn.autocommit = True
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    try:
        started = time.time()
        with conn.cursor() as cur:
            cur.execute(sql)
            elapsed_ms = round((time.time() - started) * 1000, 1)
            if cur.description is not None:
                cols = [d.name for d in cur.description]
                raw_rows = cur.fetchmany(SQL_CONSOLE_ROW_LIMIT + 1)
                truncated = len(raw_rows) > SQL_CONSOLE_ROW_LIMIT
                raw_rows = raw_rows[:SQL_CONSOLE_ROW_LIMIT]
                rows = [[_jsonable(v) for v in row] for row in raw_rows]
                return jsonify({
                    "columns": cols, "rows": rows, "row_count": len(rows),
                    "truncated": truncated, "elapsed_ms": elapsed_ms,
                    "status": cur.statusmessage,
                })
            return jsonify({
                "columns": None, "rows": [], "row_count": cur.rowcount,
                "truncated": False, "elapsed_ms": elapsed_ms,
                "status": cur.statusmessage,
            })
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# API — metrics
# ---------------------------------------------------------------------------

@app.route("/api/metrics")
def api_metrics():
    with _cache_lock:
        data = dict(_cache)
    return jsonify(data)


@app.route("/api/metrics/<instance_id>")
def api_metrics_instance(instance_id):
    with _cache_lock:
        data = _cache.get(instance_id)
    if data is None:
        return jsonify({"error": "unknown instance"}), 404
    return jsonify(data)


@app.route("/api/instances/<instance_id>/trend-history")
def api_trend_history(instance_id):
    """Longer trend look-back than the HISTORY_MAXLEN embedded in every
    /api/metrics poll, read from the on-disk sample store. Timestamps are
    formatted with a date component since a multi-hour window can cross
    midnight, unlike the short in-memory history's bare HH:MM:SS."""
    try:
        samples = int(request.args.get("samples", HISTORY_MAXLEN))
    except (TypeError, ValueError):
        return jsonify({"error": "invalid samples"}), 400
    samples = max(1, min(samples, TREND_RETENTION_SAMPLES))

    rows = load_trend_history(instance_id, samples)
    return jsonify({
        "ts":    [_fmt_deep_ts(r[0]) for r in rows],
        "tps":   [r[1] for r in rows],
        "conn":  [r[2] for r in rows],
        "cache": [r[3] for r in rows],
    })


def _fmt_deep_ts(ts_iso):
    try:
        return datetime.fromisoformat(ts_iso).strftime("%m/%d %H:%M")
    except Exception:
        return ts_iso


# ---------------------------------------------------------------------------
# Static files
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.route("/<path:path>")
def static_files(path):
    # Never serve static files for API routes — return JSON 404 instead.
    if path.startswith("api/"):
        return jsonify({"error": "not found"}), 404
    try:
        return send_from_directory("static", path)
    except Exception:
        return send_from_directory("static", "index.html")


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=5000)
    args = parser.parse_args()

    print("Initializing trend history store …")
    init_trend_db()
    print("Initial data collection …")
    refresh_all()
    print("Starting background refresh thread …")
    threading.Thread(target=background_refresh, daemon=True).start()
    print(f"Dashboard running at http://localhost:{args.port}")
    app.run(host="0.0.0.0", port=args.port, debug=False, threaded=True)
