"""
PostgreSQL Multi-Instance Dashboard
Flask backend — dynamic instance list with add/remove API.
"""

import json
import os
import re
import time
import threading
import traceback
import urllib.request
import uuid
from datetime import datetime, timezone
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

        # --- TPS -----------------------------------------------------------
        tps = scalar(conn, """
            SELECT ROUND(
                SUM(xact_commit + xact_rollback)::numeric /
                GREATEST(EXTRACT(EPOCH FROM (now() - MIN(stats_reset))), 1), 2)
            FROM pg_stat_database
            WHERE datname NOT IN ('template0','template1')
        """)
        result["tps"] = float(tps) if tps else 0.0

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

        # --- WAL -----------------------------------------------------------
        try:
            wal = scalar(conn, "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')")
            result["wal_bytes_total"] = int(wal) if wal else 0
        except Exception:
            result["wal_bytes_total"] = None

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
            ("stat_monitor", "SELECT bucket_start_time,datname AS dbname,username,LEFT(query,100) AS query,"
             "calls,ROUND(total_exec_time::numeric,2) AS total_ms,"
             "ROUND(mean_exec_time::numeric,2) AS mean_ms,cpu_user_time,cpu_sys_time "
             "FROM edb_stat_monitor ORDER BY total_exec_time DESC LIMIT 10"),
            ("wait_states",  "SELECT wait_event_type AS wait_type, wait_event, count(*) AS cnt "
             "FROM edb_wait_states_data() GROUP BY wait_event_type, wait_event ORDER BY cnt DESC LIMIT 20"),
            ("index_advisor","SELECT index AS recommendation, estimated_size_in_bytes, "
             "ROUND(estimated_pct_cost_reduction::numeric,1) AS est_cost_reduction_pct, "
             "ROUND(abs_benefit::numeric,2) AS abs_benefit, "
             "COALESCE(array_length(benefited_queryids,1),0) AS benefited_queries, "
             "benefited_queryids "
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


# ---------------------------------------------------------------------------
# Background refresh
# ---------------------------------------------------------------------------

def refresh_all():
    current = get_instances()
    results = {}
    threads = []

    def worker(inst):
        results[inst["id"]] = collect_instance(inst)

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
    threading.Thread(target=lambda: _cache.__setitem__(new_id, collect_instance(inst))
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
        target=lambda: _cache.__setitem__(instance_id, collect_instance(updated)),
        daemon=True
    ).start()
    return jsonify(updated)


@app.route("/api/instances/<instance_id>/refresh", methods=["POST"])
def api_refresh_instance(instance_id):
    inst = next((i for i in get_instances() if i["id"] == instance_id), None)
    if not inst:
        return jsonify({"error": "not found"}), 404
    data = collect_instance(inst)
    with _cache_lock:
        _cache[instance_id] = data
        _cache["_refreshed"] = datetime.now(timezone.utc).isoformat()
    return jsonify(data)


@app.route("/api/instances/<instance_id>/explain", methods=["POST"])
def api_explain(instance_id):
    """Look up a pg_stat_statements query by queryid and EXPLAIN it.

    Query text from pg_stat_statements is normalized ($1, $2, …) — there are
    no real parameter values to substitute. On PG16+ we use EXPLAIN's
    GENERIC_PLAN option, built for exactly this case. On older versions we
    fall back to substituting NULL for each placeholder, which yields a
    structurally correct but row-estimate-inaccurate plan.
    """
    body = request.get_json(force=True) or {}
    queryid = body.get("queryid")
    if not queryid:
        return jsonify({"error": "missing queryid"}), 400

    inst = next((i for i in get_instances() if i["id"] == instance_id), None)
    if not inst:
        return jsonify({"error": "unknown instance"}), 404

    try:
        conn = psycopg2.connect(make_dsn(inst))
        conn.autocommit = True
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    try:
        full_query = scalar(
            conn,
            "SELECT query FROM pg_stat_statements WHERE queryid = %s::bigint LIMIT 1",
            (queryid,),
        )
        if not full_query:
            return jsonify({"error": "query not found — it may have aged out of pg_stat_statements"}), 404

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
            return jsonify({"plan": plan_text, "mode": mode, "query": full_query})
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
            "SELECT queryid, query, calls, ROUND(total_exec_time::numeric,2) AS total_ms, "
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

    print("Initial data collection …")
    refresh_all()
    print("Starting background refresh thread …")
    threading.Thread(target=background_refresh, daemon=True).start()
    print(f"Dashboard running at http://localhost:{args.port}")
    app.run(host="0.0.0.0", port=args.port, debug=False, threaded=True)
