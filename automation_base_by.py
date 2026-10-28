#!/usr/bin/env python3
"""
automation_base_by.py

Merged Bayern import automation. Runs BOTH jobs, in order, on every invocation:

  JOB 1  BUILDINGS   (from by/building/fetch_building.py)
         - Download Bayern Hausumringe zips (7 Bezirke), unzip, import ALL SHP
           data into ONE PostGIS table: hausumringe.
         - Populate local table "remote_address" by fetching from a remote
           PostgreSQL DB (house JOIN street, filtered to ags_key LIKE '09%'
           and quality IN A/B/C).

         Local table remote_address schema:
           id (TEXT, PK)
           street_name, street_id, house_number, postcode, locality,
           component_locality, district, federal_state, rs_key, ags_key (TEXT)
           house_center_point_geom   (geometry(Point,   25832))
           house_bbox_geom           (geometry(Polygon, 25832))
           street_center_point_geom  (geometry(Point,   25832))
           street_bbox_geom          (geometry(Polygon, 25832))

         Live DB: by_alkis_building

  JOB 2  USAGE       (from by/usage/fetch_usage.py)
         - Download Nutzung_kreis.gpkg, then import ALL layers into ONE PostGIS
           table named after the gpkg filename (nutzung_kreis).
         - Adds gml_id derived from the source 'oid' field, and src_layer to
           keep the original layer name.
         - After import, performs hard validation before declaring the database
           usable. If validation fails, this job exits non-zero and MUST NOT be
           treated as a successfully generated replacement database.

         Live DB: by_alkis_usage
         (the destination TABLE stays nutzung_kreis)

DATABASE LIFECYCLE
  Every job moves through four names. For job <job> and the jobID argument:

      by_alkis_<job>_<jobID>_importing   created first (from alkis_template);
                                        the suffix means the import has not
                                        finished
      by_alkis_<job>_<jobID>             suffix dropped once the job completes
      by_alkis_<job>                     LIVE -- what consumers read
      by_alkis_<job>_old                 previous live, kept for rollback

  Promotion happens only after EVERY job has completed. It then, for each job:
    1. pg_dumps the live and the working database into
       <backup_dir>/by/<jobID>/;
    2. zips the archives downloaded this run into the same folder;
    3. drops by_alkis_<job>_old if it exists;
    4. renames the live database to by_alkis_<job>_old;
    5. renames the working database to by_alkis_<job>;
    6. deletes everything in the download directory.

  Steps 1-2 run for ALL jobs before any rename, so a failed backup aborts while
  the live set is still in place. Step 6 runs only after the swap has fully
  succeeded.

  PostgreSQL refuses ALTER DATABASE ... RENAME and DROP DATABASE while any
  session is attached, so each rename/drop is preceded by a connection drain
  that polls pg_stat_activity every [promote] conn_wait_interval_seconds for at
  most conn_wait_timeout_seconds. A session is terminated if its state is
  'idle', or if its application_name matches [promote] force_terminate_app_names
  and not force_terminate_exclude -- which is how pgAdmin's Dashboard poller,
  permanently 'active', gets cleared. Real queries, open transactions and
  pgAdmin Query Tool tabs are waited on, and on timeout the swap is abandoned
  with the live databases untouched.

STRUCTURE NOTE
  The two jobs are kept deliberately independent. Each job carries its own
  private copy of the six helpers whose names collided between the two source
  files, prefixed _bld_* and _usg_* respectively:

      _bld_download_file   / _usg_download_file   (timeouts differ: 240 / 180)
      _bld_run             / _usg_run
      _bld_connect         / _usg_connect
      _bld_ensure_database / _usg_ensure_database
      _bld_require_postgis / _usg_require_postgis
      _bld_pg_dsn          / _usg_pg_dsn

  Editing one job's copy therefore cannot affect the other job. The remaining
  job-specific functions had no name overlap and are carried over unchanged.
  Each section also retains its original source formatting.

FAILURE BEHAVIOUR
  Per-job reporting matches the originals:
    - JOB 1 (buildings) has no validation gate and no banner; a failure prints a
      traceback.
    - JOB 2 (usage) keeps its hard validation gate and its
      "IMPORT FAILED / DATABASE MUST NOT BE PROMOTED" banner.

  A failure in one job does NOT prevent the other from running -- they import
  into separate databases. But promotion is ALL-OR-NOTHING: if any job failed,
  nothing is promoted, the live databases are left exactly as they were, and the
  working databases are kept for inspection. The process exits 1.

CONFIGURATION
  All connection details, urls and table names come from config.py, which reads
  the project's config.ini from beside itself -- there are no hardcoded
  fallbacks and no --host/--port flags. Keys in the job groups shared with
  automation_base_st.py are state-prefixed; only the by_* ones are read here.
  Sections:

      [paths]            backup_dir                         (shared with main.py)
      [promote]          conn_wait_interval_seconds,
                         conn_wait_timeout_seconds,
                         force_terminate_app_names,
                         force_terminate_exclude            (shared, unprefixed)
      [database]         host, port, user, password         (shared with main.py)
      [building]         by_URLS                  (one zip url per line)
      [remote_database]  TABLE_NAME, REMOTE_TABLE_NAME,
                         remote_pg_user, remote_pg_password,
                         remote_pg_database, remote_pg_host, remote_pg_port
      [usage]            by_GPKG_URL

  config.ini holds plaintext database passwords: chmod 600 it.

Requires:
  sudo apt-get update && sudo apt-get install -y gdal-bin postgresql-client
  pip install requests psycopg2-binary
  (ogr2ogr, ogrinfo and pg_dump must be on PATH)

DOWNLOADS AND BACKUPS
  Source files are downloaded to a fixed, project-relative directory:

      <dir containing this script>/_downloads/by

  There is no --workdir flag. After a successful promotion the downloaded
  archives are zipped into the backup folder and the directory is emptied:

      <backup_dir>/by/<jobID>/
          by_alkis_<job>.dump                each outgoing live database
          by_alkis_<job>_<jobID>.dump        each newly imported database
          by_downloads_<jobID>_<ts>.zip     the archives fetched this run

  The zip holds only the files fetched from the source urls, not the directories
  they were unpacked into -- those are reproducible from the archives and would
  multiply the size. It is stored uncompressed, since the sources are already
  compressed.

  NOTE: the download directory is NOT jobID-scoped. Two job IDs running
  concurrently share it and will overwrite each other's files.

Run (from any directory):
  python automation_base_by.py <jobID>

  Re-running a jobID whose working database still exists is refused, because
  that database holds a completed but unpromoted import. Drop it or use a new
  jobID. A leftover *_importing database is scratch and is dropped
  automatically at pre-flight.
"""

import argparse
import contextlib
import os
import re
import shutil
import subprocess
import sys
import time
import traceback
import zipfile
from pathlib import Path

import requests
import psycopg2
import psycopg2.extras
from psycopg2 import sql, errors

import check_update
import config
import state_db


# ============================================================================
# CONFIGURATION
#
# Every value below is taken from config.py by load_config(), which main()
# calls before either job runs. Nothing here has a
# hardcoded fallback: a missing section or key is a hard error, so a typo in the
# config file fails immediately and loudly rather than silently importing the
# wrong database.
#
# The per-job values (URLS, TABLE_NAME, REMOTE_TABLE_NAME, remote_pg_*,
# GPKG_URL) are declared in their own job sections further down, next to the
# code that uses them.
# ============================================================================
CONFIG_FILENAME = "config.ini"

# Every job creates its working database from this template. CREATE DATABASE
# ... TEMPLATE requires ZERO sessions on the TEMPLATE, not just on the new
# database, so it is drained first -- see run_*_job.
TEMPLATE_DB = "alkis_template"

# Database naming. For each job the four names are, in lifecycle order:
#
#   <state>_alkis_<job>_<jobID>_importing   created first; suffix means unfinished
#   <state>_alkis_<job>_<jobID>             suffix dropped once the job completes
#   <state>_alkis_<job>                     live; what consumers read
#   <state>_alkis_<job>_old                 previous live, kept for rollback
STATE = "by"

# Downloads always live under the project, never a CLI argument:
#     <dir containing this script>/_downloads/<state>
# Resolved from __file__ rather than the cwd so a cron entry does not have to
# care where it was launched from.
PROJECT_DIR = Path(__file__).resolve().parent
DOWNLOAD_DIR = PROJECT_DIR / "_downloads" / STATE

# Archives actually fetched from the source urls this run, recorded by each job
# as it downloads. Only these go into the backup zip -- not the directories they
# are unpacked into.
DOWNLOADED_ARCHIVES: list[Path] = []

# [promote]
BACKUP_DIR: str | None = None
CONN_WAIT_INTERVAL_SECONDS: int | None = None
CONN_WAIT_TIMEOUT_SECONDS: int | None = None
FORCE_TERMINATE_APP_NAMES: list[str] = []
FORCE_TERMINATE_EXCLUDE: list[str] = []

# [database]
PG_HOST: str | None = None
PG_PORT: int | None = None
PG_USER: str | None = None
PG_PASSWORD: str | None = None


def _require(value, key: str):
    """A value from config.py, or a hard error naming the config.ini key."""
    if value is None:
        raise RuntimeError(f"{CONFIG_FILENAME}: {key} is not set")
    return value


def load_config() -> None:
    """
    Populate the module-level configuration globals from config.py.

    config.py reads config.ini from beside itself, so the working directory no
    longer matters. It treats this script's keys as optional so the rest of the
    project imports without them; every one is required here.
    """
    global PG_HOST, PG_PORT, PG_USER, PG_PASSWORD
    global BACKUP_DIR, CONN_WAIT_INTERVAL_SECONDS, CONN_WAIT_TIMEOUT_SECONDS
    global FORCE_TERMINATE_APP_NAMES, FORCE_TERMINATE_EXCLUDE
    global URLS
    global TABLE_NAME, REMOTE_TABLE_NAME
    global remote_pg_user, remote_pg_password, remote_pg_database
    global remote_pg_host, remote_pg_port
    global GPKG_URL

    # ---- [database] -- local target PostgreSQL, shared by both jobs ----
    # The same login main.py uses, so ALKIS_DB_USER / ALKIS_DB_PASSWORD apply.
    PG_HOST = _require(config.DB_HOST or None, "[database] host")
    PG_PORT = int(_require(config.DB_PORT or None, "[database] port"))
    PG_USER = config.DB_USER
    PG_PASSWORD = config.DB_PASSWORD

    # ---- [building] -- one Hausumringe zip URL per line ----
    URLS = _require(config.BY_BUILDING_URLS, "[building] by_URLS")
    if not URLS:
        raise RuntimeError(
            "Config [building] by_URLS is empty; at least one zip URL is required."
        )

    # ---- [remote_database] ----
    TABLE_NAME = _require(config.BY_TABLE_NAME, "[remote_database] TABLE_NAME")
    REMOTE_TABLE_NAME = _require(config.BY_REMOTE_TABLE_NAME, "[remote_database] REMOTE_TABLE_NAME")
    remote_pg_user = _require(config.BY_REMOTE_PG_USER, "[remote_database] remote_pg_user")
    remote_pg_password = _require(config.BY_REMOTE_PG_PASSWORD, "[remote_database] remote_pg_password")
    remote_pg_database = _require(config.BY_REMOTE_PG_DATABASE, "[remote_database] remote_pg_database")
    remote_pg_host = _require(config.BY_REMOTE_PG_HOST, "[remote_database] remote_pg_host")
    remote_pg_port = _require(config.BY_REMOTE_PG_PORT, "[remote_database] remote_pg_port")

    # ---- [usage] ----
    GPKG_URL = _require(config.BY_GPKG_URL, "[usage] by_GPKG_URL")

    # ---- backup location ([paths]) and the connection-drain timings ([promote]) ----
    BACKUP_DIR = str(config.BACKUP_DIR)
    CONN_WAIT_INTERVAL_SECONDS = _require(config.CONN_WAIT_INTERVAL_SECONDS, "[promote] conn_wait_interval_seconds")
    CONN_WAIT_TIMEOUT_SECONDS = _require(config.CONN_WAIT_TIMEOUT_SECONDS, "[promote] conn_wait_timeout_seconds")
    FORCE_TERMINATE_APP_NAMES = _require(config.FORCE_TERMINATE_APP_NAMES, "[promote] force_terminate_app_names")
    FORCE_TERMINATE_EXCLUDE = _require(config.FORCE_TERMINATE_EXCLUDE, "[promote] force_terminate_exclude")


# ############################################################################
# ############################################################################
# ##                                                                        ##
# ##   JOB 1 -- BAYERN BUILDINGS                                            ##
# ##   source: by/building/fetch_building.py                                 ##
# ##                                                                        ##
# ############################################################################
# ############################################################################

# Config: [building] URLS -- the Hausumringe zips, one per line.
#
# NOTE: order is significant. The FIRST url is imported with -overwrite and
# therefore defines the schema of the destination table; the rest -append into
# it. Reordering config.ini changes which Bezirk creates the table.
URLS: list[str] = []

# Config: [remote_database]
#
# TABLE_NAME is the LOCAL destination table for the Hausumringe shapefiles
# (hausumringe); REMOTE_TABLE_NAME is the LOCAL table filled from the remote
# database (remote_address). Both live under [remote_database] as requested.
TABLE_NAME: str | None = None
REMOTE_TABLE_NAME: str | None = None

remote_pg_user: str | None = None
remote_pg_password: str | None = None
remote_pg_database: str | None = None
remote_pg_host: str | None = None
remote_pg_port: int | None = None


def _bld_download_file(url: str, out_path: Path, timeout: int = 240) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".part")

    with requests.get(url, stream=True, timeout=timeout, allow_redirects=True) as r:
        r.raise_for_status()
        expected = r.headers.get("Content-Length")
        expected_bytes = int(expected) if expected and expected.isdigit() else None

        written = 0
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
                    written += len(chunk)

    if expected_bytes is not None and written != expected_bytes:
        tmp.unlink(missing_ok=True)
        raise RuntimeError(f"Download size mismatch: wrote {written} bytes, expected {expected_bytes} bytes.")

    tmp.replace(out_path)


def unzip_all(zip_path: Path, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(out_dir)


def _bld_run(cmd: list[str]) -> str:
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"Command failed ({p.returncode}): {' '.join(cmd)}\n\nSTDERR:\n{p.stderr}")
    return p.stdout


def _bld_connect(dbname: str, host: str, port: int, user: str, password: str):
    return psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)


@contextlib.contextmanager
def _bld_db(dbname: str, host: str, port: int, user: str, password: str):
    """
    Connection that is actually CLOSED on exit.

    psycopg2's own connection context manager commits or rolls back the
    transaction but leaves the socket open. Promotion renames these databases,
    and PostgreSQL refuses to rename a database with any session attached, so
    every connection has to be closed explicitly. The inner `with conn` keeps
    the original commit/rollback semantics.
    """
    conn = _bld_connect(dbname, host, port, user, password)
    try:
        with conn:
            yield conn
    finally:
        conn.close()

def _bld_ensure_database(dbname: str, host: str, port: int, user: str, password: str, template: str | None = None) -> None:
    conn = None
    try:
        conn = _bld_connect("postgres", host, port, user, password)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (dbname,))
            if cur.fetchone() is None:
                try:
                    if template:
                        cur.execute(
                            sql.SQL("CREATE DATABASE {} TEMPLATE {}").format(
                                sql.Identifier(dbname),
                                sql.Identifier(template),
                            )
                        )
                    else:
                        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
                except errors.DuplicateDatabase:
                    pass
    finally:
        if conn is not None:
            conn.close()


def _bld_require_postgis(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
    conn.commit()


def _bld_pg_dsn(host: str, port: int, dbname: str, user: str, password: str) -> str:
    return f"PG:host={host} port={port} dbname={dbname} user={user} password={password}"


def add_geom_index(conn, table_name: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} USING GIST (geom);").format(
                sql.Identifier(f"{table_name}_geom_gist"),
                sql.Identifier(table_name),
            )
        )
    conn.commit()


def ensure_src_zip_column(conn, table_name: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS src_zip TEXT;").format(
                sql.Identifier(table_name)
            )
        )
    conn.commit()


def set_src_zip_for_last_append(conn, table_name: str, src_zip: str) -> None:
    """
    Best-effort: fill src_zip for rows where it's NULL (after each append).
    Works because we import in batches per zip; rows appended will have src_zip NULL.
    """
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("UPDATE {} SET src_zip = %s WHERE src_zip IS NULL;").format(
                sql.Identifier(table_name)
            ),
            (src_zip,),
        )
    conn.commit()


def ogr2ogr_import(
    shp_path: Path,
    table_name: str,
    host: str,
    port: int,
    dbname: str,
    user: str,
    password: str,
    mode: str,  # "overwrite" or "append"
) -> None:
    dsn = _bld_pg_dsn(host, port, dbname, user, password)

    cmd = [
        "ogr2ogr",
        "-f", "PostgreSQL",
        dsn,
        str(shp_path),
        "-nln", table_name,
        "-lco", "GEOMETRY_NAME=geom",
        "-nlt", "PROMOTE_TO_MULTI",
        "-oo", "ENCODING=UTF-8",
    ]
    if mode == "overwrite":
        cmd.insert(cmd.index("-nln"), "-overwrite")
    elif mode == "append":
        cmd.insert(cmd.index("-nln"), "-append")
    else:
        raise ValueError("mode must be overwrite or append")

    _bld_run(cmd)


def find_single_shp(extract_dir: Path) -> Path:
    shps = sorted([p for p in extract_dir.rglob("*.shp") if p.is_file()])
    if not shps:
        raise RuntimeError(f"No .shp found under {extract_dir}")
    # deterministic pick if multiple
    return shps[0]


# ----------------------------
# remote_address population
# ----------------------------
def ensure_remote_address_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {} (
                    id TEXT PRIMARY KEY,
                    street_name TEXT,
                    street_id TEXT,
                    house_number TEXT,
                    postcode TEXT,
                    locality TEXT,
                    component_locality TEXT,
                    district TEXT,
                    federal_state TEXT,
                    rs_key TEXT,
                    ags_key TEXT,
                    house_center_point_geom geometry(Point, 25832),
                    house_bbox_geom geometry(Polygon, 25832),
                    street_center_point_geom geometry(Point, 25832),
                    street_bbox_geom geometry(Polygon, 25832)
                );
                """
            ).format(sql.Identifier(REMOTE_TABLE_NAME))
        )
        cur.execute(
            sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} (street_id);").format(
                sql.Identifier(f"{REMOTE_TABLE_NAME}_street_id_idx"),
                sql.Identifier(REMOTE_TABLE_NAME),
            )
        )
        cur.execute(
            sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} USING GIST (house_center_point_geom);").format(
                sql.Identifier(f"{REMOTE_TABLE_NAME}_house_center_gist"),
                sql.Identifier(REMOTE_TABLE_NAME),
            )
        )
        cur.execute(
            sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} USING GIST (house_bbox_geom);").format(
                sql.Identifier(f"{REMOTE_TABLE_NAME}_house_bbox_gist"),
                sql.Identifier(REMOTE_TABLE_NAME),
            )
        )
        cur.execute(
            sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} USING GIST (street_center_point_geom);").format(
                sql.Identifier(f"{REMOTE_TABLE_NAME}_street_center_gist"),
                sql.Identifier(REMOTE_TABLE_NAME),
            )
        )
        cur.execute(
            sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} USING GIST (street_bbox_geom);").format(
                sql.Identifier(f"{REMOTE_TABLE_NAME}_street_bbox_gist"),
                sql.Identifier(REMOTE_TABLE_NAME),
            )
        )
    conn.commit()


def populate_remote_address(local_conn, remote_conn, batch_size: int = 20000) -> None:
    """
    Fetch from remote DB and fill local remote_address (TRUNCATE + INSERT in chunks).
    Uses ST_AsEWKB so we can safely pass bytes and reconstruct geometry in local PostGIS.
    """
    ensure_remote_address_table(local_conn)

    with local_conn.cursor() as lcur:
        lcur.execute(sql.SQL("TRUNCATE TABLE {};").format(sql.Identifier(REMOTE_TABLE_NAME)))
    local_conn.commit()

    remote_sql = """
        SELECT
            house.id,
            house.street_name,
            house.street_id,
            house.house_number,
            house.postcode,
            house.locality,
            house.component_locality,
            house.district,
            house.federal_state,
            house.rs_key,
            house.ags_key,
            ST_AsEWKB(ST_Transform(house.center_point, 25832)) AS house_center_point_geom_ewkb,
            ST_AsEWKB(ST_Transform(house.bbox, 25832))         AS house_bbox_geom_ewkb,
            ST_AsEWKB(ST_Transform(street.center_point, 25832)) AS street_center_point_geom_ewkb,
            ST_AsEWKB(ST_Transform(street.bbox, 25832))         AS street_bbox_geom_ewkb
        FROM house
		  JOIN street
		   ON street.street_name = house.street_name
		   AND street.postcode    = house.postcode
		   AND street.locality    = house.locality
		   AND COALESCE(street.component_locality,'') = COALESCE(house.component_locality,'')
        WHERE house.ags_key LIKE '09%%'
          AND house.quality IN ('A', 'B', 'C');
    """

    inserted = 0

    # server-side cursor to avoid loading everything into RAM
    with remote_conn.cursor(name="remote_house_cursor") as rcur:
        rcur.itersize = batch_size
        rcur.execute(remote_sql)

        with local_conn.cursor() as lcur:
            while True:
                rows = rcur.fetchmany(batch_size)
                if not rows:
                    break

                psycopg2.extras.execute_values(
                    lcur,
                    f"""
                    INSERT INTO {REMOTE_TABLE_NAME} (
                        id, street_name, street_id, house_number,
                        postcode, locality, component_locality,
                        district, federal_state, rs_key, ags_key,
                        house_center_point_geom, house_bbox_geom,
                        street_center_point_geom, street_bbox_geom
                    )
                    VALUES %s
                    """,
                    rows,
                    template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ST_GeomFromEWKB(%s), ST_GeomFromEWKB(%s), ST_GeomFromEWKB(%s), ST_GeomFromEWKB(%s))",
                    page_size=5000,
                )

                inserted += len(rows)
                if inserted % (batch_size * 2) == 0:
                    local_conn.commit()
                    print(f"  -> remote_address inserted: {inserted:,}")

    local_conn.commit()
    print(f"  -> remote_address done. Total rows: {inserted:,}")


def run_building_job(args, dbname: str) -> None:
    """
    JOB 1 entry point. Body is by/building/fetch_building.py main() with the
    argparse block lifted out to the shared CLI and the target database name
    passed in (derived from the jobID by main()).
    """
    if shutil.which("ogr2ogr") is None:
        raise RuntimeError("ogr2ogr not found. Install: sudo apt-get install -y gdal-bin")

    if not remote_pg_password:
        raise RuntimeError(
            f"remote_pg_password is empty. Set it under [remote_database] in {CONFIG_FILENAME}."
        )

    workdir = DOWNLOAD_DIR
    dl_dir = workdir / "hausumringe_zips"
    ex_dir = workdir / "hausumringe_extract"
    dl_dir.mkdir(parents=True, exist_ok=True)
    ex_dir.mkdir(parents=True, exist_ok=True)

    _step(f"[1/6] Ensure database exists: {dbname}")
    # CREATE DATABASE ... TEMPLATE needs the TEMPLATE free of sessions.
    wait_for_no_connections(TEMPLATE_DB)
    _bld_ensure_database(dbname, PG_HOST, PG_PORT, PG_USER, PG_PASSWORD)

    with _bld_db(dbname, PG_HOST, PG_PORT, PG_USER, PG_PASSWORD) as conn:
        _bld_require_postgis(conn)

    _step(f"[2/6] Populate local table '{REMOTE_TABLE_NAME}' from remote DB ({remote_pg_host}:{remote_pg_port}/{remote_pg_database})")
    with _bld_db(dbname, PG_HOST, PG_PORT, PG_USER, PG_PASSWORD) as local_conn, \
         _bld_connect(remote_pg_database, remote_pg_host, remote_pg_port, remote_pg_user, remote_pg_password) as remote_conn:
        populate_remote_address(local_conn, remote_conn)

    _step(f"[3/6] Download + unzip {len(URLS)} zips")
    jobs: list[tuple[Path, str]] = []  # (shp_path, zip_basename)
    for url in URLS:
        zip_name = Path(url).name
        zpath = dl_dir / zip_name
        out = ex_dir / Path(zip_name).stem

        print(f"  -> download {zip_name}")
        _bld_download_file(url, zpath)
        DOWNLOADED_ARCHIVES.append(zpath)
        print(f"     unzip -> {out}")
        unzip_all(zpath, out)

        shp = find_single_shp(out)
        jobs.append((shp, zip_name))

    _step(f"[4/6] Import into ONE table: {TABLE_NAME}")
    first = True
    for shp_path, zip_name in jobs:
        mode = "overwrite" if first else "append"
        print(f"  -> {mode}: {shp_path}  (from {zip_name})")
        ogr2ogr_import(
            shp_path=shp_path,
            table_name=TABLE_NAME,
            host=PG_HOST,
            port=PG_PORT,
            dbname=dbname,
            user=PG_USER,
            password=PG_PASSWORD,
            mode=mode,
        )

        if not args.no_src_zip:
            with _bld_db(dbname, PG_HOST, PG_PORT, PG_USER, PG_PASSWORD) as conn:
                ensure_src_zip_column(conn, TABLE_NAME)
                set_src_zip_for_last_append(conn, TABLE_NAME, zip_name)

        first = False

    _step(f"[5/6] Create GiST index on {TABLE_NAME}.geom")
    with _bld_db(dbname, PG_HOST, PG_PORT, PG_USER, PG_PASSWORD) as conn:
        add_geom_index(conn, TABLE_NAME)

    _end_step()
    print("[6/6] Done.")


# ############################################################################
# ############################################################################
# ##                                                                        ##
# ##   JOB 2 -- BAYERN USAGE                                                ##
# ##   source: by/usage/fetch_usage.py                                      ##
# ##                                                                        ##
# ############################################################################
# ############################################################################

# Config: [usage] GPKG_URL
GPKG_URL: str | None = None


REQUIRED_COLUMNS = {
    "gml_id",
    "nutzart",
    "bez",
    "geom",
    "src_layer",
}


def sanitize_ident(name: str, prefix: str = "t_", maxlen: int = 63) -> str:
    n = name.lower()
    n = re.sub(r"[^a-z0-9_]+", "_", n).strip("_")

    if not n or not re.match(r"^[a-z_]", n):
        n = prefix + (n if n else "x")

    return n[:maxlen]


def _usg_download_file(url: str, out_path: Path, timeout: int = 180) -> None:
    """
    Download to a temporary .part file first.

    If Content-Length is available, verify that the received file size
    matches before replacing the existing local file.
    """

    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".part")

    with requests.get(
        url,
        stream=True,
        timeout=timeout,
        allow_redirects=True,
    ) as r:
        r.raise_for_status()

        expected = r.headers.get("Content-Length")
        expected_bytes = (
            int(expected)
            if expected and expected.isdigit()
            else None
        )

        written = 0

        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue

                f.write(chunk)
                written += len(chunk)

    if expected_bytes is not None and written != expected_bytes:
        tmp.unlink(missing_ok=True)

        raise RuntimeError(
            "Download size mismatch: "
            f"wrote {written} bytes, expected {expected_bytes} bytes."
        )

    tmp.replace(out_path)


def _usg_run(cmd: list[str]) -> str:
    """
    Execute a command and abort if it returns a non-zero exit code.
    """

    p = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    if p.returncode != 0:
        raise RuntimeError(
            f"Command failed ({p.returncode}): {' '.join(cmd)}\n\n"
            f"STDERR:\n{p.stderr}"
        )

    return p.stdout


def list_layers(gpkg_path: Path) -> list[str]:
    """
    Detect all layers in the GeoPackage.

    Uses:
        ogrinfo -ro -q <gpkg>

    Typical output:
        1: Landkreis Aichach-Friedberg
        2: Landkreis Altötting

    Newer ogrinfo versions may append the geometry type:
        1: Landkreis Aichach-Friedberg (Multi Polygon)

    The geometry suffix is stripped before using the layer name.
    """

    out = _usg_run([
        "ogrinfo",
        "-ro",
        "-q",
        str(gpkg_path),
    ])

    layers = []

    for line in out.splitlines():
        m = re.match(r"^\s*\d+\s*:\s*(.+?)\s*$", line)

        if m:
            name = m.group(1)

            # Newer ogrinfo versions append geometry type,
            # e.g. "(Multi Polygon)".
            name = re.sub(
                r"\s+\([^)]+\)\s*$",
                "",
                name,
            )

            layers.append(name)

    if not layers:
        raise RuntimeError(
            "Could not detect any layers via ogrinfo output."
        )

    return layers


def _usg_connect(
    dbname: str,
    host: str,
    port: int,
    user: str,
    password: str,
):
    return psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port,
    )


@contextlib.contextmanager
def _usg_db(dbname: str, host: str, port: int, user: str, password: str):
    """
    Connection that is actually CLOSED on exit.

    psycopg2's own connection context manager commits or rolls back the
    transaction but leaves the socket open. Promotion renames these databases,
    and PostgreSQL refuses to rename a database with any session attached, so
    every connection has to be closed explicitly. The inner `with conn` keeps
    the original commit/rollback semantics.
    """
    conn = _usg_connect(dbname, host, port, user, password)
    try:
        with conn:
            yield conn
    finally:
        conn.close()

def _usg_ensure_database(
    dbname: str,
    host: str,
    port: int,
    user: str,
    password: str,
    template: str | None = None,
) -> None:
    conn = None

    try:
        conn = _usg_connect(
            "postgres",
            host,
            port,
            user,
            password,
        )

        # CREATE DATABASE cannot run inside a transaction.
        conn.autocommit = True

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM pg_database
                WHERE datname = %s;
                """,
                (dbname,),
            )

            if cur.fetchone() is None:
                try:
                    if template:
                        cur.execute(
                            sql.SQL(
                                "CREATE DATABASE {} TEMPLATE {}"
                            ).format(
                                sql.Identifier(dbname),
                                sql.Identifier(template),
                            )
                        )
                    else:
                        cur.execute(
                            sql.SQL(
                                "CREATE DATABASE {}"
                            ).format(
                                sql.Identifier(dbname)
                            )
                        )

                except errors.DuplicateDatabase:
                    pass

    finally:
        if conn is not None:
            conn.close()


def _usg_require_postgis(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "CREATE EXTENSION IF NOT EXISTS postgis;"
        )

    conn.commit()


def _usg_pg_dsn(
    host: str,
    port: int,
    dbname: str,
    user: str,
    password: str,
) -> str:
    """
    ogr2ogr expects PG:... connection syntax.
    """

    return (
        f"PG:host={host} "
        f"port={port} "
        f"dbname={dbname} "
        f"user={user} "
        f"password={password}"
    )


def ogr2ogr_import_all_layers(
    gpkg_path: Path,
    layers: list[str],
    table_name: str,
    host: str,
    port: int,
    dbname: str,
    user: str,
    password: str,
) -> None:
    """
    Import every source layer into one PostgreSQL table.

    IMPORTANT:

    The GeoPackage source field is named 'oid'.

    During the first -overwrite import, ogr2ogr/PostgreSQL may rename the
    destination field to 'oid_'.

    During later -append imports, that causes the source 'oid' field to no
    longer map correctly to the already-created destination 'oid_' column.

    Therefore the source oid is explicitly aliased to gml_id inside the
    OGR SQL SELECT:

        oid AS gml_id

    This guarantees that every imported layer has a consistently named
    destination identifier column.
    """

    dsn = _usg_pg_dsn(
        host,
        port,
        dbname,
        user,
        password,
    )

    first = True

    for lyr in layers:

        # Escape single quotes for SQL string literal.
        lyr_literal = lyr.replace("'", "''")

        # Quote layer name for OGRSQL.
        lyr_quoted = lyr.replace('"', '""')

        #
        # IMPORTANT FIX:
        #
        # Do NOT remove "oid AS gml_id".
        #
        # Without this alias, oid values from appended layers can become
        # NULL because PostgreSQL/OGR changes the destination field name
        # from oid to oid_.
        #
        sql_sel = (
            f'SELECT *, '
            f'oid AS gml_id, '
            f'\'{lyr_literal}\' AS src_layer '
            f'FROM "{lyr_quoted}"'
        )

        cmd = [
            "ogr2ogr",
            "-f",
            "PostgreSQL",
            dsn,
            str(gpkg_path),
            "-nln",
            table_name,
            "-nlt",
            "PROMOTE_TO_MULTI",
            "-lco",
            "GEOMETRY_NAME=geom",
            "-sql",
            sql_sel,
        ]

        if first:
            # First layer creates/overwrites destination table.
            cmd.insert(
                cmd.index("-nln"),
                "-overwrite",
            )

            first = False

        else:
            # All later source layers append into the same table.
            cmd.insert(
                cmd.index("-nln"),
                "-append",
            )

        print(f"  -> importing layer: {lyr}")

        _usg_run(cmd)


def column_exists(
    conn,
    table_name: str,
    col_name: str,
) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
              AND column_name = %s
            LIMIT 1;
            """,
            (
                table_name,
                col_name,
            ),
        )

        return cur.fetchone() is not None


def get_table_columns(
    conn,
    table_name: str,
) -> set[str]:
    """
    Return the column names present in public.<table_name>.
    """

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
            ORDER BY ordinal_position;
            """,
            (table_name,),
        )

        return {
            row[0]
            for row in cur.fetchall()
        }


def validate_import(
    conn,
    table_name: str,
    expected_layers: list[str],
) -> None:
    """
    HARD GUARD RAIL.

    The generated database is considered INVALID unless:

        1. All required production columns exist.
        2. The table contains at least one row.
        3. gml_id has ZERO NULL values.
        4. Every GPKG layer detected before import is represented in
           src_layer after import.

    Duplicate gml_id values are allowed intentionally because the previous
    known-working database also contained duplicate gml_id values.

    Any failed check raises RuntimeError, causing the script to exit
    non-zero.
    """

    print("")
    print("============================================================")
    print("VALIDATING IMPORT")
    print("============================================================")

    actual_columns = get_table_columns(
        conn,
        table_name,
    )

    missing_columns = (
        REQUIRED_COLUMNS - actual_columns
    )

    if missing_columns:
        raise RuntimeError(
            "VALIDATION FAILED: required columns are missing from "
            f"public.{table_name}: "
            + ", ".join(sorted(missing_columns))
        )

    print(
        "  [OK] Required columns exist: "
        + ", ".join(sorted(REQUIRED_COLUMNS))
    )

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                SELECT
                    COUNT(*) AS total_rows,
                    COUNT(gml_id) AS gml_id_not_null,
                    COUNT(*) FILTER (
                        WHERE gml_id IS NULL
                    ) AS gml_id_null,
                    COUNT(DISTINCT gml_id) AS distinct_gml_id
                FROM {};
                """
            ).format(
                sql.Identifier(table_name)
            )
        )

        (
            total_rows,
            gml_id_not_null,
            gml_id_null,
            distinct_gml_id,
        ) = cur.fetchone()

    print(
        f"  Rows:              {total_rows:,}"
    )

    print(
        f"  gml_id non-NULL:   {gml_id_not_null:,}"
    )

    print(
        f"  gml_id NULL:       {gml_id_null:,}"
    )

    print(
        f"  distinct gml_id:   {distinct_gml_id:,}"
    )

    if total_rows == 0:
        raise RuntimeError(
            "VALIDATION FAILED: "
            f"public.{table_name} contains zero rows."
        )

    if gml_id_null != 0:
        raise RuntimeError(
            "VALIDATION FAILED: "
            f"{gml_id_null:,} of {total_rows:,} rows "
            "have NULL gml_id."
        )

    if gml_id_not_null != total_rows:
        raise RuntimeError(
            "VALIDATION FAILED: "
            "COUNT(gml_id) does not equal COUNT(*). "
            f"COUNT(*)={total_rows:,}, "
            f"COUNT(gml_id)={gml_id_not_null:,}."
        )

    print(
        "  [OK] Every imported row has a non-NULL gml_id."
    )

    #
    # Validate imported source layers.
    #
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                SELECT DISTINCT src_layer
                FROM {}
                WHERE src_layer IS NOT NULL;
                """
            ).format(
                sql.Identifier(table_name)
            )
        )

        imported_layers = {
            row[0]
            for row in cur.fetchall()
        }

    expected_layer_set = set(expected_layers)

    missing_layers = (
        expected_layer_set - imported_layers
    )

    unexpected_layers = (
        imported_layers - expected_layer_set
    )

    print(
        f"  Expected layers:   {len(expected_layer_set)}"
    )

    print(
        f"  Imported layers:   {len(imported_layers)}"
    )

    if missing_layers:
        raise RuntimeError(
            "VALIDATION FAILED: source layers are missing "
            "from the resulting PostgreSQL table:\n  - "
            + "\n  - ".join(
                sorted(missing_layers)
            )
        )

    if unexpected_layers:
        raise RuntimeError(
            "VALIDATION FAILED: unexpected src_layer values "
            "were found in the resulting PostgreSQL table:\n  - "
            + "\n  - ".join(
                sorted(unexpected_layers)
            )
        )

    if len(imported_layers) != len(expected_layer_set):
        raise RuntimeError(
            "VALIDATION FAILED: source layer count mismatch. "
            f"Expected {len(expected_layer_set)}, "
            f"found {len(imported_layers)}."
        )

    print(
        "  [OK] Every detected GPKG layer is present."
    )

    print("")
    print("============================================================")
    print("VALIDATION PASSED")
    print("============================================================")
    print("")


def add_indexes(
    conn,
    table_name: str,
) -> None:

    with conn.cursor() as cur:

        #
        # Geometry index
        #
        cur.execute(
            sql.SQL(
                "CREATE INDEX IF NOT EXISTS {} "
                "ON {} USING GIST (geom);"
            ).format(
                sql.Identifier(
                    f"{table_name}_geom_gist"
                ),
                sql.Identifier(table_name),
            )
        )

        #
        # src_layer index
        #
        if column_exists(
            conn,
            table_name,
            "src_layer",
        ):
            cur.execute(
                sql.SQL(
                    "CREATE INDEX IF NOT EXISTS {} "
                    "ON {} (src_layer);"
                ).format(
                    sql.Identifier(
                        f"{table_name}_src_layer_idx"
                    ),
                    sql.Identifier(table_name),
                )
            )

        #
        # gml_id index
        #
        # This is the identifier required by the downstream
        # foreign table/materialized-view queries.
        #
        if column_exists(
            conn,
            table_name,
            "gml_id",
        ):
            cur.execute(
                sql.SQL(
                    "CREATE INDEX IF NOT EXISTS {} "
                    "ON {} (gml_id);"
                ).format(
                    sql.Identifier(
                        f"{table_name}_gml_id_idx"
                    ),
                    sql.Identifier(table_name),
                )
            )

        #
        # Original OID-related index logic retained.
        #
        if column_exists(
            conn,
            table_name,
            "oid",
        ):
            cur.execute(
                sql.SQL(
                    "CREATE INDEX IF NOT EXISTS {} "
                    "ON {} (oid);"
                ).format(
                    sql.Identifier(
                        f"{table_name}_oid_idx"
                    ),
                    sql.Identifier(table_name),
                )
            )

        elif column_exists(
            conn,
            table_name,
            "OID",
        ):
            cur.execute(
                sql.SQL(
                    'CREATE INDEX IF NOT EXISTS {} '
                    'ON {} ("OID");'
                ).format(
                    sql.Identifier(
                        f"{table_name}_OID_idx"
                    ),
                    sql.Identifier(table_name),
                )
            )

    conn.commit()


def grant_select(
    conn,
    table_name: str,
    role: str,
) -> None:

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "GRANT SELECT ON TABLE public.{} TO {};"
            ).format(
                sql.Identifier(table_name),
                sql.Identifier(role),
            )
        )

    conn.commit()


def run_usage_job(args, dbname: str) -> None:
    """
    JOB 2 entry point. Body is by/usage/fetch_usage.py main() with the argparse
    block lifted out to the shared CLI and the target database name passed in
    (derived from the jobID by main()).

    Note that only the DATABASE name is jobID-scoped. The destination TABLE is
    still named after the GeoPackage (nutzung_kreis), as before.
    """

    workdir = DOWNLOAD_DIR

    workdir.mkdir(
        parents=True,
        exist_ok=True,
    )

    gpkg_path = (
        workdir / "Nutzung_kreis.gpkg"
    )

    table_name = sanitize_ident(
        gpkg_path.stem
    )

    _step(
        f"[1/6] Download: {GPKG_URL}"
    )

    _usg_download_file(
        GPKG_URL,
        gpkg_path,
    )
    DOWNLOADED_ARCHIVES.append(gpkg_path)

    _step(
        f"[2/6] Detect layers via ogrinfo: "
        f"{gpkg_path.name}"
    )

    layers = list_layers(
        gpkg_path
    )

    print(
        f"      Found layers: {len(layers)}"
    )

    _step(
        f"[3/6] Ensure database exists: {dbname}"
    )

    # CREATE DATABASE ... TEMPLATE needs the TEMPLATE free of sessions.

    wait_for_no_connections(TEMPLATE_DB)
    _usg_ensure_database(
        dbname,
        PG_HOST,
        PG_PORT,
        PG_USER,
        PG_PASSWORD,
        template=TEMPLATE_DB,
    )

    _step(
        f"[4/6] Import all layers into table: "
        f"{table_name}"
    )

    with _usg_db(
        dbname,
        PG_HOST,
        PG_PORT,
        PG_USER,
        PG_PASSWORD,
    ) as conn:
        _usg_require_postgis(conn)

    ogr2ogr_import_all_layers(
        gpkg_path=gpkg_path,
        layers=layers,
        table_name=table_name,
        host=PG_HOST,
        port=PG_PORT,
        dbname=dbname,
        user=PG_USER,
        password=PG_PASSWORD,
    )

    #
    # HARD VALIDATION GATE
    #
    # If anything is wrong here, RuntimeError is raised and the
    # job fails. Index creation/grants/"Done" are NOT reached.
    #
    _step(
        f"[5/6] Validate imported database: {dbname}"
    )

    with _usg_db(
        dbname,
        PG_HOST,
        PG_PORT,
        PG_USER,
        PG_PASSWORD,
    ) as conn:

        validate_import(
            conn,
            table_name,
            layers,
        )

    _step(
        "[6/6] Create indexes and grant permissions"
    )

    with _usg_db(
        dbname,
        PG_HOST,
        PG_PORT,
        PG_USER,
        PG_PASSWORD,
    ) as conn:

        add_indexes(
            conn,
            table_name,
        )

        grant_select(
            conn,
            table_name,
            "fdw_reader",
        )

    _end_step()
    print("")
    print(
        f"SUCCESS: database '{dbname}' passed validation."
    )
    print(
        "It is safe to proceed with the production database swap."
    )


# ============================================================================
# DATABASE PROMOTION
#
# Promotion runs ONLY when every job completed. It dumps every live and working
# database first, so that a failed dump aborts while the live set is still in
# place, and only then performs the renames.
#
# PostgreSQL refuses ALTER DATABASE ... RENAME and DROP DATABASE while any
# session is connected, so each is preceded by a connection drain that polls
# pg_stat_activity. Sessions are NEVER terminated: on timeout the swap is
# abandoned and the live databases are left untouched.
# ============================================================================

def db_names(job: str, job_id: str) -> dict[str, str]:
    """The four database names for one job and jobID."""
    live = f"{STATE}_alkis_{job}"
    return {
        "live": live,
        "old": f"{live}_old",
        "working": f"{live}_{job_id}",
        "importing": f"{live}_{job_id}_importing",
    }


def pg_admin_connect():
    """
    Connect to the 'postgres' maintenance database with autocommit on.

    CREATE/DROP/ALTER DATABASE cannot run inside a transaction block, and the
    session must not be connected to the database being altered -- hence a
    separate maintenance connection rather than reusing a job's connection.
    """
    conn = psycopg2.connect(
        dbname="postgres",
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT,
    )
    conn.autocommit = True
    return conn


def database_exists(dbname: str) -> bool:
    conn = pg_admin_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (dbname,))
            return cur.fetchone() is not None
    finally:
        conn.close()


# After terminating sessions, re-poll after this long rather than waiting a full
# CONN_WAIT_INTERVAL_SECONDS: a connection pool or a pgAdmin panel may reconnect
# straight away, and we want to notice quickly. The overall timeout still bounds
# the loop.
TERMINATE_SETTLE_SECONDS = 2.0

# The predicate deciding whether a session is killed or waited for. Evaluated in
# SQL, so the patterns use real PostgreSQL LIKE semantics (% and _) rather than
# an approximation of them in Python.
#
# A session is terminated when EITHER
#   - its state is exactly 'idle'                     (no query, no transaction)
#   - or its application_name matches one of [promote] force_terminate_app_names
#     and none of force_terminate_exclude             (regardless of state)
#
# The second arm is what clears pgAdmin's Dashboard and browser-tree pollers,
# which sit in state 'active' re-running their monitoring queries and would
# otherwise block the swap until the timeout. Query Tool connections are
# excluded by default so a query you are deliberately running is waited for.
_TERMINATE_PREDICATE = """
    state = 'idle'
    OR (
              application_name LIKE ANY(%s)
        AND NOT (application_name LIKE ANY(%s))
    )
"""


def classify_connections(dbname: str) -> tuple:
    """
    Split the sessions on dbname into (to_terminate, to_wait_for).

    Each row is (pid, usename, application_name, client_addr, state).

    NOTE: state is NULL for sessions belonging to another role when we lack the
    privilege to inspect them (non-superuser reading a superuser's backend), so
    every use of it has to tolerate None. A NULL state also makes the
    `state = 'idle'` arm NULL rather than true, so such a session is only
    terminated if its application_name matches -- which is the conservative
    outcome we want.
    """
    conn = pg_admin_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT pid, usename, application_name, client_addr, state,
                       ({_TERMINATE_PREDICATE}) AS should_terminate
                FROM pg_stat_activity
                WHERE datname = %s
                  AND pid <> pg_backend_pid();
                """,
                (FORCE_TERMINATE_APP_NAMES, FORCE_TERMINATE_EXCLUDE, dbname),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    return (
        [r[:5] for r in rows if r[5]],
        [r[:5] for r in rows if not r[5]],
    )


def terminate_sessions(dbname: str, pids: list) -> tuple:
    """
    pg_terminate_backend the given pids, re-evaluating the terminate predicate
    inside the same statement.

    Re-checking atomically matters: a session that was idle at poll time may
    have started real work since, and killing it would destroy that work. Rows
    that no longer satisfy the predicate are simply not returned, so
    pg_terminate_backend is never called on them.

    Terminating is a PRIVILEGED operation. A non-superuser can only signal its
    own role's backends (or any non-superuser backend if granted
    pg_signal_backend); a superuser's backend can only be terminated by another
    superuser. Each pid is therefore attempted separately and a refusal is
    reported and tolerated rather than aborting the whole drain -- the session
    simply gets waited for instead.

    Returns (killed, denied) where killed is [(pid, application_name, state)]
    and denied is [(pid, reason)].
    """
    killed = []
    denied = []

    conn = pg_admin_connect()  # autocommit: one failed statement does not
    try:                       # poison the ones after it
        for pid in pids:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT pid, application_name, state,
                               pg_terminate_backend(pid)
                        FROM pg_stat_activity
                        WHERE datname = %s
                          AND pid = %s
                          AND pid <> pg_backend_pid()
                          AND ({_TERMINATE_PREDICATE});
                        """,
                        (
                            dbname,
                            pid,
                            FORCE_TERMINATE_APP_NAMES,
                            FORCE_TERMINATE_EXCLUDE,
                        ),
                    )
                    row = cur.fetchone()
                    if row and row[3]:
                        killed.append((row[0], row[1], row[2]))
            except psycopg2.Error as exc:
                denied.append((pid, str(exc).strip().splitlines()[0]))
    finally:
        conn.close()

    return killed, denied


def wait_for_no_connections(dbname: str) -> None:
    """
    Block until no session is connected to dbname.

    Sessions are terminated when they are plainly idle, or when their
    application_name marks them as a monitoring client configured under
    [promote] force_terminate_app_names (pgAdmin's Dashboard, by default) --
    those poll continuously and would never clear on their own.

    Everything else -- a real query, an open transaction, a pgAdmin Query Tool
    tab -- is waited on, polled every CONN_WAIT_INTERVAL_SECONDS for up to
    CONN_WAIT_TIMEOUT_SECONDS. On timeout this raises, which aborts the swap and
    leaves the live databases untouched.
    """
    deadline = time.monotonic() + CONN_WAIT_TIMEOUT_SECONDS
    terminated_total = 0

    while True:
        to_kill, to_wait = classify_connections(dbname)

        if not to_kill and not to_wait:
            return

        if to_kill:
            killed, denied = terminate_sessions(dbname, [r[0] for r in to_kill])
            terminated_total += len(killed)
            for pid, app, state in killed:
                print(
                    f"      terminated pid={pid} app={app or '-'} "
                    f"state={state or 'unknown'} on {dbname}"
                )
            if denied:
                # Not permitted to signal these (typically a superuser-owned
                # session). Wait for them instead of spinning on a retry that
                # can never succeed.
                denied_pids = {pid for pid, _reason in denied}
                for pid, reason in denied:
                    print(f"      CANNOT terminate pid={pid}: {reason}")
                to_wait = to_wait + [r for r in to_kill if r[0] in denied_pids]

        if not to_wait:
            # Everything present was terminable. Re-poll shortly in case a
            # client reconnects; the overall deadline bounds this.
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError(
                    f"Timed out after {CONN_WAIT_TIMEOUT_SECONDS}s draining "
                    f"{dbname}: sessions keep reconnecting "
                    f"({terminated_total} terminated so far). Swap aborted; "
                    "live databases untouched."
                )
            time.sleep(min(TERMINATE_SETTLE_SECONDS, remaining))
            continue

        print(f"      {len(to_wait)} session(s) on {dbname} being waited for:")
        for pid, usename, app, addr, state in to_wait:
            print(
                f"        pid={pid} user={usename} app={app or '-'} "
                f"from={addr or 'local'} state={state or 'unknown'}"
            )

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError(
                f"Timed out after {CONN_WAIT_TIMEOUT_SECONDS}s waiting for "
                f"{len(to_wait)} session(s) to finish on {dbname} "
                f"(states: {', '.join(sorted({r[4] or 'unknown' for r in to_wait}))}). "
                "Swap aborted; live databases untouched."
            )

        sleep_for = min(CONN_WAIT_INTERVAL_SECONDS, remaining)
        print(f"      retrying in {sleep_for:.0f}s ({remaining:.0f}s before giving up)")
        time.sleep(sleep_for)


def drop_database(dbname: str) -> None:
    wait_for_no_connections(dbname)
    conn = pg_admin_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("DROP DATABASE {};").format(sql.Identifier(dbname)))
        print(f"      dropped {dbname}")
    finally:
        conn.close()


def rename_database(src: str, dst: str) -> None:
    wait_for_no_connections(src)
    conn = pg_admin_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("ALTER DATABASE {} RENAME TO {};").format(
                    sql.Identifier(src),
                    sql.Identifier(dst),
                )
            )
        print(f"      renamed {src} -> {dst}")
    finally:
        conn.close()


def dump_database(dbname: str, out_path: Path) -> None:
    """
    pg_dump in custom format (-Fc), restorable with pg_restore.

    The password goes to the child through PGPASSWORD rather than the command
    line, so it does not appear in ps. A failed dump removes the partial file
    and raises, which aborts the swap.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)

    env = dict(os.environ, PGPASSWORD=PG_PASSWORD)
    cmd = [
        "pg_dump",
        "-Fc",
        "-h", str(PG_HOST),
        "-p", str(PG_PORT),
        "-U", PG_USER,
        "-d", dbname,
        "-f", str(out_path),
    ]

    p = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    if p.returncode != 0:
        out_path.unlink(missing_ok=True)
        raise RuntimeError(
            f"pg_dump failed for {dbname} (exit {p.returncode}):\n{p.stderr}"
        )

    size_mib = out_path.stat().st_size / (1024 * 1024)
    print(f"      dumped {dbname} -> {out_path.name} ({size_mib:,.1f} MiB)")


def preflight(plan: list, names: dict, job_id: str) -> None:
    """
    Refuse to start if a working database for this jobID already exists, and
    clear away any *_importing leftovers from a previous failed run.

    A leftover *_importing database is unambiguously scratch -- the suffix means
    the import never finished -- so it is dropped. A bare working database is
    not: it means a previous run of this jobID imported successfully but was
    never promoted, which is real data, so this stops instead of destroying it.
    """
    for job, _num, _run, _banner_on_fail in plan:
        n = names[job]
        if database_exists(n["working"]):
            raise RuntimeError(
                f"Database {n['working']} already exists: a previous run of "
                f"jobID '{job_id}' completed the {job} job but was never "
                "promoted. Drop it, or use a different jobID."
            )

    for job, _num, _run, _banner_on_fail in plan:
        n = names[job]
        if database_exists(n["importing"]):
            print(f"  leftover {n['importing']} (import never finished) -> dropping")
            drop_database(n["importing"])

    print("  pre-flight OK")


def backup_path(job_id: str) -> Path:
    """<backup_dir>/<state>/<jobID> -- created on demand."""
    return Path(BACKUP_DIR).resolve() / STATE / job_id


def archive_downloads(zip_path: Path) -> None:
    """
    Zip the archives fetched from the source urls into zip_path.

    Only the downloads themselves are included, not the directories they were
    unpacked into: the extracts are reproducible from the archives and would
    multiply the size several times over. ZIP_STORED because the sources are
    already compressed (.zip / .gpkg), so recompressing costs time for nothing.
    allowZip64 because these routinely exceed 2 GiB.
    """
    present = [p for p in DOWNLOADED_ARCHIVES if p.is_file()]
    missing = [p for p in DOWNLOADED_ARCHIVES if not p.is_file()]

    for p in missing:
        print(f"      WARNING: {p} was downloaded but is no longer on disk")

    if not present:
        raise RuntimeError(
            "No downloaded archives found to zip; expected at least one. "
            "Refusing to write an empty backup archive."
        )

    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(
        zip_path, "w", compression=zipfile.ZIP_STORED, allowZip64=True
    ) as zf:
        for p in present:
            try:
                arcname = str(p.relative_to(DOWNLOAD_DIR))
            except ValueError:
                arcname = p.name
            zf.write(p, arcname=arcname)
            print(f"      + {arcname}  ({p.stat().st_size / (1024 * 1024):,.1f} MiB)")

    print(
        f"      wrote {zip_path.name} "
        f"({zip_path.stat().st_size / (1024 * 1024):,.1f} MiB, {len(present)} file(s))"
    )


def clear_download_dir() -> None:
    """
    Delete everything inside DOWNLOAD_DIR, keeping the directory itself.

    Only reached after a fully successful swap, and only once the archives have
    been zipped into the backup folder -- so nothing unrecoverable is removed.
    """
    if not DOWNLOAD_DIR.exists():
        print(f"      {DOWNLOAD_DIR} does not exist -- nothing to clear")
        return

    removed = 0
    for entry in sorted(DOWNLOAD_DIR.iterdir()):
        if entry.is_dir() and not entry.is_symlink():
            shutil.rmtree(entry)
        else:
            entry.unlink()
        print(f"      removed {entry.name}")
        removed += 1

    print(f"      cleared {removed} entry/entries from {DOWNLOAD_DIR}")


def promote(plan: list, names: dict, job_id: str) -> None:
    """
    Back everything up, then swap working -> live for every job, then clear the
    download directory.

    Order matters. Every dump AND the downloads zip are written before any
    database is renamed, so a failed backup aborts while the live set is still
    in place. The download directory is wiped last, only once the swap has
    fully succeeded.
    """
    timestamp = time.strftime("%Y%m%dT%H%M%S")
    backup_dir = backup_path(job_id)
    backup_dir.mkdir(parents=True, exist_ok=True)

    _banner(f"PROMOTE  (backups -> {backup_dir})")

    # Before the swap and, above all, before clear_download_dir() below: the
    # marker is hashed from the downloads, which that step deletes.
    _step("Recording update marker")
    record_marker(job_id)

    _step("Dumping live + working databases")
    for job, _num, _run, _banner_on_fail in plan:
        n = names[job]
        print(f"  {job}:")
        if database_exists(n["live"]):
            dump_database(n["live"], backup_dir / f"{n['live']}.dump")
        else:
            print(f"      no live {n['live']} yet -- nothing to dump (first run)")
        dump_database(n["working"], backup_dir / f"{n['working']}.dump")

    _end_step()
    print("")
    _step("Archiving downloaded source files")
    archive_downloads(backup_dir / f"{STATE}_downloads_{job_id}_{timestamp}.zip")

    _end_step()
    print("")
    _step("Swapping working -> live")
    for job, _num, _run, _banner_on_fail in plan:
        n = names[job]
        print(f"  {job}:")
        if database_exists(n["old"]):
            drop_database(n["old"])
        if database_exists(n["live"]):
            rename_database(n["live"], n["old"])
        else:
            print(f"      no live {n['live']} to retire (first run)")
        rename_database(n["working"], n["live"])

    _end_step()
    print("")
    _step("Clearing download directory")
    clear_download_dir()


# ############################################################################
# ############################################################################
# ##                                                                        ##
# ##   SHARED CLI                                                           ##
# ##                                                                        ##
# ############################################################################
# ############################################################################

def _banner(line: str) -> None:
    print("")
    print("============================================================")
    print(line)
    print("============================================================")


# Elapsed-time reporting. _step() prints a step heading and closes the previous
# step with its elapsed time; _end_step() closes the current one. _timed() does
# the same for a whole phase, in a finally, so a phase that fails still reports
# how long it ran.
_current_step: tuple[str, float] | None = None
# Set by main() once the arguments parse; the __main__ block reports the total.
_run_started: float | None = None


def _elapsed(started: float) -> str:
    """Wall clock since a time.monotonic() reading, as H:MM:SS."""
    secs = round(time.monotonic() - started)
    return f"{secs // 3600}:{secs % 3600 // 60:02d}:{secs % 60:02d}"


def _end_step() -> None:
    global _current_step
    if _current_step is not None:
        label, started = _current_step
        print(f"      elapsed {_elapsed(started)}  ({label})")
        _current_step = None


def _step(label: str) -> None:
    global _current_step
    _end_step()
    print(label)
    _current_step = (label, time.monotonic())


@contextlib.contextmanager
def _timed(label: str):
    started = time.monotonic()
    try:
        yield
    finally:
        _end_step()
        print(f"{label} elapsed {_elapsed(started)}")


def record_marker(job_id: str) -> None:
    """Record what this run downloaded in the update_state table (state_db.py).

    Hashed off the files in DOWNLOAD_DIR, so it costs no bandwidth -- which is
    also why it has to happen before promote() empties that directory. One row
    for this state and job; every other state's marker is left as it is.
    """
    marker = check_update.RECORDERS[STATE](DOWNLOAD_DIR)
    state_db.record_marker(STATE, job_id, marker)
    print(f"      recorded {len(marker)} file(s) in {state_db.STATE_DB}.update_state")


def download_only(job_id: str) -> None:
    """--download: fetch both jobs' sources, record the marker, and stop.

    Every source lands where the jobs themselves put it, so a later full run
    finds the files already there. Nothing is unzipped, nothing is imported and
    no database is touched -- so this needs neither GDAL nor PostgreSQL.
    """
    dl_dir = DOWNLOAD_DIR / "hausumringe_zips"
    gpkg_path = DOWNLOAD_DIR / "Nutzung_kreis.gpkg"
    total = len(URLS) + 2

    for num, url in enumerate(URLS, 1):
        zip_name = Path(url).name
        _step(f"[{num}/{total}] Download {zip_name}")
        _bld_download_file(url, dl_dir / zip_name)

    _step(f"[{len(URLS) + 1}/{total}] Download {gpkg_path.name}")
    _usg_download_file(GPKG_URL, gpkg_path)

    _step(f"[{total}/{total}] Record update marker")
    record_marker(job_id)
    _end_step()


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Run both Bayern import jobs, then promote them to live.",
    )
    ap.add_argument(
        "jobID",
        help=(
            "Job identifier. Each job imports into "
            "<state>_alkis_<job>_<jobID>_importing, which becomes "
            "<state>_alkis_<job>_<jobID> on success and is promoted to the live "
            "<state>_alkis_<job> once every job has completed."
        ),
    )
    ap.add_argument(
        "--no-src-zip",
        action="store_true",
        help="Buildings job only: do not create/populate src_zip column",
    )
    ap.add_argument(
        "--download",
        action="store_true",
        help=(
            "Only download the sources and record the update marker in "
            "the update_state table: no unzip, no import, no promotion or renaming"
        ),
    )
    args = ap.parse_args()

    # Started here, not before parsing, so --help prints no total.
    global _run_started
    _run_started = time.monotonic()

    # --download runs none of these, so it must not require them installed.
    if not args.download:
        for tool in ("ogr2ogr", "ogrinfo", "pg_dump"):
            if shutil.which(tool) is None:
                raise RuntimeError(f"{tool} not found on PATH.")

    # Everything the jobs need comes from config.py, so this must succeed
    # before anything else.
    load_config()
    print(f"Config loaded:     {config.CONFIG_PATH}")
    if not args.download:
        print(f"Target PostgreSQL: {PG_USER}@{PG_HOST}:{PG_PORT}")
    print(f"Downloads:         {DOWNLOAD_DIR}")

    if args.download:
        print("Mode:              --download (no unzip, no import, no promotion)")
        print("")
        print(f"Job ID: {args.jobID}")
        DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
        download_only(args.jobID)
        return

    print(f"Backup directory:  {BACKUP_DIR}/{STATE}/{args.jobID}")
    print(
        f"Connection drain:  retry every {CONN_WAIT_INTERVAL_SECONDS}s, "
        f"give up after {CONN_WAIT_TIMEOUT_SECONDS}s"
    )

    # (job name, step number, entry point, keep the source's failure banner)
    plan = [
        ("building", 1, run_building_job, False),
        ("usage",    2, run_usage_job,    True),   # keeps fetch_usage.py's banner
    ]
    names = {job: db_names(job, args.jobID) for job, _n, _r, _b in plan}
    total = len(plan)

    print("")
    print(f"Job ID: {args.jobID}")
    for job, _num, _run, _b in plan:
        n = names[job]
        print(f"  {job:9s} {n['importing']}")
        print(f"  {'':9s}   -> {n['working']}  -> live {n['live']}  (retiring to {n['old']})")

    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    _banner("PRE-FLIGHT")
    with _timed("PRE-FLIGHT"):
        preflight(plan, names, args.jobID)

    failed: list[str] = []

    for job, num, run, banner_on_fail in plan:
        n = names[job]
        _banner(f"JOB {num}/{total}  {job.upper()}  ->  {n['importing']}")
        with _timed(f"JOB {num}/{total} {job.upper()}"):
            try:
                run(args, n["importing"])
                # Import finished: drop the _importing marker.
                rename_database(n["importing"], n["working"])
            except Exception as exc:
                if banner_on_fail:
                    print("", file=sys.stderr)
                    print("=" * 60, file=sys.stderr)
                    print("IMPORT FAILED / DATABASE MUST NOT BE PROMOTED", file=sys.stderr)
                    print("=" * 60, file=sys.stderr)
                    print(str(exc), file=sys.stderr)
                    print("=" * 60, file=sys.stderr)
                else:
                    traceback.print_exc()
                print("")
                print(
                    f"JOB {num} ({job} -> {n['importing']}) FAILED; "
                    "continuing with the remaining jobs.",
                    file=sys.stderr,
                )
                failed.append(job)

    # Promotion is all-or-nothing: a single failed job means the live set is
    # left exactly as it was.
    if failed:
        print("")
        print(f"FAILED job(s): {', '.join(failed)}", file=sys.stderr)
        print(
            "NOT PROMOTED. The live databases are untouched and the working "
            "databases have been kept for inspection.",
            file=sys.stderr,
        )
        sys.exit(1)

    with _timed("PROMOTE"):
        promote(plan, names, args.jobID)

    print("")
    print(f"All {total} job(s) completed and promoted to live.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\\nInterrupted.", file=sys.stderr)
        sys.exit(130)
    finally:
        # Reached on success, on sys.exit(1) after a failed job, and on Ctrl-C.
        if _run_started is not None:
            _end_step()
            _banner(f"TOTAL elapsed {_elapsed(_run_started)}")
