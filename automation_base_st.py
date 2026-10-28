#!/usr/bin/env python3
"""
automation_base_st.py

Merged Sachsen-Anhalt (ST) import automation. Runs ALL THREE jobs, in order, on
every invocation:

  JOB 1  BUILDING  (from st/building/fetch_building.py)
         - GBIS_Gebaeude.zip  -> table gbis_gebaeude
           Schema is DISCOVERED by scanning the XML properties, then every
           feature is stream-parsed and COPYed in. Adds geom_wkt + PostGIS geom
           (MultiPolygon, 25832) + GiST index.
         - gebaeudereferenzen.zip (semicolon TXT) -> table gebaeudereferenzen
           Loaded via an UNLOGGED staging table, then geom (Point, 25832) is
           built from x/y. GiST + (str, hnr) indexes.
         PostGIS is REQUIRED for this job.
         Live DB: st_alkis_building

  JOB 2  PARCEL    (from st/parcel/fetch_parcel.py)
         - GBIS_Flurstuecke.zip -> table gbis_flurstuecke
           Explicit Flurstueck column model (FLST_COLS), all XML files in the
           zip, stream-parsed + COPY. Only adv:Flurstueck features are kept.
         Live DB: st_alkis_parcel

  JOB 3  USAGE     (from st/usage/fetch_usage.py)
         - GBIS_TN.zip (expects one XML) -> table named after the XML file
           (gbis_tn), columns oid/gml_id/feature_type/aktualit/nutzart/geom_wkt.
         Live DB: st_alkis_usage
         (the destination TABLE stays gbis_tn)

DATABASE LIFECYCLE
  Every job moves through four names. For job <job> and the jobID argument:

      st_alkis_<job>_<jobID>_importing   created first (from alkis_template);
                                        the suffix means the import has not
                                        finished
      st_alkis_<job>_<jobID>             suffix dropped once the job completes
      st_alkis_<job>                     LIVE -- what consumers read
      st_alkis_<job>_old                 previous live, kept for rollback

  Promotion happens only after EVERY job has completed. It then, for each job:
    1. pg_dumps the live and the working database into
       <backup_dir>/st/<jobID>/;
    2. zips the archives downloaded this run into the same folder;
    3. drops st_alkis_<job>_old if it exists;
    4. renames the live database to st_alkis_<job>_old;
    5. renames the working database to st_alkis_<job>;
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
  The three jobs are kept deliberately independent. 14 helper names are defined
  in more than one source file -- and 8 of those have DIFFERENT bodies between
  files -- so each job carries its own prefixed copy:

      _bld_*   from st/building/fetch_building.py
      _prc_*   from st/parcel/fetch_parcel.py
      _usg_*   from st/usage/fetch_usage.py

  Prefixed: add_postgis_geom, connect, copy_rows, download_file,
  ensure_database, ensure_table, feature_to_row, parse_isoish, poslist_to_ring,
  ring_to_wkt, sanitize_ident, stream_import, try_enable_postgis, unzip_all.

  Editing one job's copy therefore cannot affect another job. Helpers unique to
  a single source file keep their original names. Each section also retains its
  original source formatting.

  NS / WFS_MEMBER_TAG / GML_ID_ATTR are byte-identical in all three sources and
  are shared as a single copy below.

FAILURE BEHAVIOUR
  None of the three ST sources has a validation gate, so none is invented here:
  a job that fails prints a traceback. A failure in one job does NOT prevent the
  others from running -- all three import into separate databases. But promotion
  is ALL-OR-NOTHING: if any job failed, nothing is promoted, the live databases
  are left exactly as they were, and the working databases are kept for
  inspection. The process exits 1, naming which job failed.

CONFIGURATION
  All connection details, urls, batch sizes and PostGIS switches come from
  config.py, which reads the project's config.ini from beside itself -- no
  hardcoded fallbacks and no --host/--port flags. Keys in the shared job groups
  are prefixed st_ / by_ so that automation_base_st.py and automation_base_by.py
  can share one file:

      [paths]     backup_dir                              (shared with main.py)
      [promote]   conn_wait_interval_seconds,
                  conn_wait_timeout_seconds,
                  force_terminate_app_names,
                  force_terminate_exclude                 (shared, unprefixed)
      [database]  host, port, user, password              (shared with main.py)
      [building]  st_GBIS_GEBAEUDE_ZIP_URL, st_GEBAEUDE_REF_ZIP_URL,
                  st_batch_size                           (by_URLS is the BY job)
      [parcel]    st_ZIP_URL, st_batch_size, st_postgis
      [usage]     st_ZIP_URL, st_batch_size, st_postgis   (by_GPKG_URL is BY)

  config.ini holds plaintext database passwords: chmod 600 it.

Requires:
  sudo apt-get install -y postgresql-client        (pg_dump, for the backups)
  pip install requests psycopg2-binary
  (no GDAL -- unlike the BY jobs, these parse XML directly)

DOWNLOADS AND BACKUPS
  Source files are downloaded to a fixed, project-relative directory:

      <dir containing this script>/_downloads/st

  There is no --workdir flag. After a successful promotion the downloaded
  archives are zipped into the backup folder and the directory is emptied:

      <backup_dir>/st/<jobID>/
          st_alkis_<job>.dump                each outgoing live database
          st_alkis_<job>_<jobID>.dump        each newly imported database
          st_downloads_<jobID>_<ts>.zip     the archives fetched this run

  The zip holds only the files fetched from the source urls, not the directories
  they were unpacked into -- those are reproducible from the archives and would
  multiply the size. It is stored uncompressed, since the sources are already
  compressed.

  NOTE: the download directory is NOT jobID-scoped. Two job IDs running
  concurrently share it and will overwrite each other's files.

Run (from any directory):
  python automation_base_st.py <jobID>

  Re-running a jobID whose working database still exists is refused, because
  that database holds a completed but unpromoted import. Drop it or use a new
  jobID. A leftover *_importing database is scratch and is dropped
  automatically at pre-flight.
"""

import argparse
import contextlib
import csv
import datetime as dt
import os
import re
import shutil
import subprocess
import sys
import time
import traceback
import zipfile
from io import StringIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
import psycopg2
from psycopg2 import sql, errors
from xml.etree.ElementTree import iterparse

import check_update
import config
import state_db


# ============================================================================
# CONFIGURATION
#
# Every value below is taken from config.py by load_config(), which main()
# calls before any job runs. Nothing here has a
# hardcoded fallback: a missing section or key is a hard error, so a typo in the
# config file fails immediately and loudly rather than silently importing the
# wrong database.
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
STATE = "st"

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

# [database] -- local target PostgreSQL, shared by all three jobs
PG_HOST: str | None = None
PG_PORT: int | None = None
PG_USER: str | None = None
PG_PASSWORD: str | None = None

# [building] st_*
ST_GBIS_GEBAEUDE_ZIP_URL: str | None = None
ST_GEBAEUDE_REF_ZIP_URL: str | None = None
ST_BUILDING_BATCH_SIZE: int | None = None

# [parcel] st_*
ST_PARCEL_ZIP_URL: str | None = None
ST_PARCEL_BATCH_SIZE: int | None = None
ST_PARCEL_POSTGIS: bool | None = None

# [usage] st_*
ST_USAGE_ZIP_URL: str | None = None
ST_USAGE_BATCH_SIZE: int | None = None
ST_USAGE_POSTGIS: bool | None = None


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
    global ST_GBIS_GEBAEUDE_ZIP_URL, ST_GEBAEUDE_REF_ZIP_URL, ST_BUILDING_BATCH_SIZE
    global ST_PARCEL_ZIP_URL, ST_PARCEL_BATCH_SIZE, ST_PARCEL_POSTGIS
    global ST_USAGE_ZIP_URL, ST_USAGE_BATCH_SIZE, ST_USAGE_POSTGIS

    # ---- [database] ----
    # The same login main.py uses, so ALKIS_DB_USER / ALKIS_DB_PASSWORD apply.
    PG_HOST = _require(config.DB_HOST or None, "[database] host")
    PG_PORT = int(_require(config.DB_PORT or None, "[database] port"))
    PG_USER = config.DB_USER
    PG_PASSWORD = config.DB_PASSWORD

    # ---- [building] -- st_ keys only; by_URLS belongs to automation_base_by ----
    ST_GBIS_GEBAEUDE_ZIP_URL = _require(config.ST_GBIS_GEBAEUDE_ZIP_URL, "[building] st_GBIS_GEBAEUDE_ZIP_URL")
    ST_GEBAEUDE_REF_ZIP_URL = _require(config.ST_GEBAEUDE_REF_ZIP_URL, "[building] st_GEBAEUDE_REF_ZIP_URL")
    ST_BUILDING_BATCH_SIZE = _require(config.ST_BUILDING_BATCH_SIZE, "[building] st_batch_size")

    # ---- [parcel] ----
    ST_PARCEL_ZIP_URL = _require(config.ST_PARCEL_ZIP_URL, "[parcel] st_ZIP_URL")
    ST_PARCEL_BATCH_SIZE = _require(config.ST_PARCEL_BATCH_SIZE, "[parcel] st_batch_size")
    ST_PARCEL_POSTGIS = _require(config.ST_PARCEL_POSTGIS, "[parcel] st_postgis")

    # ---- [usage] -- st_ keys only; by_GPKG_URL belongs to automation_base_by ----
    ST_USAGE_ZIP_URL = _require(config.ST_USAGE_ZIP_URL, "[usage] st_ZIP_URL")
    ST_USAGE_BATCH_SIZE = _require(config.ST_USAGE_BATCH_SIZE, "[usage] st_batch_size")
    ST_USAGE_POSTGIS = _require(config.ST_USAGE_POSTGIS, "[usage] st_postgis")

    # ---- backup location ([paths]) and the connection-drain timings ([promote]) ----
    BACKUP_DIR = str(config.BACKUP_DIR)
    CONN_WAIT_INTERVAL_SECONDS = _require(config.CONN_WAIT_INTERVAL_SECONDS, "[promote] conn_wait_interval_seconds")
    CONN_WAIT_TIMEOUT_SECONDS = _require(config.CONN_WAIT_TIMEOUT_SECONDS, "[promote] conn_wait_timeout_seconds")
    FORCE_TERMINATE_APP_NAMES = _require(config.FORCE_TERMINATE_APP_NAMES, "[promote] force_terminate_app_names")
    FORCE_TERMINATE_EXCLUDE = _require(config.FORCE_TERMINATE_EXCLUDE, "[promote] force_terminate_exclude")


# ============================================================================
# SHARED ALKIS XML NAMESPACES
#
# Byte-identical in all three source files, so kept as a single copy. Every
# carried-over function that references these therefore stays unchanged.
# ============================================================================
NS = {
    "wfs": "http://www.opengis.net/wfs/2.0",
    "gml": "http://www.opengis.net/gml/3.2",
    "adv": "http://repository.gdi-de.org/schemas/adv/produkt/alkis-vereinfacht/2.0",
}

WFS_MEMBER_TAG = f"{{{NS['wfs']}}}member"
GML_ID_ATTR = f"{{{NS['gml']}}}id"


# ############################################################################
# ############################################################################
# ##                                                                        ##
# ##   JOB 1 -- SACHSEN-ANHALT BUILDING
# ##   source: st/building/fetch_building.py
# ##                                                                        ##
# ############################################################################
# ############################################################################



def localname(tag: str) -> str:
    return tag.split("}")[-1] if "}" in tag else tag


def _bld_sanitize_ident(name: str, prefix: str = "t_", maxlen: int = 63) -> str:
    n = name.lower()
    n = re.sub(r"[^a-z0-9_]+", "_", n).strip("_")
    if not n or not re.match(r"^[a-z_]", n):
        n = prefix + (n if n else "x")
    return n[:maxlen]


def _bld_download_file(url: str, out_path: Path, timeout: int = 180) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".part")
    with requests.get(url, stream=True, timeout=timeout, allow_redirects=True) as r:
        r.raise_for_status()
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
    tmp.replace(out_path)


def _bld_unzip_all(zip_path: Path, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(out_dir)
    return out_dir


def find_files(root: Path, suffix: str) -> List[Path]:
    return sorted([p for p in root.rglob(f"*{suffix}") if p.is_file()])


def _bld_parse_isoish(s: Optional[str]) -> Optional[dt.datetime]:
    if not s:
        return None
    s = s.strip()
    if s.endswith("Z") and "T" not in s:
        try:
            d = dt.date.fromisoformat(s[:-1])
            return dt.datetime(d.year, d.month, d.day, tzinfo=dt.timezone.utc)
        except Exception:
            return None
    if s.endswith("Z") and "T" in s:
        s = s[:-1] + "+00:00"
    try:
        return dt.datetime.fromisoformat(s)
    except Exception:
        return None


def _bld_poslist_to_ring(poslist_text: str) -> List[Tuple[float, float]]:
    nums = [n for n in poslist_text.strip().split() if n]
    if len(nums) < 8 or len(nums) % 2 != 0:
        return []
    coords = []
    for i in range(0, len(nums), 2):
        coords.append((float(nums[i]), float(nums[i + 1])))
    if coords and coords[0] != coords[-1]:
        coords.append(coords[0])
    return coords


def _bld_ring_to_wkt(ring: List[Tuple[float, float]]) -> str:
    return ", ".join(f"{x} {y}" for x, y in ring)


def feature_geometry_wkt(feature_elem) -> Optional[str]:
    polys = []
    for poly in feature_elem.findall(".//gml:Polygon", namespaces=NS):
        pos = poly.find(".//gml:exterior//gml:posList", namespaces=NS)
        if pos is None or not (pos.text and pos.text.strip()):
            continue
        ring = _bld_poslist_to_ring(pos.text)
        if not ring:
            continue
        polys.append(f"(({_bld_ring_to_wkt(ring)}))")
    if not polys:
        return None
    return "MULTIPOLYGON(" + ", ".join(polys) + ")"


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


def require_postgis(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
    conn.commit()


# -------------------------
# TXT: gebaeudereferenzen
# -------------------------

TXT_COLS = [
    "id",
    "landschl",
    "regbezschl",
    "kreisschl",
    "gmdschl",
    "ottschl",
    "strschl",
    "str",
    "hnr",
    "x",
    "y",
]

def ensure_gebaeudereferenzen_table(conn, table_name: str) -> None:
    with conn.cursor() as cur:
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {};").format(sql.Identifier(table_name)))
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE {} (
                  id TEXT PRIMARY KEY,
                  landschl TEXT,
                  regbezschl TEXT,
                  kreisschl TEXT,
                  gmdschl TEXT,
                  ottschl TEXT,
                  strschl TEXT,
                  str TEXT,
                  hnr TEXT,
                  x DOUBLE PRECISION,
                  y DOUBLE PRECISION,
                  geom geometry(Point, 25832)
                );
                """
            ).format(sql.Identifier(table_name))
        )
    conn.commit()


_COPY_ESCAPES = str.maketrans({"\\": "\\\\", "\t": "\\t", "\n": "\\n", "\r": "\\r"})


def _copy_line(values) -> str:
    """
    One row in COPY's text format: tab-separated, None as \\N, and backslash,
    tab, newline and CR backslash-escaped. Not csv.writer -- its escapechar
    turns \\N into \\\\N (a literal string, not NULL) and its quotes mean
    nothing to COPY's text format.
    """
    return "\t".join(r"\N" if v is None else str(v).translate(_COPY_ESCAPES)
                     for v in values) + "\n"


def copy_gebaeudereferenzen_txt(conn, table_name: str, txt_path: Path, batch_lines: int = 200000) -> int:
    """
    Streaming parse of the TXT using your fixed positions.
    Loads base cols via COPY into a temp staging table, then sets geom, then inserts into final.
    (Using a staging table avoids per-row INSERT overhead.)
    """
    staging = f"{table_name}__staging"

    with conn.cursor() as cur:
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {};").format(sql.Identifier(staging)))
        cur.execute(
            sql.SQL(
                """
                CREATE UNLOGGED TABLE {} (
                  id TEXT,
                  landschl TEXT,
                  regbezschl TEXT,
                  kreisschl TEXT,
                  gmdschl TEXT,
                  ottschl TEXT,
                  strschl TEXT,
                  str TEXT,
                  hnr TEXT,
                  x DOUBLE PRECISION,
                  y DOUBLE PRECISION
                );
                """
            ).format(sql.Identifier(staging))
        )
    conn.commit()

    total = 0

    def flush_copy(buf: StringIO, n_rows: int) -> None:
        buf.seek(0)
        with conn.cursor() as cur:
            cur.copy_from(buf, staging, columns=TXT_COLS, null="\\N")
        conn.commit()

    sio = StringIO()
    n_in_buf = 0

    with open(txt_path, encoding="utf-8", errors="ignore", newline="") as csvfile:
        reader = csv.reader(csvfile, delimiter=";")
        for row in reader:
            if not row or len(row) < 20:
                continue

            # Your fixed positions:
            _id = row[1].strip()
            if not _id:
                continue

            landschl = row[3].strip()
            regbezschl = row[5].strip()
            kreisschl = row[7].strip()
            gmdschl = row[9].strip()
            ottschl = row[11].strip()
            strschl = row[13].strip()
            street = row[14].strip()
            hnr = row[15].strip()
            if len(row[16].strip()) != 0:
                hnr = f"{hnr} {row[16].strip()}"

            try:
                x = float(row[18])
                y = float(row[19])
            except Exception:
                continue

            sio.write(_copy_line([
                _id,
                landschl or None,
                regbezschl or None,
                kreisschl or None,
                gmdschl or None,
                ottschl or None,
                strschl or None,
                street or None,
                hnr or None,
                x,
                y,
            ]))
            n_in_buf += 1
            total += 1

            if n_in_buf >= batch_lines:
                flush_copy(sio, n_in_buf)
                sio = StringIO()
                n_in_buf = 0

    if n_in_buf:
        flush_copy(sio, n_in_buf)

    # Move staging -> final, compute geom, create index
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                INSERT INTO {} (id, landschl, regbezschl, kreisschl, gmdschl, ottschl, strschl, str, hnr, x, y, geom)
                SELECT
                  id, landschl, regbezschl, kreisschl, gmdschl, ottschl, strschl, str, hnr, x, y,
                  ST_SetSRID(ST_MakePoint(x,y), 25832)
                FROM {};
                """
            ).format(sql.Identifier(table_name), sql.Identifier(staging))
        )
        cur.execute(sql.SQL("DROP TABLE {};").format(sql.Identifier(staging)))
        cur.execute(
            sql.SQL("CREATE INDEX {} ON {} USING GIST (geom);").format(
                sql.Identifier(f"{table_name}_geom_gist"),
                sql.Identifier(table_name),
            )
        )
        cur.execute(
            sql.SQL("CREATE INDEX {} ON {} (str, hnr);").format(
                sql.Identifier(f"{table_name}_str_hnr_idx"),
                sql.Identifier(table_name),
            )
        )
    conn.commit()

    return total


# -------------------------
# XML: gbis_gebaeude
# -------------------------

def scan_xml_properties(xml_paths: List[Path], limit_members_per_file: int = 2000) -> List[str]:
    wanted = []
    seen = set()
    for xml_path in xml_paths:
        ctx = iterparse(str(xml_path), events=("end",))
        scanned = 0
        for _, elem in ctx:
            if elem.tag != WFS_MEMBER_TAG:
                continue
            feature = next(iter(elem), None)
            if feature is not None:
                for ch in list(feature):
                    ln = localname(ch.tag)
                    if ln in {"oid", "aktualit", "geometrie", "identifier"}:
                        continue
                    if len(list(ch)) == 0 and ch.text and ch.text.strip():
                        if ln not in seen:
                            seen.add(ln)
                            wanted.append(ln)
            elem.clear()
            scanned += 1
            if scanned >= limit_members_per_file:
                break
    return wanted


def ensure_gbis_gebaeude_table(conn, table_name: str, prop_cols: List[str]) -> Tuple[List[str], Dict[str, str]]:
    """
    Table keeps XML-derived properties (text), plus geom_wkt (text) + geom (PostGIS) for indexing.
    No street/hnr columns.
    """
    mapping: Dict[str, str] = {}
    used = set()

    for p in prop_cols:
        col = _bld_sanitize_ident(p, prefix="p_")
        base = col
        k = 1
        while col in used or col in {"oid", "gml_id", "feature_type", "aktualit", "geom_wkt", "geom"}:
            k += 1
            col = f"{base}_{k}"[:63]
        used.add(col)
        mapping[p] = col

    copy_cols = ["oid", "gml_id", "feature_type", "aktualit"] + [mapping[p] for p in prop_cols] + ["geom_wkt"]

    col_defs = [
        sql.SQL("oid TEXT PRIMARY KEY"),
        sql.SQL("gml_id TEXT"),
        sql.SQL("feature_type TEXT"),
        sql.SQL("aktualit TIMESTAMPTZ"),
    ]
    for p in prop_cols:
        col_defs.append(sql.SQL("{} TEXT").format(sql.Identifier(mapping[p])))
    col_defs.extend([
        sql.SQL("geom_wkt TEXT"),
        sql.SQL("geom geometry(MultiPolygon, 25832)"),
    ])

    with conn.cursor() as cur:
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {};").format(sql.Identifier(table_name)))
        cur.execute(
            sql.SQL("CREATE TABLE {} ({});").format(
                sql.Identifier(table_name),
                sql.SQL(",\n  ").join(col_defs),
            )
        )
    conn.commit()
    return copy_cols, mapping


def _bld_feature_to_row(feature_elem, prop_cols: List[str]) -> Tuple[str, Optional[str], str, Optional[str], Dict[str, Optional[str]], Optional[str]]:
    feature_type = localname(feature_elem.tag)
    gml_id = feature_elem.get(GML_ID_ATTR)

    oid = (feature_elem.findtext("adv:oid", default="", namespaces=NS) or "").strip()
    if not oid:
        oid = gml_id or ""

    aktualit_raw = feature_elem.findtext("adv:aktualit", default=None, namespaces=NS)
    aktualit_dt = _bld_parse_isoish(aktualit_raw)
    aktualit_iso = aktualit_dt.isoformat() if aktualit_dt else None

    props: Dict[str, Optional[str]] = {}
    for ch in list(feature_elem):
        ln = localname(ch.tag)
        if ln in {"oid", "aktualit", "geometrie", "identifier"}:
            continue
        if ln in prop_cols and len(list(ch)) == 0:
            v = (ch.text or "").strip()
            props[ln] = v if v else None

    geom_wkt = feature_geometry_wkt(feature_elem)
    return oid, gml_id, feature_type, aktualit_iso, props, geom_wkt


def copy_gbis_batch(conn, table_name: str, copy_cols: List[str], prop_cols: List[str], rows: List[Tuple]) -> None:
    sio = StringIO()
    for (oid, gml_id, feature_type, aktualit_iso, props, geom_wkt) in rows:
        line = [oid, gml_id or None, feature_type or None, aktualit_iso or None]
        for p in prop_cols:
            line.append(props.get(p))
        line.append(geom_wkt or None)
        sio.write(_copy_line(line))
    sio.seek(0)
    with conn.cursor() as cur:
        cur.copy_from(sio, table_name, columns=copy_cols, null="\\N")
    conn.commit()


def build_and_index_geom(conn, table_name: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                UPDATE {} SET geom = ST_Multi(ST_GeomFromText(geom_wkt, 25832))
                WHERE geom IS NULL AND geom_wkt IS NOT NULL AND geom_wkt <> '';
                """
            ).format(sql.Identifier(table_name))
        )
        cur.execute(
            sql.SQL("CREATE INDEX {} ON {} USING GIST (geom);").format(
                sql.Identifier(f"{table_name}_geom_gist"),
                sql.Identifier(table_name),
            )
        )
    conn.commit()


def import_all_xml_into_one_table(conn, xml_files: List[Path], table_name: str, batch_size: int = 20000) -> int:
    prop_cols = scan_xml_properties(xml_files)
    copy_cols, _ = ensure_gbis_gebaeude_table(conn, table_name, prop_cols)

    total = 0
    for xml_path in xml_files:
        print(f"  -> parsing {xml_path.name}")
        batch: List[Tuple] = []
        ctx = iterparse(str(xml_path), events=("end",))
        for _, elem in ctx:
            if elem.tag != WFS_MEMBER_TAG:
                continue
            feature = next(iter(elem), None)
            if feature is not None:
                row = _bld_feature_to_row(feature, prop_cols)
                if row[0]:
                    batch.append(row)
                    total += 1
            elem.clear()
            if len(batch) >= batch_size:
                copy_gbis_batch(conn, table_name, copy_cols, prop_cols, batch)
                batch.clear()
        if batch:
            copy_gbis_batch(conn, table_name, copy_cols, prop_cols, batch)
            batch.clear()

    build_and_index_geom(conn, table_name)
    return total


def run_building_job(args, dbname: str) -> None:
    """
    JOB 1 entry point. Body is st/building/fetch_building.py main() with the
    argparse block lifted out to the shared CLI, the target database name passed
    in (derived from the jobID by main()), and the two source urls plus the batch
    size taken from config.ini.

    Zip filenames are now derived from the configured urls rather than hardcoded,
    so changing a url in config.ini changes the local filename with it.
    """
    gbis_zip_name = Path(ST_GBIS_GEBAEUDE_ZIP_URL).name
    ref_zip_name = Path(ST_GEBAEUDE_REF_ZIP_URL).name

    buildings_table = _bld_sanitize_ident(Path(gbis_zip_name).stem)   # gbis_gebaeude
    refs_table = _bld_sanitize_ident(Path(ref_zip_name).stem)         # gebaeudereferenzen

    workdir = DOWNLOAD_DIR
    workdir.mkdir(parents=True, exist_ok=True)

    gbis_zip = workdir / gbis_zip_name
    gbis_dir = workdir / Path(gbis_zip_name).stem

    ref_zip = workdir / ref_zip_name
    ref_dir = workdir / Path(ref_zip_name).stem

    _step(f"[1/6] Download buildings zip")
    _bld_download_file(ST_GBIS_GEBAEUDE_ZIP_URL, gbis_zip)
    DOWNLOADED_ARCHIVES.append(gbis_zip)

    _step(f"[2/6] Unzip buildings zip")
    _bld_unzip_all(gbis_zip, gbis_dir)
    xml_files = find_files(gbis_dir, ".xml")
    if not xml_files:
        raise RuntimeError(f"No XML files found in {gbis_dir}")

    _step(f"[3/6] Download refs zip")
    _bld_download_file(ST_GEBAEUDE_REF_ZIP_URL, ref_zip)
    DOWNLOADED_ARCHIVES.append(ref_zip)

    _step(f"[4/6] Unzip refs zip")
    _bld_unzip_all(ref_zip, ref_dir)
    txt_files = find_files(ref_dir, ".txt")
    if not txt_files:
        # fallback to single file
        candidates = [p for p in ref_dir.rglob("*") if p.is_file()]
        if len(candidates) == 1:
            txt_files = candidates
        else:
            raise RuntimeError(f"No .txt (or single file) found in {ref_dir}")
    ref_txt = txt_files[0]

    _step(f"[5/6] Ensure database exists: {dbname}")
    # CREATE DATABASE ... TEMPLATE needs the TEMPLATE free of sessions.
    wait_for_no_connections(TEMPLATE_DB)
    _bld_ensure_database(dbname, PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, template=TEMPLATE_DB)

    _step(f"[6/6] Import")
    with _bld_db(dbname, PG_HOST, PG_PORT, PG_USER, PG_PASSWORD) as conn:
        require_postgis(conn)

        print(f"  - Create & load TXT table: {refs_table}")
        ensure_gebaeudereferenzen_table(conn, refs_table)
        n_refs = copy_gebaeudereferenzen_txt(conn, refs_table, ref_txt)
        print(f"    Loaded refs rows: {n_refs}")

        print(f"  - Create & load XML table: {buildings_table}")
        n_bld = import_all_xml_into_one_table(
            conn, xml_files, buildings_table, batch_size=ST_BUILDING_BATCH_SIZE
        )
        print(f"    Loaded buildings rows: {n_bld}")

    _end_step()
    print("Done.")


# ############################################################################
# ############################################################################
# ##                                                                        ##
# ##   JOB 2 -- SACHSEN-ANHALT PARCEL
# ##   source: st/parcel/fetch_parcel.py
# ##                                                                        ##
# ############################################################################
# ############################################################################



def _prc_sanitize_ident(stem: str, prefix: str = "t_", maxlen: int = 63) -> str:
    name = stem.lower()
    name = re.sub(r"[^a-z0-9_]+", "_", name).strip("_")
    if not name or not re.match(r"^[a-z_]", name):
        name = f"{prefix}{name}" if name else f"{prefix}import"
    return name[:maxlen]


def _prc_download_file(url: str, out_path: Path, timeout: int = 180) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".part")

    with requests.get(url, stream=True, timeout=timeout) as r:
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


def _prc_unzip_all(zip_path: Path, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(out_dir)
    return out_dir


def find_xml_files(root: Path) -> List[Path]:
    return sorted([p for p in root.rglob("*.xml") if p.is_file()])


def _prc_parse_isoish(s: Optional[str]) -> Optional[dt.datetime]:
    if not s:
        return None
    s = s.strip()
    if s.endswith("Z") and "T" not in s:
        try:
            d = dt.date.fromisoformat(s[:-1])
            return dt.datetime(d.year, d.month, d.day, tzinfo=dt.timezone.utc)
        except Exception:
            return None
    if s.endswith("Z") and "T" in s:
        s = s[:-1] + "+00:00"
    try:
        return dt.datetime.fromisoformat(s)
    except Exception:
        return None


def _prc_poslist_to_ring(poslist_text: str) -> List[Tuple[float, float]]:
    nums = [n for n in poslist_text.strip().split() if n]
    if len(nums) < 8 or len(nums) % 2 != 0:
        return []
    coords: List[Tuple[float, float]] = []
    for i in range(0, len(nums), 2):
        coords.append((float(nums[i]), float(nums[i + 1])))
    if coords and coords[0] != coords[-1]:
        coords.append(coords[0])
    return coords


def _prc_ring_to_wkt(ring: List[Tuple[float, float]]) -> str:
    return ", ".join(f"{x} {y}" for x, y in ring)


def multipolygon_wkt_from_feature(feature_elem) -> Optional[str]:
    parts: List[str] = []
    for poly in feature_elem.findall(".//gml:Polygon", namespaces=NS):
        pos = poly.find(".//gml:exterior//gml:posList", namespaces=NS)
        if pos is None or not (pos.text and pos.text.strip()):
            continue
        ring = _prc_poslist_to_ring(pos.text)
        if not ring:
            continue
        parts.append(f"(({_prc_ring_to_wkt(ring)}))")
    if not parts:
        return None
    return "MULTIPOLYGON(" + ", ".join(parts) + ")"


def _prc_connect(dbname: str, host: str, port: int, user: str, password: str):
    return psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)


@contextlib.contextmanager
def _prc_db(dbname: str, host: str, port: int, user: str, password: str):
    """
    Connection that is actually CLOSED on exit.

    psycopg2's own connection context manager commits or rolls back the
    transaction but leaves the socket open. Promotion renames these databases,
    and PostgreSQL refuses to rename a database with any session attached, so
    every connection has to be closed explicitly. The inner `with conn` keeps
    the original commit/rollback semantics.
    """
    conn = _prc_connect(dbname, host, port, user, password)
    try:
        with conn:
            yield conn
    finally:
        conn.close()

def _prc_ensure_database(dbname: str, host: str, port: int, user: str, password: str, template: str | None = None) -> None:
    conn = None
    try:
        conn = _prc_connect("postgres", host, port, user, password)
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


def _prc_try_enable_postgis(conn) -> bool:
    with conn.cursor() as cur:
        try:
            cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
            conn.commit()
            return True
        except Exception:
            conn.rollback()
            return False


# ---- explicit Flurstueck column model (from your snippet) ----

FLST_COLS = [
    "oid",          # adv:oid (primary key)
    "gml_id",       # gml:id attribute
    "aktualit",     # adv:aktualit
    "idflurst",
    "flstkennz",
    "land",
    "landschl",
    "gemarkung",
    "gemaschl",
    "flur",
    "flurschl",
    "flstnrzae",
    "flstnrnen",
    "kreis",
    "kreisschl",
    "gemeinde",
    "gmdschl",
    "flaeche",
    "abwrecht",
    "lagebeztxt",
    "tntxt",
    "geom_wkt",
]

def _prc_ensure_table(conn, table_name: str) -> None:
    with conn.cursor() as cur:
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {};").format(sql.Identifier(table_name)))
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE {} (
                  oid TEXT PRIMARY KEY,
                  gml_id TEXT,
                  aktualit TIMESTAMPTZ,

                  idflurst TEXT,
                  flstkennz TEXT,
                  land TEXT,
                  landschl TEXT,
                  gemarkung TEXT,
                  gemaschl TEXT,
                  flur TEXT,
                  flurschl TEXT,
                  flstnrzae TEXT,
                  flstnrnen TEXT,
                  kreis TEXT,
                  kreisschl TEXT,
                  gemeinde TEXT,
                  gmdschl TEXT,

                  flaeche DOUBLE PRECISION,
                  abwrecht TEXT,
                  lagebeztxt TEXT,
                  tntxt TEXT,

                  geom_wkt TEXT
                );
                """
            ).format(sql.Identifier(table_name))
        )
    conn.commit()


def _prc_add_postgis_geom(conn, table_name: str, srid: int = 25832) -> None:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                ALTER TABLE {} ADD COLUMN IF NOT EXISTS geom geometry(MultiPolygon, %s);
                UPDATE {} SET geom = ST_Multi(ST_GeomFromText(geom_wkt, %s))
                  WHERE geom IS NULL AND geom_wkt IS NOT NULL AND geom_wkt <> '';
                CREATE INDEX IF NOT EXISTS {} ON {} USING GIST (geom);
                """
            ).format(
                sql.Identifier(table_name),
                sql.Identifier(table_name),
                sql.Identifier(f"{table_name}_geom_gist"),
                sql.Identifier(table_name),
            ),
            (srid, srid),
        )
    conn.commit()


def get_text(feature_elem, name: str) -> Optional[str]:
    # The feature uses default namespace == NS['adv'], so search as adv:<name>
    v = feature_elem.findtext(f"adv:{name}", default=None, namespaces=NS)
    if v is None:
        return None
    v = v.strip()
    return v if v else None


def _prc_feature_to_row(feature_elem) -> Tuple:
    gml_id = feature_elem.get(GML_ID_ATTR)

    oid = get_text(feature_elem, "oid") or (gml_id or "")
    aktualit_raw = get_text(feature_elem, "aktualit")
    aktualit_dt = _prc_parse_isoish(aktualit_raw)
    aktualit_iso = aktualit_dt.isoformat() if aktualit_dt else None

    # numeric field
    flaeche_raw = get_text(feature_elem, "flaeche")
    try:
        flaeche = float(flaeche_raw) if flaeche_raw is not None else None
    except Exception:
        flaeche = None

    geom_wkt = multipolygon_wkt_from_feature(feature_elem)

    return (
        oid,
        gml_id,
        aktualit_iso,
        get_text(feature_elem, "idflurst"),
        get_text(feature_elem, "flstkennz"),
        get_text(feature_elem, "land"),
        get_text(feature_elem, "landschl"),
        get_text(feature_elem, "gemarkung"),
        get_text(feature_elem, "gemaschl"),
        get_text(feature_elem, "flur"),
        get_text(feature_elem, "flurschl"),
        get_text(feature_elem, "flstnrzae"),
        get_text(feature_elem, "flstnrnen"),
        get_text(feature_elem, "kreis"),
        get_text(feature_elem, "kreisschl"),
        get_text(feature_elem, "gemeinde"),
        get_text(feature_elem, "gmdschl"),
        flaeche,
        get_text(feature_elem, "abwrecht"),
        get_text(feature_elem, "lagebeztxt"),
        get_text(feature_elem, "tntxt"),
        geom_wkt,
    )


def _prc_copy_rows(conn, table_name: str, rows: List[Tuple]) -> None:
    sio = StringIO()
    for r in rows:
        sio.write(_copy_line(r))
    sio.seek(0)

    with conn.cursor() as cur:
        cur.copy_from(
            file=sio,
            table=table_name,
            columns=FLST_COLS,
            null="\\N",
        )
    conn.commit()


def _prc_stream_import(xml_path: Path, conn, table_name: str, batch_size: int = 20000) -> int:
    count = 0
    batch: List[Tuple] = []
    context = iterparse(str(xml_path), events=("end",))
    for _, elem in context:
        if elem.tag == WFS_MEMBER_TAG:
            feature = next(iter(elem), None)
            if feature is not None:
                # we only want Flurstueck features (ignore others if present)
                if feature.tag.endswith("Flurstueck"):
                    row = _prc_feature_to_row(feature)
                    if row[0]:
                        batch.append(row)
                        count += 1
            elem.clear()

            if len(batch) >= batch_size:
                _prc_copy_rows(conn, table_name, batch)
                batch.clear()

    if batch:
        _prc_copy_rows(conn, table_name, batch)
        batch.clear()

    return count


def run_parcel_job(args, dbname: str) -> None:
    """
    JOB 2 entry point. Body is st/parcel/fetch_parcel.py main() with the argparse
    block lifted out to the shared CLI, the target database name passed in, and
    the url / batch size / postgis switch taken from config.ini.

    The original --no-postgis flag is now the [parcel] st_postgis config key.
    """
    workdir = DOWNLOAD_DIR
    workdir.mkdir(parents=True, exist_ok=True)

    zip_name = Path(ST_PARCEL_ZIP_URL).name          # GBIS_Flurstuecke.zip
    zip_path = workdir / zip_name
    extract_dir = workdir / Path(zip_name).stem
    table_name = _prc_sanitize_ident(Path(zip_name).stem)   # gbis_flurstuecke

    _step(f"[1/5] Download: {ST_PARCEL_ZIP_URL}")
    _prc_download_file(ST_PARCEL_ZIP_URL, zip_path)
    DOWNLOADED_ARCHIVES.append(zip_path)

    _step(f"[2/5] Unzip -> {extract_dir}")
    _prc_unzip_all(zip_path, extract_dir)

    xml_files = find_xml_files(extract_dir)
    if not xml_files:
        raise RuntimeError(f"No XML files found in {extract_dir}")

    _step(f"[3/5] Ensure DB: {dbname}")
    # CREATE DATABASE ... TEMPLATE needs the TEMPLATE free of sessions.
    wait_for_no_connections(TEMPLATE_DB)
    _prc_ensure_database(dbname, PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, template=TEMPLATE_DB)

    with _prc_db(dbname, PG_HOST, PG_PORT, PG_USER, PG_PASSWORD) as conn:
        _step(f"[4/5] Create table: {table_name}")
        _prc_ensure_table(conn, table_name)

        _step(f"[5/5] Import {len(xml_files)} XML files")
        total = 0
        for p in xml_files:
            n = _prc_stream_import(p, conn, table_name, batch_size=ST_PARCEL_BATCH_SIZE)
            total += n
            print(f"      {p.name}: {n}")
        print(f"      TOTAL: {total}")

        if ST_PARCEL_POSTGIS:
            if _prc_try_enable_postgis(conn):
                _prc_add_postgis_geom(conn, table_name, srid=25832)
                print("      PostGIS geom + GiST index created.")
            else:
                print("      PostGIS could not be enabled; kept geom_wkt only.")

    _end_step()
    print("Done.")


# ############################################################################
# ############################################################################
# ##                                                                        ##
# ##   JOB 3 -- SACHSEN-ANHALT USAGE
# ##   source: st/usage/fetch_usage.py
# ##                                                                        ##
# ############################################################################
# ############################################################################



def sanitize_table_name(stem: str) -> str:
    name = stem.lower()
    name = re.sub(r"[^a-z0-9_]+", "_", name).strip("_")
    if not name or not re.match(r"^[a-z_]", name):
        name = f"t_{name}" if name else "t_import"
    return name


def _usg_download_file(url: str, out_path: Path, timeout: int = 60) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with requests.get(url, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        expected = r.headers.get("Content-Length")
        expected_bytes = int(expected) if expected and expected.isdigit() else None

        written = 0
        tmp_path = out_path.with_suffix(out_path.suffix + ".part")
        with open(tmp_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
                    written += len(chunk)

        if expected_bytes is not None and written != expected_bytes:
            tmp_path.unlink(missing_ok=True)
            raise RuntimeError(
                f"Download size mismatch: wrote {written} bytes, expected {expected_bytes} bytes. "
                f"(The server or network may have truncated the file.)"
            )

        tmp_path.replace(out_path)


def unzip_single_xml(zip_path: Path, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zf:
        xml_members = [m for m in zf.namelist() if m.lower().endswith(".xml")]
        if not xml_members:
            raise RuntimeError("ZIP does not contain any .xml file.")
        if len(xml_members) > 1:
            # If there are multiple, still pick the first deterministically
            xml_members.sort()
        xml_member = xml_members[0]

        extracted_path = out_dir / Path(xml_member).name
        with zf.open(xml_member) as src, open(extracted_path, "wb") as dst:
            dst.write(src.read())

    return extracted_path


def _usg_parse_isoish(s: Optional[str]) -> Optional[dt.datetime]:
    if not s:
        return None
    s = s.strip()
    # Examples: "2022-01-19Z" or "2025-12-16T05:35:02.086+01:00"
    if s.endswith("Z") and "T" not in s:
        # date-only with Z
        try:
            d = dt.date.fromisoformat(s[:-1])
            return dt.datetime(d.year, d.month, d.day, tzinfo=dt.timezone.utc)
        except Exception:
            return None
    if s.endswith("Z") and "T" in s:
        s = s[:-1] + "+00:00"
    try:
        return dt.datetime.fromisoformat(s)
    except Exception:
        return None


def _usg_poslist_to_ring(poslist_text: str) -> List[Tuple[float, float]]:
    nums = [n for n in poslist_text.strip().split() if n]
    if len(nums) < 8 or len(nums) % 2 != 0:
        return []
    coords = []
    for i in range(0, len(nums), 2):
        coords.append((float(nums[i]), float(nums[i + 1])))

    # Ensure closed ring
    if coords and coords[0] != coords[-1]:
        coords.append(coords[0])
    return coords


def _usg_ring_to_wkt(ring: List[Tuple[float, float]]) -> str:
    return ", ".join(f"{x} {y}" for x, y in ring)


def _usg_feature_to_row(feature_elem) -> Tuple[str, Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Extract a minimal row:
      oid, gml_id, feature_type, aktualit (ISO string), nutzart, geom_wkt (MULTIPOLYGON WKT or None)

    We:
      - look for adv:oid, adv:aktualit, adv:nutzart
      - build MULTIPOLYGON from any gml:Polygon exterior posList found in the feature
    """
    feature_type = feature_elem.tag.split("}")[-1] if "}" in feature_elem.tag else feature_elem.tag
    gml_id = feature_elem.get(GML_ID_ATTR)

    oid = feature_elem.findtext("adv:oid", default="", namespaces=NS).strip()
    if not oid:
        # fall back: sometimes oid is absent; keep gml_id as pseudo-id if available
        oid = gml_id or ""

    aktualit_raw = feature_elem.findtext("adv:aktualit", default=None, namespaces=NS)
    aktualit_dt = _usg_parse_isoish(aktualit_raw)
    aktualit_iso = aktualit_dt.isoformat() if aktualit_dt else None

    nutzart = feature_elem.findtext("adv:nutzart", default=None, namespaces=NS)
    if nutzart is not None:
        nutzart = nutzart.strip() or None

    # Collect polygons (exterior only)
    polygons_wkt_parts: List[str] = []
    for poly in feature_elem.findall(".//gml:Polygon", namespaces=NS):
        pos = poly.find(".//gml:exterior//gml:posList", namespaces=NS)
        if pos is None or not (pos.text and pos.text.strip()):
            continue
        ring = _usg_poslist_to_ring(pos.text)
        if not ring:
            continue
        polygons_wkt_parts.append(f"(({_usg_ring_to_wkt(ring)}))")

    geom_wkt = None
    if polygons_wkt_parts:
        geom_wkt = "MULTIPOLYGON(" + ", ".join(polygons_wkt_parts) + ")"

    return (oid, gml_id, feature_type, aktualit_iso, nutzart, geom_wkt)


def _usg_connect(dbname: str, host: str, port: int, user: str, password: str):
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

def _usg_ensure_database(dbname: str, host: str, port: int, user: str, password: str, template: str | None = None) -> None:
    conn = None
    try:
        conn = _usg_connect("postgres", host, port, user, password)
        conn.autocommit = True  # REQUIRED for CREATE DATABASE
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (dbname,))
            exists = cur.fetchone() is not None
            if not exists:
                if template:
                    cur.execute(
                        sql.SQL("CREATE DATABASE {} TEMPLATE {}").format(
                            sql.Identifier(dbname),
                            sql.Identifier(template),
                        )
                    )
                else:
                    cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
    finally:
        if conn is not None:
            conn.close()

def _usg_ensure_table(conn, table_name: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {} (
                  oid TEXT PRIMARY KEY,
                  gml_id TEXT,
                  feature_type TEXT,
                  aktualit TIMESTAMPTZ,
                  nutzart TEXT,
                  geom_wkt TEXT
                );
                """
            ).format(sql.Identifier(table_name))
        )
    conn.commit()


def _usg_try_enable_postgis(conn) -> bool:
    with conn.cursor() as cur:
        try:
            cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
            conn.commit()
            return True
        except Exception:
            conn.rollback()
            return False


def _usg_add_postgis_geom(conn, table_name: str, srid: int = 25832) -> None:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                ALTER TABLE {} ADD COLUMN IF NOT EXISTS geom geometry(MultiPolygon, %s);
                UPDATE {} SET geom = ST_Multi(ST_GeomFromText(geom_wkt, %s))
                  WHERE geom IS NULL AND geom_wkt IS NOT NULL AND geom_wkt <> '';
                CREATE INDEX IF NOT EXISTS {} ON {} USING GIST (geom);
                """
            ).format(
                sql.Identifier(table_name),
                sql.Identifier(table_name),
                sql.Identifier(f"{table_name}_geom_gist"),
                sql.Identifier(table_name),
            ),
            (srid, srid),
        )
    conn.commit()


def _usg_copy_rows(conn, table_name: str, rows: List[Tuple[str, Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]]) -> None:
    """
    Use COPY for speed. We COPY into columns matching our row layout.
    """
    sio = StringIO()
    for (oid, gml_id, feature_type, aktualit_iso, nutzart, geom_wkt) in rows:
        sio.write(_copy_line([
            oid,
            gml_id or None,
            feature_type or None,
            aktualit_iso or None,
            nutzart or None,
            geom_wkt or None,
        ]))
    sio.seek(0)

    with conn.cursor() as cur:
        cur.copy_from(
            file=sio,
            table=table_name,
            columns=("oid", "gml_id", "feature_type", "aktualit", "nutzart", "geom_wkt"),
            null="\\N",
        )
    conn.commit()


def _usg_stream_import(xml_path: Path, conn, table_name: str, batch_size: int = 20000) -> int:
    """
    Stream-parse the XML and import rows in batches.
    Looks for </wfs:member> end events and processes contained feature.
    """
    count = 0
    batch: List[Tuple[str, Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]] = []

    # iterparse on huge files: only use 'end' events and clear elements
    context = iterparse(str(xml_path), events=("end",))
    for event, elem in context:
        if elem.tag == WFS_MEMBER_TAG:
            # member should contain exactly 1 feature element
            feature = next(iter(elem), None)
            if feature is not None:
                row = _usg_feature_to_row(feature)
                if row[0]:  # oid must be non-empty for primary key
                    batch.append(row)
                    count += 1

            # Important: free memory
            elem.clear()

            if len(batch) >= batch_size:
                _usg_copy_rows(conn, table_name, batch)
                batch.clear()

    if batch:
        _usg_copy_rows(conn, table_name, batch)
        batch.clear()

    return count


def run_usage_job(args, dbname: str) -> None:
    """
    JOB 3 entry point. Body is st/usage/fetch_usage.py main() with the argparse
    block lifted out to the shared CLI, the target database name passed in, and
    the url / batch size / postgis switch taken from config.ini.

    The original --postgis flag is now the [usage] st_postgis config key, which
    keeps this job's default of OFF.

    Note that only the DATABASE name is jobID-scoped. The destination TABLE is
    still named after the XML file inside the zip (gbis_tn), as before.
    """
    workdir = DOWNLOAD_DIR
    workdir.mkdir(parents=True, exist_ok=True)

    zip_name = Path(ST_USAGE_ZIP_URL).name           # GBIS_TN.zip
    zip_path = workdir / zip_name

    _step(f"[1/5] Downloading: {ST_USAGE_ZIP_URL}")
    _usg_download_file(ST_USAGE_ZIP_URL, zip_path)
    DOWNLOADED_ARCHIVES.append(zip_path)

    _step(f"[2/5] Unzipping: {zip_path}")
    xml_path = unzip_single_xml(zip_path, workdir)
    print(f"      Extracted XML: {xml_path}")

    table_name = sanitize_table_name(xml_path.stem)
    _step(f"[3/5] Table name: {table_name}")

    _step(f"[4/5] Ensuring database exists: {dbname}")
    # CREATE DATABASE ... TEMPLATE needs the TEMPLATE free of sessions.
    wait_for_no_connections(TEMPLATE_DB)
    _usg_ensure_database(dbname, PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, template=TEMPLATE_DB)

    with _usg_db(dbname, PG_HOST, PG_PORT, PG_USER, PG_PASSWORD) as conn:
        _usg_ensure_table(conn, table_name)

        _step(f"[5/5] Importing (stream-parse + COPY) ... this can take a while for multi-million rows.")
        inserted = _usg_stream_import(xml_path, conn, table_name, batch_size=ST_USAGE_BATCH_SIZE)
        print(f"      Inserted rows: {inserted}")

        if ST_USAGE_POSTGIS:
            enabled = _usg_try_enable_postgis(conn)
            if enabled:
                print("      PostGIS enabled. Creating geom column + GIST index from geom_wkt ...")
                _usg_add_postgis_geom(conn, table_name, srid=25832)
            else:
                print("      PostGIS could not be enabled (extension not available / insufficient rights). Keeping geom_wkt only.")

    _end_step()
    print("Done.")


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
    """--download: fetch all three jobs' sources, record the marker, and stop.

    Every source lands where the jobs themselves put it, so a later full run
    finds the files already there. Nothing is unzipped, nothing is imported and
    no database is touched -- so this needs no PostgreSQL.
    """
    # (url, the job's own downloader), in the order the jobs fetch them.
    sources = [
        (ST_GBIS_GEBAEUDE_ZIP_URL, _bld_download_file),
        (ST_GEBAEUDE_REF_ZIP_URL, _bld_download_file),
        (ST_PARCEL_ZIP_URL, _prc_download_file),
        (ST_USAGE_ZIP_URL, _usg_download_file),
    ]
    total = len(sources) + 1

    for num, (url, fetch) in enumerate(sources, 1):
        name = Path(url).name
        _step(f"[{num}/{total}] Download {name}")
        fetch(url, DOWNLOAD_DIR / name)

    _step(f"[{total}/{total}] Record update marker")
    record_marker(job_id)
    _end_step()


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Run all three Sachsen-Anhalt import jobs, then promote them to live.",
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
        for tool in ("pg_dump",):
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
        ("parcel",   2, run_parcel_job,   False),
        ("usage",    3, run_usage_job,    False),
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
