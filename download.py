#!/usr/bin/env python3
"""Download stage for every state's ALKIS NAS delivery.

The acquisition half: discover URLs, transfer archives, unpack them into
<inventory_dir>/<state>/<job>/. Writing the importer job file is deliberately NOT
here -- that half is near-identical across states and lives in make_nas.py.

When a state finishes, everything staged for it is bundled into
<backup_dir>/<state>/<job>/downloads.zip. Console output is mirrored to
<backup_dir>/<state>/<job>/logs.log.

Usage:
    python3 download.py --job-id=J nw       # one state, into job J
    python3 download.py --job-id=J nw he th # several
    python3 download.py nw                  # mints its own job id
    python3 download.py --list              # show known states

Paths come from config.ini. Without --job-id= a fresh id is minted, which will not
match an existing job -- pass one when working against a real run.
"""

import argparse
import csv
import gzip
import hashlib
import html
import json
import random
import re
import shutil
import subprocess
import sys
import tarfile
import time
import zipfile
import zlib
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import requests
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Paths come from config.ini. INVENTORY_DIR holds the unpacked data; STAGING_DIR
# holds in-flight archives and is kept outside it so that partial and leftover
# files can never be picked up by the .lst directory walk.
import state_db
from config import INVENTORY_DIR, STAGING_DIR, BACKUP_DIR
from logtee import tee_unless_parent

# Baden-Wuerttemberg's checked-in URL list, alongside this script.
SCRIPT_DIR = Path(__file__).resolve().parent

CHUNK = 1024 * 1024
TIMEOUT = 180
# Pause before each retry in process_urls(). A portal that cuts transfers usually
# does so for a minute or two, and retrying instantly just burns the attempts --
# see the IncompleteRead note in CLAUDE.md.
RETRY_DELAY = 30

# Set by --download: fetch archives into staging and stop there. The unpack
# helpers below become no-ops rather than every state's driver growing a branch,
# since unpacking is what all thirteen of them do with what they fetch.
DOWNLOAD_ONLY = False

TIME_FMT = '%Y-%m-%d %H:%M:%S'
JOB_FMT = '%Y%m%d_%H%M%S'

# Every path this run touches is scoped to a job id so concurrent or repeated runs
# never share a directory. main.py passes one in with --job-id=; run standalone, a
# fresh one is minted here.
JOB_ID = datetime.now().strftime(JOB_FMT)


def elapsed(start, end):
    """Whole-second wall clock between two datetimes, as H:MM:SS."""
    return timedelta(seconds=round((end - start).total_seconds()))


# The step being timed, as (label, started). step() closes the previous one, so a
# step's elapsed line lands after its own output rather than before the next step.
_current_step = None


def step(label):
    """Print a step heading, closing the previous step with its elapsed time."""
    global _current_step
    end_step()
    print(label)
    _current_step = (label, datetime.now())


def end_step():
    """Close the open step, if any. Called on failure too, so a step that dies
    still reports how long it ran."""
    global _current_step
    if _current_step is not None:
        label, started = _current_step
        _current_step = None
        print('      elapsed %s  (%s)' % (elapsed(started, datetime.now()), label))

# Schleswig-Holstein serves only through these; unauthenticated requests are refused.
PROXIES_FILE = SCRIPT_DIR / 'proxies.txt'
_proxies = None


# --------------------------------------------------------------------------
# shared transfer / unpack helpers
# --------------------------------------------------------------------------

def state_dir(state):
    """Unpacked data for this job: <inventory_dir>/<state>/<job id>/."""
    path = INVENTORY_DIR / state / JOB_ID
    path.mkdir(parents=True, exist_ok=True)
    return path


def staging_dir(state):
    """In-flight archives and .part files: <staging_dir>/<state>/<job id>/.

    Nothing here is deleted during a run -- archives survive extraction, and failed
    .part files are left for inspection. Whole superseded job folders are pruned at
    the end of the chain by main.py, per [cleanup]. Archives inside the *extracted*
    data are still removed, or the .lst directory walk would sweep them up.
    """
    path = STAGING_DIR / state / JOB_ID
    path.mkdir(parents=True, exist_ok=True)
    return path


def download(url, dest, verify=True, proxies=None, headers=None, stalls=100):
    """Stream url to dest via a .part file renamed into place on success.

    Resumes rather than restarting. Portals cut long transfers -- Brandenburg
    drops its 215 MB packages after a few MB at a time -- and starting over each
    time never finishes a large file. So the bytes already in the .part file are
    kept and the rest asked for with a Range header, exactly as a browser does.
    An interruption that made progress resumes immediately -- the bytes landed,
    so the server is plainly serving and there is nothing to wait for. One that
    arrives after the last byte ends the transfer rather than resuming it. Only an
    attempt that moves nothing counts against `stalls` and waits RETRY_DELAY;
    after `stalls` of those the transfer fails and process_urls() decides
    whether to come back to it.

    A server that ignores the Range and answers 200 restarts the file from zero,
    so a portal without resume support still works, just without the resuming.

    Only within one call: a .part left by an earlier run is not resumed, since
    the portal may have republished the package since, and the two halves would
    make a file that is neither. Mid-transfer that is covered by If-Range, which
    makes the server send the whole file again if it has changed.
    """
    dest = Path(dest)
    tmp = dest.with_suffix(dest.suffix + '.part')

    have = 0
    total = None
    validator = None
    stalled = 0

    while True:
        sent = dict(headers or {})
        if have:
            sent['Range'] = 'bytes=%d-' % have
            if validator:
                sent['If-Range'] = validator

        before = have
        try:
            with requests.get(url, stream=True, verify=verify, timeout=TIMEOUT,
                              proxies=proxies, headers=sent) as r:
                r.raise_for_status()

                # 206 means the server honoured the Range; anything else means it
                # sent the whole file again, so the .part has to go.
                resuming = have > 0 and r.status_code == 206
                if have and not resuming:
                    print('  %s: no resume, restarting from 0' % dest.name)
                    have = before = 0

                total = content_total(r, have)
                validator = r.headers.get('ETag') or r.headers.get('Last-Modified')
                with tmp.open('ab' if resuming else 'wb') as f:
                    for chunk in r.iter_content(chunk_size=CHUNK):
                        if chunk:
                            f.write(chunk)
                            have += len(chunk)
        except requests.RequestException as exc:
            # A 4xx is the server's answer, not a stall: retrying it here only
            # waits out `stalls` x RETRY_DELAY (~50 min) before the same error.
            # 408 and 429 are the exceptions -- those do mean "try again later".
            status = getattr(getattr(exc, 'response', None), 'status_code', None)
            if status is not None and 400 <= status < 500 and status not in (408, 429):
                raise
            have = tmp.stat().st_size if tmp.exists() else 0
            if total is not None and have >= total:
                # The cut came after the last byte: the file is whole, so there is
                # nothing to resume. Asking anyway earns a 416 and, before this
                # check, a long wait on a finished download.
                break
            if have > before:
                # The server cut us off, but bytes landed -- pick up where it stopped.
                print('  %s: interrupted at %d/%s bytes, resuming'
                      % (dest.name, have, total))
                stalled = 0
                continue
            stalled += 1
            if stalled >= stalls:
                raise
            print('  %s: no progress (%d/%d), retrying in %ds'
                  % (dest.name, stalled, stalls, RETRY_DELAY))
            time.sleep(RETRY_DELAY)
            continue

        if total is None or have >= total:
            break
        # The connection closed cleanly but short of the advertised length.
        if have == before:
            stalled += 1
            if stalled >= stalls:
                raise RuntimeError('incomplete download: got %d, expected %d' % (have, total))
            time.sleep(RETRY_DELAY)
        else:
            stalled = 0

    if have == 0:
        raise RuntimeError('downloaded 0 bytes')
    if total is not None and have != total:
        raise RuntimeError('incomplete download: got %d, expected %d' % (have, total))

    tmp.replace(dest)
    return dest


def content_total(response, have):
    """The full size of the file being fetched, or None if the server won't say.

    For a 206 that is the total after the slash in Content-Range; for a 200 it is
    Content-Length, which on a restart covers the whole file anyway.
    """
    if response.status_code == 206:
        rng = response.headers.get('Content-Range', '')
        tail = rng.rsplit('/', 1)[-1]
        return int(tail) if tail.isdigit() else None

    length = response.headers.get('Content-Length')
    return int(length) + have if length and length.isdigit() else None


def download_archive(state, url, filename=None):
    """Fetch an archive into the state's staging area."""
    name = filename or url.rsplit('/', 1)[1]
    return download(url, staging_dir(state) / name.replace(' ', '_'))


def verify_size(path, expected):
    """Check a finished download against the size its portal's catalogue advertises.

    Run between the transfer and the unpack. Stronger than the Content-Length check
    inside download(), which compares the bytes against a header from the very
    response that delivered them -- this compares against a figure published
    separately. Does nothing where the portal publishes no size, which is every
    state but NW.
    """
    if expected is None:
        return

    actual = Path(path).stat().st_size
    if actual != expected:
        raise RuntimeError('size mismatch for %s: got %d bytes, catalogue says %d'
                           % (Path(path).name, actual, expected))


def unzip(archive, target):
    target = Path(target)
    if DOWNLOAD_ONLY:
        return target
    target.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive, 'r') as zf:
        zf.extractall(target)
    # The staged archive is kept, not removed -- see staging_dir().
    return target


def unzip_by_name(state, archive):
    """Extract into <state>/<archive basename>. Used where source names are unique."""
    name = Path(archive).name.split('.', 1)[0]
    return unzip(archive, state_dir(state) / name)


def unzip_by_hash(state, archive):
    """Extract into <state>/<md5>. Used where source names collide across packages."""
    digest = hashlib.md5((state + str(archive)).encode('utf-8')).hexdigest()
    return unzip(archive, state_dir(state) / digest)


def extract_inner_zips(directory):
    """Unpack one level of zips shipped inside an outer archive."""
    if DOWNLOAD_ONLY:
        return
    for entry in Path(directory).iterdir():
        if entry.is_file() and zipfile.is_zipfile(entry):
            with zipfile.ZipFile(entry, 'r') as zf:
                zf.extractall(directory)
            entry.unlink()


def gunzip_xml(directory):
    """Decompress every *.xml.gz under directory, removing the .gz.

    Run over the unpacked data after each state, so the .lst lists plain XML only.
    mv and rp ship .xml.gz inside their packages and alkis-import would read those
    compressed, but leaving them gzipped puts their contents beyond the
    well-formedness check main.py runs before the import.

    Streamed, not read whole: a single NAS file can be hundreds of megabytes.
    """
    done = 0
    for path in sorted(Path(directory).rglob('*.xml.gz')):
        target = path.with_suffix('')          # drops only .gz
        with gzip.open(path, 'rb') as fin, open(target, 'wb') as fout:
            shutil.copyfileobj(fin, fout, length=CHUNK)
        path.unlink()
        done += 1

    if done:
        print('gunzipped %d .xml.gz file(s)' % done)
    return done


def extract_nested_7z(directory, delete_archives=True):
    """Recursively unpack .7z (requires the 7z binary from p7zip-full)."""
    if DOWNLOAD_ONLY:
        return
    root = Path(directory)
    while True:
        archives = list(root.rglob('*.7z'))
        if not archives:
            return
        for archive in archives:
            subprocess.run(['7z', 'x', str(archive), '-o%s' % archive.parent, '-y'],
                           check=True, stdout=subprocess.DEVNULL,
                           stderr=subprocess.STDOUT)
            if delete_archives:
                archive.unlink(missing_ok=True)


def archive_downloads(state):
    """Bundle this job's staged files into <backup_dir>/<state>/<job>/downloads.zip.

    Written via a .part file and renamed, so an interrupted run never leaves a
    half-written archive that looks complete. The staged originals stay where they
    are; this is a copy, not a move.
    """
    source = STAGING_DIR / state / JOB_ID
    if not source.is_dir():
        return None

    files = sorted(p for p in source.rglob('*') if p.is_file())
    if not files:
        print('nothing staged to archive')
        return None

    target = BACKUP_DIR / state / JOB_ID / 'downloads.zip'
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix('.zip.part')

    # ZIP_STORED, not ZIP_DEFLATED: nearly everything staged is already a compressed
    # archive, so deflating again burns a lot of CPU on multi-GB input for no saving.
    with zipfile.ZipFile(tmp, 'w', zipfile.ZIP_STORED, allowZip64=True) as zf:
        for path in files:
            zf.write(path, path.relative_to(source))

    tmp.replace(target)
    print('archived %d file(s) -> %s (%.1f MiB)'
          % (len(files), target, target.stat().st_size / (1024 * 1024)))
    return target


def process_urls(urls, handler, attempts=5, delay=RETRY_DELAY):
    """Run handler(url) over urls, strictly one at a time.

    A url is finished before the next one starts: a failure is retried in place,
    after a pause, rather than queued behind every other url. So a package is
    either in the staging directory or given up on by the time the next transfer
    begins, and the log reads in file order.

    The pause matters as much as the retry. A portal that drops a transfer keeps
    dropping them for a minute or two -- Brandenburg cut all 18 packages inside
    three seconds -- so retrying instantly spends every attempt inside the same
    blip. It is the same `delay` before every attempt, not a growing backoff, so
    the wait stays predictable: five attempts span two minutes. Attempts are
    bounded; a permanent 404 costs `attempts` tries and is reported, never
    retried forever. If any url is given up on, RuntimeError is raised once
    the rest have been tried.
    """
    failed = []

    for url in urls:
        for attempt in range(1, attempts + 1):
            try:
                handler(url)
                break
            except Exception as exc:
                print('FAIL (%d/%d) %s: %s' % (attempt, attempts, url, exc))
                if attempt == attempts:
                    failed.append(url)
                    break
                print('  retrying in %ds' % delay)
                time.sleep(delay)

    # Raised, not returned: a state missing a package must not reach the importer,
    # which would make the partial set live. Every url is still tried first, so
    # the log names all the failures at once.
    if failed:
        print('%d url(s) gave up after %d attempts:' % (len(failed), attempts))
        for url in failed:
            print('  %s' % url)
        raise RuntimeError('%d of %d url(s) failed to download'
                           % (len(failed), len(urls)))
    return failed


def load_proxies():
    """Proxy URLs from proxies.txt, one `host:port:user:pass` per line.

    Credentials are per proxy, not shared. Blank lines and # comments are ignored,
    and only the first three colons are split on, so a password may contain one.

    Read on first use and cached, not at import: SH is the only state that needs
    proxies, and main.py imports this module purely for its STATES table.
    """
    global _proxies
    if _proxies is not None:
        return _proxies

    if not PROXIES_FILE.exists():
        raise FileNotFoundError('%s is needed for the sh download' % PROXIES_FILE)

    urls = []
    for number, raw in enumerate(PROXIES_FILE.read_text().splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith('#'):
            continue

        parts = line.split(':', 3)
        if len(parts) != 4:
            raise ValueError('%s line %d: expected host:port:user:pass, got %r'
                             % (PROXIES_FILE, number, raw))

        host, port, user, password = parts
        urls.append('http://%s:%s@%s:%s' % (user, password, host, port))

    if not urls:
        raise ValueError('no proxies listed in %s' % PROXIES_FILE)

    _proxies = urls
    return _proxies


def get_proxy():
    url = random.choice(load_proxies())
    return {'http': url, 'https': url}


# --------------------------------------------------------------------------
# Brandenburg -- HTML directory index
# --------------------------------------------------------------------------

BB_URL = 'https://data.geobasis-bb.de/geobasis/daten/alkis/Vektordaten/nas/'


def bb_links():
    r = requests.get(BB_URL, timeout=TIMEOUT)
    r.raise_for_status()
    soup = BeautifulSoup(r.content, 'html.parser')
    return [BB_URL + ele.find_next('a')['href']
            for ele in soup.find_all('img', {'src': '/style/icons/file-zip.svg'})]


def bb():
    def handle(url):
        unzip_by_name('bb', download_archive('bb', url))
    process_urls(bb_links(), handle)


# --------------------------------------------------------------------------
# Berlin -- no network stage; the delivery arrives as --be-update-path
# --------------------------------------------------------------------------

BE_OLD = 'lebenszeitintervall/AA_Lebenszeitintervall/endet'
BE_NEW = 'lebenszeitintervall/AA_Lebenszeitintervall/adv:endet'


def extract_delivery(archive, target):
    """Unpack a Berlin delivery archive into target.

    Format is decided by content, not by the name: a delivery may arrive as .zip,
    .7z, .tar.gz or a single .gz, and the extension is not always honest about
    which. Returns the number of files that landed.
    """
    archive, target = Path(archive), Path(target)
    target.mkdir(parents=True, exist_ok=True)

    if zipfile.is_zipfile(archive):
        kind = 'zip'
        with zipfile.ZipFile(archive) as zf:
            zf.extractall(target)
    elif tarfile.is_tarfile(archive):
        # Covers .tar, .tar.gz and .tgz; tarfile picks the compression itself.
        kind = 'tar'
        with tarfile.open(archive) as tf:
            tf.extractall(target)
    elif archive.suffix.lower() == '.7z':
        kind = '7z'
        subprocess.run(['7z', 'x', str(archive), '-o%s' % target, '-y'],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    elif archive.suffix.lower() == '.gz':
        # A bare .gz is not a container -- it IS one delivery file. Copy it across
        # untouched so be() gunzips it with the adv: namespace fix, which
        # decompressing here would skip.
        kind = 'gzip'
        shutil.copy2(archive, target / archive.name)
    else:
        raise ValueError('%s is not a zip, 7z, tar or gzip archive' % archive)

    landed = sum(1 for p in target.rglob('*') if p.is_file())
    print('extracted %s (%s) -> %s, %d file(s)' % (archive.name, kind, target, landed))
    return landed


def be():
    """Gunzip a delivery into the job's data directory, repairing the adv: namespace.

    Reads *.xml.gz from this job's staging directory, where main.py has already
    unpacked the archive named by --be-update-path. Recursive, because an archive
    may or may not wrap its contents in a top-level folder.
    """
    source = staging_dir('be')
    target = state_dir('be')

    archives = sorted(source.rglob('*.xml.gz'))
    if not archives:
        raise FileNotFoundError(
            'no .xml.gz under %s -- main.py unpacks --be-update-path there, so pass '
            'that argument, or unpack a delivery yourself to drive this stage by hand'
            % source)

    extracted = []
    for archive in archives:
        xml_path = target / archive.with_suffix('').name
        with gzip.open(archive, 'rt', encoding='utf-8') as fin:
            content = fin.read()
        with open(xml_path, 'w', encoding='utf-8') as fout:
            fout.write(content.replace(BE_OLD, BE_NEW))
        extracted.append(xml_path.resolve())

    print('gunzipped %d delivery file(s) to %s' % (len(extracted), target))
    return extracted


# Baden-Wuerttemberg -- checked-in URL list, one archive per municipality
# --------------------------------------------------------------------------

def bw_links():
    with open(SCRIPT_DIR / 'bw_nas_alkis.csv', 'r') as f:
        return [row[0] for row in csv.reader(f) if row]


def bw():
    urls = bw_links()
    done = [0]

    def handle(url):
        # 3,380 packages take hours; naming each as it starts is what makes the
        # log followable and says where a run stopped.
        done[0] += 1
        print('  [%4d/%d] %s' % (done[0], len(urls), url.rsplit('/', 1)[1]))
        try:
            archive = download_archive('bw', url)
        except requests.HTTPError as exc:
            # Some municipality URLs simply do not exist; not worth retrying.
            if exc.response is not None and exc.response.status_code == 404:
                print('SKIP 404 %s' % url)
                return
            raise
        try:
            unzip_by_name('bw', archive)
        except zipfile.BadZipFile:
            # Portal answers with an HTML error page for a few municipalities.
            # The response is kept in staging so the body can be inspected.
            print('SKIP not-a-zip %s' % url)

    process_urls(urls, handle)


# --------------------------------------------------------------------------
# Bremen -- two fixed packages, nested .7z inside
# --------------------------------------------------------------------------

HB_URLS = [
    'https://gdi2.geo.bremen.de/inspire/download/ALKIS_o_E_o_KS/data/ALKIS_OE_OKS_NAS_HB.zip',
    'https://gdi2.geo.bremen.de/inspire/download/ALKIS_o_E_o_KS/data/ALKIS_OE_OKS_NAS_BHV.zip',
]


def hb():
    def handle(url):
        target = unzip_by_hash('hb', download_archive('hb', url))
        extract_nested_7z(target)
    process_urls(HB_URLS, handle)


# --------------------------------------------------------------------------
# Hessen -- Intershop REST API, two hops per package
# --------------------------------------------------------------------------

HE_BASE = 'https://gds.hessen.de/'
HE_INDEX = (HE_BASE + 'INTERSHOP/rest/WFS/HLBG-Geodaten-Site/-/downloadcenter'
            '?path=Liegenschaftskataster%2FBestandsdatenausgabe+Grundriss+%28xml%29')


def he_links():
    r = requests.get(HE_INDEX, timeout=TIMEOUT)
    r.raise_for_status()

    urls = []
    for node in r.json()['navigation']:
        detail = requests.get(HE_BASE + node['uri'], timeout=TIMEOUT)
        detail.raise_for_status()
        packages = detail.json()['searchresult']['packages']
        if not packages:
            print('no package for %s' % node.get('name', node['uri']))
            continue
        urls.append(HE_BASE + packages[0]['downloadLink']['uri'])
    return urls


def he():
    def handle(url):
        target = unzip_by_hash('he', download_archive('he', url))
        extract_inner_zips(target)
    process_urls(he_links(), handle)


# --------------------------------------------------------------------------
# Hamburg -- transparency portal, dated packages, newest only
# --------------------------------------------------------------------------

HH_URL = 'https://suche.transparenz.hamburg.de/dataset/alkis-ausgewaehlte-daten-hamburg5'


def hh_links():
    r = requests.get(HH_URL, timeout=TIMEOUT)
    r.raise_for_status()
    soup = BeautifulSoup(r.content, 'html.parser')

    dated = []
    for ele in soup.find_all('a', href=True):
        href = ele['href']
        low = href.lower()
        if not low.endswith('.zip') or low.endswith('_jpg.zip'):
            continue
        try:
            stamp = datetime.strptime(href.rsplit('.', 1)[0].rsplit('_', 1)[1], '%Y-%m-%d')
        except (ValueError, IndexError):
            continue
        dated.append((stamp, href))

    # Sort explicitly rather than trusting the page's link order.
    dated.sort()
    return [href for _, href in dated]


def hh(newest_only=True):
    urls = hh_links()
    if newest_only:
        urls = urls[-1:]

    def handle(url):
        unzip_by_name('hh', download_archive('hh', url))
    process_urls(urls, handle)


# --------------------------------------------------------------------------
# Mecklenburg-Vorpommern -- Atom feed
# --------------------------------------------------------------------------

MV_URL = ('https://www.geodaten-mv.de/dienste/alkis_nas_atom'
          '?type=dataset&id=32538df8-6b74-4582-8591-c77e85fbf929')


def mv_links():
    r = requests.get(MV_URL, timeout=TIMEOUT)
    r.raise_for_status()

    root = ET.fromstring(r.text)
    ns = {'atom': 'http://www.w3.org/2005/Atom'}

    urls = []
    for link in root.findall(".//atom:link[@rel='section']", ns):
        href = link.get('href')
        if not href:
            continue
        href = html.unescape(href)
        filename = parse_qs(urlparse(href).query).get('file', [''])[0]
        if '_NAS_' in filename:
            urls.append(href)
    return urls


def mv():
    def handle(url):
        filename = parse_qs(urlparse(url).query).get('file', [''])[0]
        archive = download(url, staging_dir('mv') / filename.replace(' ', '_'),
                           verify=False)
        target = unzip_by_name('mv', archive)
        extract_inner_zips(target)
    process_urls(mv_links(), handle)


# --------------------------------------------------------------------------
# Nordrhein-Westfalen -- JSON index
# --------------------------------------------------------------------------

NW_URL = 'https://www.opengeodata.nrw.de/produkte/geobasis/lk/akt/gru_xml/'


def nw_catalogue():
    """(urls, {url: expected bytes}) from index.json.

    NW is the only portal that publishes an exact byte count per file, so it is the
    only state whose downloads can be checked against something other than the
    Content-Length of the response that delivered them. Hessen advertises a
    `fileSize`, but rounded and human-readable ('154,7 MB'), which with the MB/MiB
    ambiguity is far too loose to assert on.
    """
    r = requests.get(NW_URL + 'index.json', timeout=TIMEOUT)
    r.raise_for_status()

    urls, sizes = [], {}
    for dataset in r.json()['datasets']:
        for entry in dataset['files']:
            url = NW_URL + entry['name']
            urls.append(url)
            if str(entry.get('size', '')).isdigit():
                sizes[url] = int(entry['size'])
    return urls, sizes


def nw_links():
    return nw_catalogue()[0]


def nw():
    urls, sizes = nw_catalogue()

    def handle(url):
        archive = download_archive('nw', url)
        verify_size(archive, sizes.get(url))
        unzip_by_name('nw', archive)

    process_urls(urls, handle)


# --------------------------------------------------------------------------
# Rheinland-Pfalz -- HTML index, plus the BasisDLM package
# --------------------------------------------------------------------------

RP_URL = 'https://geobasis-rlp.de/data/bda/current/xml/'
RP_BASISDLM = 'https://geobasis-rlp.de/data/basisdlm/current/xml/BasisDLM_RP_nas.zip'


def rp_links():
    r = requests.get(RP_URL, verify=False, timeout=TIMEOUT)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, 'html.parser')

    urls = [RP_URL + tag['href']
            for tag in soup.find_all('a', href=True)
            if tag['href'].endswith('.zip')]
    urls.append(RP_BASISDLM)
    return urls


def rp():
    def handle(url):
        archive = download(url, staging_dir('rp') / url.rsplit('/', 1)[1].replace(' ', '_'),
                           verify=False)
        target = unzip_by_name('rp', archive)
        extract_inner_zips(target)
    process_urls(rp_links(), handle)


# --------------------------------------------------------------------------
# Schleswig-Holstein -- GeoJSON manifest, proxied, gzip decoded in process
# --------------------------------------------------------------------------

SH_GEOJSON_URL = ('https://geodaten.schleswig-holstein.de/gaialight-sh/_apps/dladownload/'
                  'single.php?file=ALKIS_SH_Massendownload.geojson&id=4')
SH_GEOJSON_NAME = 'ALKIS_SH_Massendownload.geojson'


def sh_manifest(refresh=False):
    """Fetch the manifest. Kept out of <state>/ so it never lands in the .lst."""
    path = staging_dir('sh') / SH_GEOJSON_NAME
    if refresh or not path.exists():
        r = requests.get(SH_GEOJSON_URL, verify=False, timeout=TIMEOUT)
        r.raise_for_status()
        path.write_text(r.text)

    with open(path) as f:
        return json.load(f)


def sh_targets(manifest):
    """(url, final .xml path) for every Flur in the manifest."""
    targets = []
    for feature in manifest.get('features', []):
        props = feature.get('properties', {})
        flur = props.get('FLUR') or props.get('flur')
        link = props.get('LINK_DATA') or props.get('link_data')
        if not flur or not link:
            continue
        filename = parse_qs(urlparse(link).query)['file'][0]
        targets.append((link, state_dir('sh') / filename.removesuffix('.gz')))
    return targets


def sh_download(url, xml_final):
    """Fetch one gzipped NAS file and write the decompressed xml atomically."""
    if Path(xml_final).exists():
        return False

    gz_tmp = staging_dir('sh') / (Path(xml_final).name + '.gz')
    download(url, gz_tmp, verify=False, proxies=get_proxy(),
             headers={'Accept-Encoding': 'identity'})

    compressed = gz_tmp.read_bytes()
    if len(compressed) < 100:
        raise RuntimeError('suspiciously small response: %d bytes' % len(compressed))

    xml_tmp = gz_tmp.with_suffix('.xml.part')
    xml_tmp.write_bytes(zlib.decompress(compressed, 16 + zlib.MAX_WBITS))
    # Moves the decompressed xml out of staging; the .gz stays behind.
    xml_tmp.replace(xml_final)
    return True


def sh(refresh_manifest=False):
    manifest = sh_manifest(refresh=refresh_manifest)
    targets = sh_targets(manifest)
    print('manifest lists %d file(s)' % len(targets))

    by_url = dict(targets)

    def handle(url):
        sh_download(url, by_url[url])

    process_urls([url for url, _ in targets], handle)

    missing = [url for url, path in targets if not Path(path).exists()]
    if missing:
        raise SystemExit('%d of %d SH file(s) still missing after retries'
                         % (len(missing), len(targets)))
    print('all %d SH file(s) present' % len(targets))


# --------------------------------------------------------------------------
# Saarland -- Nextcloud share token resolved from a redirect
# --------------------------------------------------------------------------

SL_URL = 'https://www.shop.lvgl.saarland.de/cloud/freiegeobasisdaten'
SL_TOKEN_RE = re.compile(r'/cloud/index\.php/s/([A-Za-z0-9]+)')
SL_PACKAGES = [
    'ALKIS_NAS_41_Regionalverband_Saarbr%C3%BCcken.zip',
    'ALKIS_NAS_42_Merzig-Wadern.zip',
    'ALKIS_NAS_43_Neunkirchen.zip',
    'ALKIS_NAS_44_Saarlouis.zip',
    'ALKIS_NAS_45_Saarpfalz-Kreis.zip',
    'ALKIS_NAS_46_St.Wendel.zip',
    'ALKIS_NAS_49_Kreisfreies_Gebiet.zip',
]


def sl_token():
    r = requests.get(SL_URL, timeout=TIMEOUT, allow_redirects=False,
                     headers={'User-Agent': 'Mozilla/5.0'})
    if r.status_code not in (301, 302, 303, 307, 308):
        raise RuntimeError('expected a redirect, got HTTP %d' % r.status_code)

    location = r.headers.get('Location')
    if not location:
        raise RuntimeError('redirect carried no Location header')

    match = SL_TOKEN_RE.search(location)
    if not match:
        raise RuntimeError('no share token in Location: %s' % location)
    return match.group(1)


def sl_links():
    token = sl_token()
    return ['https://www.shop.lvgl.saarland.de/cloud/public.php/dav/files/'
            '%s/OD_ALKIS_nas_LK/%s' % (token, name) for name in SL_PACKAGES]


def sl():
    def handle(url):
        unzip_by_hash('sl', download_archive('sl', url))
    process_urls(sl_links(), handle)


# --------------------------------------------------------------------------
# Sachsen -- share id scraped out of an inline JS config
# --------------------------------------------------------------------------

SN_URL = 'https://www.geodaten.sachsen.de/batch-download-4719.html'


def sn_config(text, name):
    """Read one `batchConfig.<name>={...}` object out of the page's inline JS.

    The assignments are not semicolon-terminated, so the object is parsed with
    raw_decode, which consumes exactly one JSON value and ignores whatever follows.
    """
    match = re.search(r'batchConfig\.%s\s*=\s*' % name, text)
    if not match:
        raise RuntimeError('batchConfig.%s not found on %s' % (name, SN_URL))
    value, _ = json.JSONDecoder().raw_decode(text, match.end())
    return value


def sn_links():
    r = requests.get(SN_URL, timeout=TIMEOUT)
    r.raise_for_status()

    mapping = sn_config(r.text, 'mapping')
    products = sn_config(r.text, 'products')

    share_id = products['ALKIS_Gem']['share_id']
    return ['https://geocloud.landesvermessung.sachsen.de/public.php/dav/files/'
            '%s/%s.xml.zip' % (share_id, key) for key in mapping]


def sn():
    def handle(url):
        target = unzip_by_hash('sn', download_archive('sn', url))
        extract_inner_zips(target)
    process_urls(sn_links(), handle)


# --------------------------------------------------------------------------
# Thueringen -- Atom service document
# --------------------------------------------------------------------------

TH_URL = 'https://geoportal.geoportal-th.de/dienste/atom_th_alkis?type=dataset'
TH_TITLE_TERMS = ('ALKIS im NAS Format', 'zusammengefasst', 'EPSG:25832')


def th_links():
    r = requests.get(TH_URL, timeout=TIMEOUT)
    r.raise_for_status()
    soup = BeautifulSoup(r.content, 'xml')

    urls = []
    for title in soup.find_all('title'):
        text = title.get_text(strip=True)
        if not all(term in text for term in TH_TITLE_TERMS):
            continue
        entry = title.find_parent('entry')
        if entry is None:
            continue
        for link in entry.find_all('link', rel='section'):
            href = link.get('href')
            if href:
                urls.append(href)
    return urls


def th():
    def handle(url):
        target = unzip_by_name('th', download_archive('th', url))
        extract_inner_zips(target)
    process_urls(th_links(), handle)


# --------------------------------------------------------------------------

STATES = {
    'bb': bb,   # Brandenburg
    'be': be,   # Berlin           (local staging, no network)
    'bw': bw,   # Baden-Wuerttemberg
    'hb': hb,   # Bremen
    'he': he,   # Hessen
    'hh': hh,   # Hamburg
    'mv': mv,   # Mecklenburg-Vorpommern
    'nw': nw,   # Nordrhein-Westfalen
    'rp': rp,   # Rheinland-Pfalz
    'sh': sh,   # Schleswig-Holstein
    'sl': sl,   # Saarland
    'sn': sn,   # Sachsen
    'th': th,   # Thueringen
}
# Not covered by any source: by (Bayern), ni (Niedersachsen), st (Sachsen-Anhalt).


def state_arg(known):
    """An argparse `type` that accepts one of `known`, for a list of states.

    Not `choices=`: on Python 3.10 argparse validates a nargs='*' positional's
    empty default against choices too, so "no states given" fails as
    "invalid choice: []" instead of printing the help. `type` is applied to each
    argument string as it is read, so the empty case never reaches it. The
    message is argparse's own, wrapped as "argument STATE: ...".
    """
    def check(value):
        if value not in known:
            raise argparse.ArgumentTypeError(
                'invalid choice: %r (choose from %s)'
                % (value, ', '.join(known)))
        return value
    return check


def build_parser():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    # state_arg does the validation: an unknown abbreviation is rejected with the
    # full list, so no hand-rolled check is needed.
    parser.add_argument('states', nargs='*', type=state_arg(sorted(STATES)),
                        metavar='STATE', help='state abbreviations to download')
    parser.add_argument('--job-id', metavar='ID',
                        help='job id to write under (default: minted from the clock)')
    parser.add_argument('--download', dest='download_only', action='store_true',
                        help='fetch archives into staging only: no unpacking '
                             'and no downloads.zip')
    parser.add_argument('--list', action='store_true',
                        help='print the known state abbreviations and exit')
    return parser


def record_update_marker(state):
    """Store what this run downloaded as the state's last seen marker.

    Built by check_update.RECORDERS from the staged archives where that is
    cheaper than asking the portal again -- bw hashes the ten samples off disk
    rather than re-fetching 541 MB. A state with no check recorded nothing, and
    a failure here is a warning: the data is already on disk, and a marker is
    bookkeeping, not data.
    """
    import check_update   # here, not at module scope: check_update imports this module

    build = check_update.RECORDERS.get(state)
    if build is None:
        return

    try:
        marker = build(staging_dir(state))
    except Exception as exc:
        print('WARNING: update marker not recorded: %s' % exc)
        return

    # One row for this state and job; every other state's marker is left as it is.
    state_db.record_marker(state, JOB_ID, marker)
    print('recorded %d file(s) in %s.update_state' % (len(marker), state_db.STATE_DB))


def main(argv):
    global JOB_ID

    parser = build_parser()
    args = parser.parse_args(argv)

    if args.list:
        for abbr in sorted(STATES):
            print(abbr)
        return 0

    if not args.states:
        parser.print_help()
        return 0

    # An empty --job-id= would collapse every path to <root>/<state>/, so only a
    # non-empty value replaces the minted default.
    if args.job_id:
        JOB_ID = args.job_id

    global DOWNLOAD_ONLY
    DOWNLOAD_ONLY = args.download_only

    print('job     %s' % JOB_ID)
    if DOWNLOAD_ONLY:
        print('mode    --download: staging only, nothing unpacked or archived')
    overall = datetime.now()
    for abbr in args.states:
        started = datetime.now()
        log = BACKUP_DIR / abbr / JOB_ID / 'logs.log'
        print('log     %s' % log)
        with tee_unless_parent(log):
            print('=' * 20, abbr)
            print('start   %s' % started.strftime(TIME_FMT))
            # --download drops the two steps that work on unpacked data.
            last = 2 if DOWNLOAD_ONLY else 4
            try:
                step('[1/%d] discover and download packages' % last)
                STATES[abbr]()
                if not DOWNLOAD_ONLY:
                    # The .lst must list plain XML, so nothing stays gzipped in the data.
                    step('[2/4] gunzip any *.xml.gz')
                    gunzip_xml(state_dir(abbr))
                    # Only on success -- a failed state's staging is incomplete.
                    step('[3/4] archive staged files into downloads.zip')
                    archive_downloads(abbr)
                # Likewise only on success: check_update.py then reports this state
                # as up to date until the portal moves again.
                step('[%d/%d] record update marker' % (last, last))
                record_update_marker(abbr)
            finally:
                # Reported even when a state fails, so a partial run stays measurable.
                end_step()
                finished = datetime.now()
                print('end     %s' % finished.strftime(TIME_FMT))
                print('elapsed %s' % elapsed(started, finished))

    if len(args.states) > 1:
        print('=' * 20, 'total')
        print('elapsed %s' % elapsed(overall, datetime.now()))
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
