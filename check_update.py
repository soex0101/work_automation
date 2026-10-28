#!/usr/bin/env python3
"""Report whether a state's portal has data newer than what we last recorded.

Each state has a `<state>_marker()` that asks its portal one cheap question --
a publication date, a per-file timestamp, a hash -- and returns the answer as a
string, or as a dict of one per file. The marker is compared with the state's
newest one in the alkis_scraper database's update_state table (state_db.py); if
they differ, the state needs an update.
What a marker means is per state and is never interpreted here, only compared,
so anything stable, comparable and JSON-serialisable works.

    python3 check_update.py nw          # one state
    python3 check_update.py nw he th    # several
    python3 check_update.py --all       # every state with a check implemented
    python3 check_update.py --job-id=J nw   # name this run's log check_update_J.log

Every run mirrors its whole console output into logs/check_update_<job>.log
beside this script, one file per run, named by --job-id or by the clock. The per-state lines
a check prints -- which package differs, how many megabytes it read -- are the
record of what the portals actually served that night.

The run ends with a summary, which cron.py parses -- keep its labels stable:

    update needed: bb nw
    up to date: he hh mv
    check failed: sh

It only reports; nothing here writes update_state. That table holds one marker
per state and job, written by whatever actually downloaded the data --
download.py, or automation_base_by/st.py -- so a state keeps reporting an update
until the data has really been fetched. With no row for a state, that state
needs an update and the check is skipped rather than run for a foregone answer.
If the database cannot be read the run fails before checking anything, and
prints no summary, so cron.py starts nothing.
"""

import argparse
import time
import hashlib
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests
import xml.etree.ElementTree as ET
from urllib.parse import unquote
from bs4 import BeautifulSoup

import config     # for the Bayern / Sachsen-Anhalt source urls
import download   # for the list of known state abbreviations, and its portal URLs
import state_db
from logtee import tee_output

SCRIPT_DIR = Path(__file__).resolve().parent

TIME_FMT = '%Y-%m-%d %H:%M:%S'
JOB_FMT = '%Y%m%d_%H%M%S'
# One file per run, beside this script, as check_update_<job>.log -- named so it
# sits unambiguously beside anything else a job writes here. A check is cheap to
# repeat but not free -- bw hashes 541 MB, by 6 GB -- so what it saw is worth
# keeping.
LOG_DIR = SCRIPT_DIR / 'logs'
TIMEOUT = 60


class Changed(Exception):
    """A check has seen enough: the state needs an update, stop looking.

    Raised by a check that costs real bandwidth -- bw downloads its samples --
    so the remaining files are never fetched. The marker is then incomplete by
    design, which is why nothing is recorded for a state that raises it.
    """


# ============================================================================
# PER-STATE CHECKS
#
# One function per state, returning the portal's current marker as a string.
# Each hits only an index page or a HEAD request -- never a download -- so a
# check stays cheap enough to run from cron. download.py's <state>_links() are
# available for the states whose marker is derived from the URL list.
# ============================================================================

def bb_marker(last_seen=None):
    """Brandenburg: the Last modified column of its Apache index, per zip.

    Same page and same rows as download.bb_links(), which takes the hrefs; this
    takes the timestamp beside each one, so a republished package shows up even
    though the file list has not changed.
    """
    r = requests.get(download.BB_URL, timeout=TIMEOUT)
    r.raise_for_status()
    soup = BeautifulSoup(r.content, 'html.parser')

    marker = {}
    for icon in soup.find_all('img', {'src': '/style/icons/file-zip.svg'}):
        row = icon.find_parent('tr')
        name = row.find('a')['href']
        cell = row.find('td', {'class': 'indexcollastmod'})
        if cell is None:
            raise RuntimeError('bb: no Last modified cell for %s' % name)
        marker[name] = cell.get_text(strip=True)

    if not marker:
        raise RuntimeError('bb: no zip rows found at %s -- page layout changed?'
                           % download.BB_URL)
    return marker


# Baden-Wuerttemberg publishes no index at all -- bw_nas_alkis.csv is a checked-in
# list of 3,380 package urls -- so there is nothing to read a date off. These ten
# cities stand in for the whole state: they are refreshed in the same pass as the
# rest, so a change in them means a change everywhere.
BW_SAMPLE = [
    'https://opengeodata.lgl-bw.de/data/alkis/ALKIS-oE_083310_Mannheim_nas.zip',
    'https://opengeodata.lgl-bw.de/data/alkis/ALKIS-oE_081460_Stuttgart_nas.zip',
    'https://opengeodata.lgl-bw.de/data/alkis/ALKIS-oE_083620_Karlsruhe_nas.zip',
    'https://opengeodata.lgl-bw.de/data/alkis/ALKIS-oE_083320_Heidelberg_nas.zip',
    'https://opengeodata.lgl-bw.de/data/alkis/ALKIS-oE_081700_Esslingen_nas.zip',
    'https://opengeodata.lgl-bw.de/data/alkis/ALKIS-oE_085710_Freiburg_nas.zip',
    'https://opengeodata.lgl-bw.de/data/alkis/ALKIS-oE_087640_Reutlingen_nas.zip',
    'https://opengeodata.lgl-bw.de/data/alkis/ALKIS-oE_088540_Ulm_nas.zip',
    'https://opengeodata.lgl-bw.de/data/alkis/ALKIS-oE_083621_Durlach_nas.zip',
    'https://opengeodata.lgl-bw.de/data/alkis/ALKIS-oE_084070_Pforzheim_nas.zip',
]


def hash_urls(urls, last_seen=None, verify=True):
    """SHA-256 of each url's content, one at a time.

    Stops at the first package whose hash differs from `last_seen` and raises
    Changed rather than fetching the rest: one difference already answers the
    question, and these packages are large. Each is hashed as it streams and
    never written to disk, which keeps the check off a full download volume.
    """
    marker = {}
    for url in urls:
        name = url.rsplit('/', 1)[1]
        digest = hashlib.sha256()
        size = 0
        with requests.get(url, stream=True, timeout=TIMEOUT, verify=verify) as r:
            r.raise_for_status()
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                digest.update(chunk)
                size += len(chunk)
        digest = digest.hexdigest()
        print('  %-42s %8.1f MB  %s' % (name, size / 1e6, digest[:16]))
        marker[name] = digest

        if last_seen and last_seen.get(name) != digest:
            raise Changed('%s differs (%s... -> %s...), stopped after %d of %d'
                          % (name, (last_seen.get(name) or 'never seen')[:12],
                             digest[:12], len(marker), len(urls)))

    return marker


def hash_staged(urls, staged):
    """The same hashes, taken from what the download left in staging.

    What download.py records: the bytes are already on disk, so this costs no
    bandwidth where asking the portal again would cost the whole set. A package
    the portal did not serve is left out rather than guessed at.
    """
    staged = Path(staged)
    marker = {}
    for url in urls:
        name = url.rsplit('/', 1)[1]
        path = staged / name
        if not path.is_file():
            # BY/ST unpack into subdirectories of their download folder, so look
            # deeper before giving up on a file.
            path = next(staged.rglob(name), None)
        if path is None or not path.is_file():
            print('  %s: not in staging, left out of the marker' % name)
            continue

        digest = hashlib.sha256()
        with path.open('rb') as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b''):
                digest.update(block)
        marker[name] = digest.hexdigest()

    if not marker:
        raise RuntimeError('none of the %d package(s) are in %s' % (len(urls), staged))
    return marker


def bw_marker(last_seen=None):
    """Baden-Wuerttemberg: the SHA-256 of ten sample packages, 541 MB for the set.

    The portal publishes no index, so there is nothing to read a date off -- see
    BW_SAMPLE above. The expensive check of the two.
    """
    return hash_urls(BW_SAMPLE, last_seen)


# Bremen ships the whole state as two packages, so the check hashes exactly what
# the download fetches -- no sampling involved. 540 MB for the pair, and the
# smaller one is checked first only because HB_URLS lists it that way.
def hb_marker(last_seen=None):
    """Bremen: the SHA-256 of both packages download.py fetches."""
    return hash_urls(download.HB_URLS, last_seen)


# ============================================================================
# RECORDERS
#
# What download.py stores once a state has been fetched. Same shape as that
# state's check, so the next check compares like with like -- but built from
# what the download left behind wherever that is cheaper than asking the portal
# again. Each takes the staging directory holding this job's archives.
# ============================================================================

def bb_recorder(staged):
    """Brandenburg: re-read the index. It is one 11 KB page."""
    return bb_marker()


def bw_recorder(staged):
    """Baden-Wuerttemberg: hash the ten samples already in staging."""
    return hash_staged(BW_SAMPLE, staged)


def hb_recorder(staged):
    """Bremen: hash both packages already in staging."""
    return hash_staged(download.HB_URLS, staged)


def he_marker(last_seen=None):
    """Hessen: the creationDate its API publishes for each district's package.

    Same two hops download.he_links() makes -- the navigation index, then one
    detail call per district -- but taking packages[0]['creationDate'] instead
    of the download link. 27 small JSON calls, no package downloaded.

    Keyed by district name, not by the download uri: that uri carries the date
    of the request (/downloadcenter/20260923/...), so keying on it would report
    a change every day.
    """
    r = requests.get(download.HE_INDEX, timeout=TIMEOUT)
    r.raise_for_status()

    marker = {}
    for node in r.json()['navigation']:
        name = node.get('name') or node['uri']
        detail = requests.get(download.HE_BASE + node['uri'], timeout=TIMEOUT)
        detail.raise_for_status()
        packages = detail.json()['searchresult']['packages']
        if not packages:
            print('  %s: no package listed' % name)
            continue
        marker[name] = packages[0]['creationDate']

    if not marker:
        raise RuntimeError('he: no packages listed at %s' % download.HE_INDEX)
    return marker


def he_recorder(staged):
    """Hessen: re-read the API. It is 28 small JSON calls, nothing downloaded."""
    return he_marker()


def hh_marker(last_seen=None):
    """Hamburg: the YYYY-MM-DD the newest package carries in its filename.

    download.hh_links() returns the dated packages oldest first and hh() takes
    the last one, so the marker is that same file's date -- the state is up to
    date exactly when the newest package on the portal is the one we hold.
    A plain string, since Hamburg ships one package rather than a set.
    """
    urls = download.hh_links()
    if not urls:
        raise RuntimeError('hh: no dated .zip found at %s' % download.HH_URL)

    newest = urls[-1]
    return newest.rsplit('.', 1)[0].rsplit('_', 1)[1]


def hh_recorder(staged):
    """Hamburg: re-read the portal page. It is one scrape, nothing downloaded."""
    return hh_marker()


def mv_marker(last_seen=None):
    """Mecklenburg-Vorpommern: the <updated> stamp on its Atom feed.

    The same feed download.mv_links() reads. MV publishes the whole dataset in
    one go, so the feed-level stamp answers for every package; the newest entry
    stamp stands in if the feed itself carries none.
    """
    r = requests.get(download.MV_URL, timeout=TIMEOUT)
    r.raise_for_status()

    ns = {'atom': 'http://www.w3.org/2005/Atom'}
    root = ET.fromstring(r.text)

    updated = root.findtext('atom:updated', namespaces=ns)
    if not updated:
        stamps = [e.findtext('atom:updated', namespaces=ns)
                  for e in root.findall('atom:entry', ns)]
        stamps = sorted(x for x in stamps if x)
        if not stamps:
            raise RuntimeError('mv: no <updated> in the feed at %s' % download.MV_URL)
        updated = stamps[-1]

    return updated.strip()


def mv_recorder(staged):
    """Mecklenburg-Vorpommern: re-read the feed. One request, nothing downloaded."""
    return mv_marker()


def nw_marker(last_seen=None):
    """Nordrhein-Westfalen: the timestamp index.json gives each file.

    The same catalogue download.nw_catalogue() reads for its byte counts; this
    takes `timestamp` instead of `size`, per file, so a single republished
    municipality is visible. One request for all 53.
    """
    r = requests.get(download.NW_URL + 'index.json', timeout=TIMEOUT)
    r.raise_for_status()

    marker = {}
    for dataset in r.json()['datasets']:
        for entry in dataset['files']:
            stamp = entry.get('timestamp')
            if not stamp:
                print('  %s: no timestamp, left out of the marker' % entry['name'])
                continue
            marker[entry['name']] = stamp

    if not marker:
        raise RuntimeError('nw: no timestamps in %sindex.json' % download.NW_URL)
    return marker


def nw_recorder(staged):
    """Nordrhein-Westfalen: re-read index.json. One request, nothing downloaded."""
    return nw_marker()


def rp_listing():
    """The date cell beside each package in the Rheinland-Pfalz index.

    The same listing download.rp_links() takes hrefs from -- an Apache table
    whose next cell after the link is the file's date. All 3,112 packages, so a
    single republished municipality shows up.
    """
    r = requests.get(download.RP_URL, verify=False, timeout=TIMEOUT)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, 'html.parser')

    marker = {}
    for tag in soup.find_all('a', href=True):
        if not tag['href'].endswith('.zip'):
            continue
        cell = tag.find_parent('td')
        date_cell = cell.find_next_sibling('td') if cell else None
        if date_cell is None:
            print('  %s: no date cell beside it, left out' % tag['href'])
            continue
        marker[tag['href']] = date_cell.get_text(strip=True)

    if not marker:
        raise RuntimeError('rp: no dated .zip rows at %s -- layout changed?'
                           % download.RP_URL)
    return marker


def rp_marker(last_seen=None):
    """Rheinland-Pfalz: the index dates, plus the SHA-256 of BasisDLM.

    BasisDLM is not in that listing -- rp_links() appends it from a fixed url --
    and the only thing the server offers for it is a Last-Modified, so it is
    hashed instead. That costs 547 MB, which is why the listing is compared
    first: a municipality that moved answers the question before the download
    starts.
    """
    marker = rp_listing()

    if last_seen:
        for name, date in marker.items():
            if last_seen.get(name) != date:
                raise Changed('%s differs (%s -> %s), BasisDLM not fetched'
                              % (name, last_seen.get(name) or 'never seen', date))

    # Only reached when all 3,112 dates match, or nothing was recorded before.
    marker.update(hash_urls([download.RP_BASISDLM], last_seen, verify=False))
    return marker


def rp_recorder(staged):
    """Rheinland-Pfalz: index dates, and BasisDLM hashed off disk.

    The download just staged BasisDLM, so hashing it there costs no bandwidth
    where rp_marker() would spend 547 MB.
    """
    marker = rp_listing()
    try:
        marker.update(hash_staged([download.RP_BASISDLM], staged))
    except RuntimeError as exc:
        print('  BasisDLM left out of the marker: %s' % exc)
    return marker


SH_DATUM = re.compile(rb'"DATUM"\s*:\s*"([^"]*)"')
# Bytes kept between chunks so a match straddling the boundary survives; longer
# than any "DATUM": "..." run, and no match is counted in both halves.
OVERLAP = 64


def count_datum(chunks):
    """Tally DATUM values over an iterable of byte chunks.

    Bytes, not text: the manifest's response declares no encoding, and a regex
    over the raw stream needs no decoding of a 243 MB document.
    """
    counts = {}
    carry = b''

    def tally(value):
        value = value.decode('utf-8', 'replace')
        counts[value] = counts.get(value, 0) + 1

    for chunk in chunks:
        if not chunk:
            continue
        block = carry + chunk
        # Everything from `cut` on is carried into the next block, so a match
        # starting there is left for then -- counting it here as well as there
        # is what inflates a tally when a match lands in the overlap.
        cut = max(0, len(block) - OVERLAP)
        for match in SH_DATUM.finditer(block):
            if match.start() >= cut:
                break
            tally(match.group(1))
        carry = block[cut:]

    for match in SH_DATUM.finditer(carry):
        tally(match.group(1))
    return counts


def sh_marker(last_seen=None):
    """Schleswig-Holstein: the DATUM of every Flur in the manifest, counted.

    The same GeoJSON download.sh_manifest() reads, and the only thing SH
    publishes about its data -- a HEAD returns no Last-Modified, ETag or even
    Content-Length, so the 243 MB manifest has to be read in full.

    All 16,885 Flure currently carry one date, so the marker is {date: count}
    rather than an entry per Flur: a 12-line JSON instead of a 700 KB one, and
    still sensitive to any Flur whose date moves, which then shows as a second
    date with its own count.

    Scanned with a regex over the stream rather than json.load()ed, so memory
    stays flat instead of holding a 16,885-feature document.
    """
    with requests.get(download.SH_GEOJSON_URL, stream=True, verify=False,
                      timeout=TIMEOUT) as r:
        r.raise_for_status()
        counts = count_datum(r.iter_content(chunk_size=1024 * 1024))

    if not counts:
        raise RuntimeError('sh: no DATUM found in %s' % download.SH_GEOJSON_NAME)

    print('  %d Flur(e), %d distinct DATUM: %s'
          % (sum(counts.values()), len(counts), ', '.join(sorted(counts))))
    return counts


def sh_recorder(staged):
    """Schleswig-Holstein: read the manifest the download already staged.

    sh_manifest() leaves it in staging, so recording costs nothing; only a run
    that somehow lacks it goes back to the portal for 243 MB.
    """
    path = Path(staged) / download.SH_GEOJSON_NAME
    if not path.is_file():
        print('  %s not in staging, re-reading the portal' % download.SH_GEOJSON_NAME)
        return sh_marker()

    with path.open('rb') as handle:
        counts = count_datum(iter(lambda: handle.read(1024 * 1024), b''))

    if not counts:
        raise RuntimeError('sh: no DATUM in the staged %s' % download.SH_GEOJSON_NAME)
    return counts


def sl_marker(last_seen=None):
    """Saarland: the Modified time Nextcloud shows for each package.

    One WebDAV PROPFIND on the folder download.sl_links() builds its urls in,
    which is where the share page's Modified column comes from -- so all seven
    packages in a single request, nothing downloaded. The share token is
    resolved fresh, as it is for a download: it is not stable between runs.
    """
    folder = download.sl_links()[0].rsplit('/', 1)[0] + '/'
    body = ('<?xml version="1.0"?><d:propfind xmlns:d="DAV:"><d:prop>'
            '<d:getlastmodified/></d:prop></d:propfind>')
    r = requests.request('PROPFIND', folder, data=body, timeout=TIMEOUT,
                         headers={'Depth': '1', 'Content-Type': 'application/xml',
                                  'User-Agent': 'Mozilla/5.0'})
    r.raise_for_status()

    ns = {'d': 'DAV:'}
    marker = {}
    for response in ET.fromstring(r.content).findall('d:response', ns):
        href = response.findtext('d:href', default='', namespaces=ns)
        name = unquote(href.rstrip('/').rsplit('/', 1)[-1])
        if href.endswith('/'):
            continue                      # the folder itself, not a package
        stamp = response.findtext('.//d:getlastmodified', namespaces=ns)
        if not stamp:
            print('  %s: no modified time, left out of the marker' % name)
            continue
        marker[name] = stamp

    if not marker:
        raise RuntimeError('sl: no files listed under %s' % folder)
    return marker


def sl_recorder(staged):
    """Saarland: re-read the folder listing. One request, nothing downloaded."""
    return sl_marker()


# Sachsen announces each publication in a changelog feed rather than dating the
# packages, so the check reads that instead of the batch-download page
# download.sn_links() scrapes. The state-wide entry is the one that matters:
# "gemarkungsweise" entries cover the per-Gemarkung products we do not fetch.
SN_CHANGELOG = 'https://www.geodaten.sachsen.de/changelog.xml'
SN_TITLE = 'Aktualisierung ALKIS sachsenweit'


def sn_marker(last_seen=None):
    """Sachsen: the <updated> of the newest state-wide ALKIS changelog entry."""
    r = requests.get(SN_CHANGELOG, timeout=TIMEOUT)
    r.raise_for_status()

    ns = {'atom': 'http://www.w3.org/2005/Atom'}
    stamps = []
    for entry in ET.fromstring(r.content).findall('.//atom:entry', ns):
        title = (entry.findtext('atom:title', default='', namespaces=ns) or '').strip()
        if title != SN_TITLE:
            continue
        updated = (entry.findtext('atom:updated', default='', namespaces=ns) or '').strip()
        if updated:
            stamps.append(updated)

    if not stamps:
        raise RuntimeError('sn: no %r entry in %s' % (SN_TITLE, SN_CHANGELOG))

    # Newest by parsed time, not by feed order or string order: the stamps carry
    # offsets (+01:00 in winter, +02:00 in summer).
    def when(stamp):
        try:
            return datetime.fromisoformat(stamp)
        except ValueError:
            return datetime.min.replace(tzinfo=timezone.utc)

    newest = max(stamps, key=when)
    print('  %d %r entries, newest %s' % (len(stamps), SN_TITLE, newest))
    return newest


def sn_recorder(staged):
    """Sachsen: re-read the changelog. One request, nothing downloaded."""
    return sn_marker()


def th_marker(last_seen=None):
    """Thueringen: the <updated> of the feed entries download.th_links() takes.

    The same feed and the same title filter, so the marker follows exactly the
    packages that get downloaded -- the feed also carries Shape and flurweise
    entries with their own stamps, and those are not ours. Keyed by title, since
    a future feed may match more than one entry.

    Deliberately not the feed-level <updated>: it moves when any entry does,
    including the ones we ignore.
    """
    r = requests.get(download.TH_URL, timeout=TIMEOUT)
    r.raise_for_status()
    soup = BeautifulSoup(r.content, 'xml')

    marker = {}
    for entry in soup.find_all('entry'):
        title_tag = entry.find('title')
        title = title_tag.get_text(strip=True) if title_tag else ''
        if not all(term in title for term in download.TH_TITLE_TERMS):
            continue
        updated = entry.find('updated')
        if updated is None:
            print('  %s: no updated, left out of the marker' % title)
            continue
        marker[title] = updated.get_text(strip=True)

    if not marker:
        raise RuntimeError('th: no entry matching %s at %s'
                           % (' + '.join(download.TH_TITLE_TERMS), download.TH_URL))
    return marker


def th_recorder(staged):
    """Thueringen: re-read the feed. One request, nothing downloaded."""
    return th_marker()


# Bayern and Sachsen-Anhalt are not this pipeline's states -- automation_base_by.py
# and automation_base_st.py fetch them -- but their sources are urls in the same
# config.ini, so the same hashing check applies. Sachsen-Anhalt's exports carry
# today's date in Last-Modified whether or not the content moved, which is why a
# hash rather than a header is the honest signal here.
#
# Those scripts download into <project>/_downloads/<state>, not into staging_dir,
# and empty it after a successful promotion -- so a recorder looks there and falls
# back to fetching when the files have already been cleared away.
EXTERNAL_DOWNLOADS = SCRIPT_DIR / '_downloads'


def by_urls():
    """Bayern's sources, from config.ini: the Hausumringe zips and the GeoPackage."""
    return list(config.BY_BUILDING_URLS or []) + [config.BY_GPKG_URL]


def st_urls():
    """Sachsen-Anhalt's sources, from config.ini: buildings, refs, parcels, usage."""
    return [config.ST_GBIS_GEBAEUDE_ZIP_URL, config.ST_GEBAEUDE_REF_ZIP_URL,
            config.ST_PARCEL_ZIP_URL, config.ST_USAGE_ZIP_URL]


def configured(state, urls):
    """The urls, or a clear error naming the state whose config keys are missing."""
    missing = [u for u in urls if not u]
    if missing or not urls:
        raise RuntimeError('%s: its source urls are not all set in config.ini' % state)
    return urls


def by_marker(last_seen=None):
    """Bayern: the SHA-256 of each configured source, one at a time."""
    return hash_urls(configured('by', by_urls()), last_seen)


def st_marker(last_seen=None):
    """Sachsen-Anhalt: the SHA-256 of each configured source, one at a time."""
    return hash_urls(configured('st', st_urls()), last_seen)


def external_recorder(state, urls, staged=None):
    """Hash what the state's own script downloaded, or fetch if it has cleared up."""
    try:
        return hash_staged(urls, staged or EXTERNAL_DOWNLOADS / state)
    except RuntimeError as exc:
        print('  %s -- hashing from the portal instead' % exc)
        return hash_urls(urls)


def by_recorder(staged=None):
    """Bayern: hash from where its own script downloads, not this pipeline's staging."""
    return external_recorder('by', configured('by', by_urls()), staged)


def st_recorder(staged=None):
    """Sachsen-Anhalt: likewise, from <project>/_downloads/st unless told otherwise."""
    return external_recorder('st', configured('st', st_urls()), staged)


# abbreviation -> marker function, filled in as each state's logic is defined.
CHECKS = {
    'bb': bb_marker,
    'bw': bw_marker,
    'hb': hb_marker,
    'he': he_marker,
    'hh': hh_marker,
    'mv': mv_marker,
    'nw': nw_marker,
    'rp': rp_marker,
    'sh': sh_marker,
    'sl': sl_marker,
    'sn': sn_marker,
    'th': th_marker,
    'by': by_marker,
    'st': st_marker,
}

# abbreviation -> how download.py records that state once it has the data.
RECORDERS = {
    'bb': bb_recorder,
    'bw': bw_recorder,
    'hb': hb_recorder,
    'he': he_recorder,
    'hh': hh_recorder,
    'mv': mv_recorder,
    'nw': nw_recorder,
    'rp': rp_recorder,
    'sh': sh_recorder,
    'sl': sl_recorder,
    'sn': sn_recorder,
    'th': th_recorder,
    'by': by_recorder,
    'st': st_recorder,
}


# ============================================================================


def check(state, recorded, force=False):
    """(needs_update, marker, lines) for one state.

    marker is None when no marker was read to completion -- either the state has
    never been recorded, or the check stopped at the first difference.

    With no last seen marker the answer is already "needs update", so the check
    is not run at all. That matters for bw, where running it means downloading
    541 MB to confirm what is known before the first byte. `force` reads it
    anyway; nothing on the command line sets it, since a caller that wants the
    marker itself calls the CHECKS entry directly.
    """
    last_seen = recorded.get(state, {}).get('marker')
    if not last_seen and not force:
        return True, None, ['never recorded']

    try:
        marker = CHECKS[state](last_seen)
    except Changed as exc:
        return True, None, [str(exc)]

    if marker != last_seen:
        return True, marker, describe(marker, last_seen)
    return False, marker, []


def describe(marker, last_seen):
    """How a marker differs from the recorded one, as lines to print.

    A dict marker holds one entry per file, so the difference is named file by
    file -- that is what says which packages the portal republished.
    """
    if isinstance(marker, dict) and last_seen is None:
        return ['never recorded (%d file(s))' % len(marker)]
    if not isinstance(marker, dict) or not isinstance(last_seen, dict):
        return ['portal %s, last seen %s' % (marker, last_seen if last_seen else 'never')]

    added = sorted(k for k in marker if k not in last_seen)
    changed = sorted(k for k in marker if k in last_seen and marker[k] != last_seen[k])
    gone = sorted(k for k in last_seen if k not in marker)

    lines = ['%d changed, %d new, %d gone (of %d file(s))'
             % (len(changed), len(added), len(gone), len(marker))]
    lines += ['  %s  %s -> %s' % (k, last_seen[k], marker[k]) for k in changed]
    lines += ['  %s  new  %s' % (k, marker[k]) for k in added]
    lines += ['  %s  no longer listed' % k for k in gone]
    return lines


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


def elapsed(started):
    """Wall clock since a time.monotonic() reading, as H:MM:SS."""
    secs = round(time.monotonic() - started)
    return '%d:%02d:%02d' % (secs // 3600, secs % 3600 // 60, secs % 60)


def build_parser():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('states', nargs='*',
                        type=state_arg(sorted(set(download.STATES) | set(CHECKS))),
                        metavar='STATE', help='state abbreviations to check')
    parser.add_argument('--all', action='store_true',
                        help='check every state that has a check implemented')
    parser.add_argument('--job-id', metavar='ID',
                        help='job id naming this run\'s log, as '
                             'logs/check_update_<id>.log (default: from the clock)')
    return parser


def main(argv):
    parser = build_parser()
    args = parser.parse_args(argv)

    states = sorted(CHECKS) if args.all else args.states
    if not states:
        parser.print_help()
        return

    # An empty --job-id= would name the log '.log', so only a non-empty value
    # replaces the minted default.
    job_id = args.job_id or datetime.now().strftime(JOB_FMT)
    log = LOG_DIR / ('check_update_%s.log' % job_id)
    print('job     %s' % job_id)
    print('log     %s' % log)

    recorded = state_db.latest_markers()
    run_started = time.monotonic()
    needed, current, failed = [], [], []

    with tee_output(log):
        print('start   %s' % datetime.now().strftime(TIME_FMT))

        for num, abbr in enumerate(states, 1):
            print('[%d/%d] %s' % (num, len(states), abbr))
            started = time.monotonic()

            if abbr not in CHECKS:
                print('%s: no check implemented' % abbr, file=sys.stderr)
                continue

            try:
                needs, marker, lines = check(abbr, recorded)
                if needs:
                    needed.append(abbr)
                    print('%s: update available -- %s' % (abbr, lines[0]))
                    for line in lines[1:]:
                        print(line)
                else:
                    current.append(abbr)
                    print('%s: up to date (%s)'
                          % (abbr,
                             '%d file(s)' % len(marker) if isinstance(marker, dict) else marker))
            except Exception as exc:
                failed.append(abbr)
                print('%s: CHECK FAILED: %s' % (abbr, exc), file=sys.stderr)
            finally:
                # In a finally so a failed check still says how long it took -- a
                # portal that hangs until the timeout is the usual reason.
                print('      elapsed %s  (%s)' % (elapsed(started), abbr))

        print('end     %s' % datetime.now().strftime(TIME_FMT))
        print('elapsed %s' % elapsed(run_started))

        # The run's verdict, last so cron.py can read it off the end of the
        # output: space-separated abbreviations, '-' for none.
        print('')
        print('update needed: %s' % (' '.join(needed) or '-'))
        print('up to date: %s' % (' '.join(current) or '-'))
        print('check failed: %s' % (' '.join(failed) or '-'))


if __name__ == '__main__':
    main(sys.argv[1:])
