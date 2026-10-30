#!/usr/bin/env python3
"""Count GCP resources per FortiCNAPP billing category using Cloud Asset Inventory."""

import argparse
import base64
import functools
import gzip
import json
import math
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import google.auth
from google.api_core import exceptions as core_exceptions
from google.api_core import retry as core_retry
from google.cloud import asset_v1
from google.cloud.asset_v1 import ContentType
from google.protobuf.json_format import MessageToDict


# Bumped by hand whenever a change can move a count. A count is only interpretable
# alongside the version that produced it: the counting rules change as mapping decisions
# land, so without this there is no way to tell a change in a customer's estate from a
# change in this script.
SCRIPT_VERSION = "2026.10.27"


@functools.lru_cache(maxsize=None)
def script_version():
    """SCRIPT_VERSION, plus the live git SHA when the script is running inside its own clone.

    Cached because it forks git, and gRPC logs "FD from fork parent still in poll list" for
    every fork after the Cloud Asset client exists. Uncached, the two late calls put five
    of those lines into every field transcript. The first call is in main(), before the
    client, and the answer cannot change during a run.

    Field engineers pull these scripts fresh per engagement, sometimes by cloning and
    sometimes as a bare file download, so neither source identifies a version on its own.
    The SHA is only trusted when git also reports this file as tracked: a script copied into
    an unrelated repo would otherwise report that repo's HEAD, which is a plausible-looking
    wrong answer rather than a missing one.

    A tracked file can still differ from the commit it names, so local edits are reported as
    "-dirty". Without that a modified script stamps a clean SHA, and the counts in that
    transcript cannot be reproduced from the commit they claim -- the same class of wrong
    answer, arrived at from the other direction.
    """
    here = Path(__file__).resolve()
    try:
        tracked = subprocess.run(
            ["git", "-C", str(here.parent), "ls-files", "--error-unmatch", here.name],
            capture_output=True, text=True, timeout=5,
        )
        if tracked.returncode != 0:
            return SCRIPT_VERSION
        sha = subprocess.run(
            ["git", "-C", str(here.parent), "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=5,
        ).stdout.strip()
        modified = subprocess.run(
            ["git", "-C", str(here.parent), "diff", "--quiet", "HEAD", "--", here.name],
            capture_output=True, text=True, timeout=5,
        ).returncode != 0
    except Exception:
        return SCRIPT_VERSION
    if not sha:
        return SCRIPT_VERSION
    return f"{SCRIPT_VERSION}+{sha}{'-dirty' if modified else ''}"


# ---------------------------------------------------------------------------
# Measurement ladder
# ---------------------------------------------------------------------------
#
# Governing principle: get the best measurement available, probe for it, and fall back to the
# next best -- per (resource type, account), not once per run. Every count carries the rung
# that produced it, so the field can judge how far a number may sit from measured product
# usage instead of having to assume.
#
# GCP is the cloud where the metric costs something. ListAssetsRequest.read_time is a *point*
# parameter, so unlike Cost Explorer (which returns a daily series in one call) there is no
# way to ask Cloud Asset Inventory for a range: every snapshot in the average is another full
# enumeration of the org. That is what makes the sampling schedule below a real decision
# rather than a detail.

WINDOW_DAYS = 30

# See sample_offsets(). Stride 5 is not arbitrary and must not be "rounded" to 7.
SAMPLE_STRIDE_DAYS = 5

# ListAssets is limited to 100 requests per minute per consumer project, and 800 per minute per
# organization. Calls are sequential, so the rate depends on latency, not estate size. A large
# estate just keeps the run going long enough to hit the limit. The API's default page is 100
# assets, and asking for its 1000 maximum cuts every multi-page enumeration by up to 10x.
CAI_PAGE_SIZE = 1000


def _note_cai_retry(exc):
    # Counted so field runs show how often the quota bites. Recorded without the message,
    # which quotes the consumer project (see Diagnostics).
    if isinstance(exc, core_exceptions.TooManyRequests):
        DIAG.bump("cai_quota_retries")


# The client's default retry covers only DeadlineExceeded and ServiceUnavailable, so a quota
# error would fail its category on the first refusal. Backing off to 60s outlasts a
# per-minute window. The retry's default 120s total is left as is, so a quota that stays
# exhausted still ends in a loud per-category failure rather than a long silent stall. The
# pager reuses this per page, so a retry resumes the enumeration rather than restarting it.
CAI_RETRY = core_retry.Retry(
    predicate=core_retry.if_exception_type(
        core_exceptions.DeadlineExceeded,
        core_exceptions.ServiceUnavailable,
        core_exceptions.TooManyRequests,
    ),
    initial=2.0,
    maximum=60.0,
    multiplier=2.0,
    on_error=_note_cai_retry,
)

TIME_RANGE_NOW = "now"
TIME_RANGE_WINDOW = f"{WINDOW_DAYS}d"

# Rung names double as the Method column's value, so they are short and name the instrument.
RUNG_ASSET_HISTORY = "cai-read-time"
RUNG_POINT_IN_TIME = "point-in-time"

# Used when the asset types inside one category did not all come from the same rung.
METHOD_MIXED = "mixed"

# Probe verdicts. UNKNOWN exists for the loud-degradation rule: a probe that cannot tell "this
# instrument is unavailable here" from "this instrument returned nothing" must be read as
# neither, so the rung is skipped, the ambiguity is tallied, and a canary reports it.
PROBE_AVAILABLE = "available"
PROBE_UNAVAILABLE = "unavailable"
PROBE_UNKNOWN = "unknown"


def sample_offsets(window_days=WINDOW_DAYS, stride_days=SAMPLE_STRIDE_DAYS):
    """Day offsets, counting back from the run, at which to snapshot the estate.

    The product's metric is a daily snapshot averaged over the month, and this script cannot
    take it literally: read_time is a point parameter, so each extra snapshot costs another
    full enumeration. Sampling is the compromise -- and *which* days get sampled matters more
    than how many.

    Workloads move with the day of the week, weekends running lighter than weekdays. A stride
    sharing a factor with 7 therefore lands every sample on the same weekday and bakes that
    bias straight into the average, which makes a weekly stride the single worst choice
    available rather than the obvious one. When gcd(stride, 7) == 1 the weekday advances by a
    fixed non-zero amount each step, so exactly 7 samples visit all 7 weekdays once each and
    the average is weekday-balanced by construction -- including two weekend days, with no
    dependence on which day the run happens to start.

    Stride 5 is the only stride that both fills a 30-day window and balances the weekdays:
    3 and 4 span 18 and 24 days, 6 overshoots at 36, and 7 is degenerate.
    """
    return [i * stride_days for i in range(window_days // stride_days + 1)]


def sample_is_weekday_balanced(stride_days, points):
    """True when `points` samples at this stride cover every weekday equally.

    Guards the --sample-every override: a field engineer who picks 7 because it reads as
    "weekly" would silently reintroduce exactly the bias the window exists to remove.
    """
    return math.gcd(stride_days, 7) == 1 and points % 7 == 0


class Measurement:
    """One asset type's count, with how it was obtained.

    `count` keeps the meaning it had before the ladder existed: a number, or None when the
    type could not be measured at all. Provenance rides alongside rather than wrapping it, so
    nothing downstream that sums or canaries these counts has to learn about rungs.
    """

    __slots__ = ("count", "rung", "time_range", "points")

    def __init__(self, count, rung=RUNG_POINT_IN_TIME, time_range=TIME_RANGE_NOW, points=None):
        self.count = count
        self.rung = rung
        self.time_range = time_range
        self.points = points

    def provenance(self):
        out = {"rung": self.rung, "time_range": self.time_range}
        if self.points is not None:
            out["points"] = self.points
        return out


def category_provenance(provenance, breakdown):
    """Derive one category's Time Range and Method from the types that composed it.

    Types that could not be measured are excluded: they contribute nothing to the count, so
    they must not drag its label either way.
    """
    used = [p for at, p in provenance.items() if breakdown.get(at) is not None]
    if not used:
        return TIME_RANGE_NOW, RUNG_POINT_IN_TIME
    rungs = {p["rung"] for p in used}
    ranges = {p["time_range"] for p in used}
    return (ranges.pop() if len(ranges) == 1 else METHOD_MIXED,
            rungs.pop() if len(rungs) == 1 else METHOD_MIXED)


# Minimum instance count before the disk-ratio canary is worth evaluating. Below this a
# demo-sized environment produces ratios that swing wildly on one disk, which is how a canary
# earns a reputation for crying wolf and gets ignored.
CANARY_MIN_INSTANCES = 5


def _pkg_version(name):
    """Installed version of a distribution, or "unknown". Recorded so a count that looks wrong
    can be tied to the SDK that produced it -- an SDK change can move a count as surely as a
    change to this script."""
    try:
        from importlib.metadata import version
        return version(name)
    except Exception:
        return "unknown"


class Diagnostics:
    """Aggregate run diagnostics, emitted under the JSON's "diagnostics" key.

    One constraint governs everything recorded here: **counts, ratios, booleans and error
    codes only.** No resource names, asset paths, project numbers or location detail that
    would fingerprint a customer. The field engineer has to be able to read this block before
    forwarding it, and it travels through several pairs of hands to reach Product. Nothing is
    ever sent anywhere by the script itself.

    Canaries and measures are computed on every run: a diagnostic that has to be switched on
    only gets switched on by engineers who already suspect a problem, which is the one sample
    that says least about how often filters break in the wild. The heavier bundle is opt-in.
    """

    def __init__(self):
        self.canaries = []
        self.measures = {}
        self.denials = {}
        self.rungs = {}

    def measure(self, name, value):
        self.measures[name] = value

    def bump(self, name, by=1):
        self.measures[name] = self.measures.get(name, 0) + by

    def rung_trace(self, asset_type, rung, verdict):
        """Tally one probe verdict by (asset type, rung, verdict).

        Counts only, never which project: this block travels to Product through several pairs
        of hands and must not fingerprint a customer. Aggregated this way it answers the
        question the ladder exists to ask -- how often each instrument is actually reachable
        in the field, per asset type.
        """
        key = f"{asset_type}/{rung}/{verdict}"
        self.rungs[key] = self.rungs.get(key, 0) + 1

    def record_denial(self, scope_kind, code):
        """Tally one failure by (scope kind, exception type). Types only -- never messages,
        which quote project numbers and asset paths."""
        key = f"{scope_kind}/{code}"
        self.denials[key] = self.denials.get(key, 0) + 1

    def canary(self, name, ok, observed, note):
        """Record an assertion. ok=None means it could not be evaluated, reported as "skipped"
        rather than passing -- an unevaluated check must never read as a clean one."""
        self.canaries.append({
            "name": name,
            "status": "skipped" if ok is None else ("pass" if ok else "fail"),
            "observed": observed,
            "note": note,
        })

    def report(self):
        """Print one line per failed canary. Silent when everything passed, which is the
        point: always on, invisible until something is actually wrong."""
        failed = [c for c in self.canaries if c["status"] == "fail"]
        for c in failed:
            print(f"  CANARY FAIL [{c['name']}]: {c['note']} (observed: {c['observed']})",
                  file=sys.stderr)
        return failed

    def to_json(self, verbose=False):
        out = {
            "canaries": self.canaries,
            "canaries_failed": sum(1 for c in self.canaries if c["status"] == "fail"),
            "measures": self.measures,
        }
        if verbose:
            out["denials"] = self.denials
            out["ladder"] = self.rungs
            out["environment"] = {
                "python": ".".join(str(v) for v in sys.version_info[:3]),
                "google-cloud-asset": _pkg_version("google-cloud-asset"),
                "google-auth": _pkg_version("google-auth"),
                "script_version": script_version(),
            }
        return out


DIAG = Diagnostics()


CATEGORIES = [
    {
        "category": "VM",
        "asset_types": ["compute.googleapis.com/Instance"],
    },
    {
        "category": "VM Image",
        "asset_types": ["compute.googleapis.com/Image"],
    },
    {
        "category": "Serverless Container",
        # A Job is counted like a Lambda function: a defined unit that runs on demand or on a
        # schedule. Its Executions are ephemeral runs and are not counted, the same way a
        # bucket is counted rather than its objects (footnote 3).
        "asset_types": ["run.googleapis.com/Service", "run.googleapis.com/Job"],
    },
    {
        "category": "Container Image",
        "asset_types": ["artifactregistry.googleapis.com/DockerImage"],
    },
    {
        "category": "Serverless Function",
        # v1 and v2 are separate asset types — query both
        "asset_types": [
            "cloudfunctions.googleapis.com/Function",
            "cloudfunctions.googleapis.com/CloudFunction",
        ],
    },
    {
        "category": "Managed DB (PaaS)",
        "asset_types": [
            "sqladmin.googleapis.com/Instance",
            # AlloyDB instances are always cluster members, so the cluster represents
            # them — same treatment as Aurora under rds:cluster on AWS.
            "alloydb.googleapis.com/Cluster",
            "spanner.googleapis.com/Instance",
            "firestore.googleapis.com/Database",
            "redis.googleapis.com/Instance",
            "redis.googleapis.com/Cluster",
            "memcache.googleapis.com/Instance",
            "memorystore.googleapis.com/Instance",
            "bigtableadmin.googleapis.com/Instance",
        ],
    },
    {
        "category": "Data Warehouse",
        "asset_types": [
            # Counted once per (project, location), not per dataset -- see warehouse_key.
            "bigquery.googleapis.com/Dataset",
            "dataproc.googleapis.com/Cluster",
        ],
    },
    {
        "category": "Bucket Storage",
        "asset_types": [
            "storage.googleapis.com/Bucket",
            "backupdr.googleapis.com/BackupVault",
        ],
    },
    {
        "category": "Non-OS Disk",
        "asset_types": ["compute.googleapis.com/Disk"],
    },
]


def resolve_project_id():
    """Resolve project ID from env var or gcloud ADC."""
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCLOUD_PROJECT")
    if project_id:
        return project_id

    try:
        result = subprocess.run(
            ["gcloud", "config", "get-value", "project"],
            capture_output=True, text=True, timeout=10,
        )
        project_id = result.stdout.strip()
        if project_id and project_id != "(unset)":
            return project_id
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass

    # Try google.auth default credentials project
    try:
        _, project_id = google.auth.default()
        if project_id:
            return project_id
    except Exception:
        pass

    return None


def asset_data(asset):
    """Return an asset's resource data as a plain dict across proto-plus/Struct shapes."""
    data = getattr(getattr(asset, "resource", None), "data", None)
    if data is None:
        return {}
    if isinstance(data, dict):
        return data
    try:
        return MessageToDict(data)
    except Exception:
        try:
            return dict(data)
        except Exception:
            return {}


def normalize_link(link):
    """Reduce a GCE self-link/source URL to its /projects/... suffix for comparison."""
    idx = link.find("/projects/")
    return link[idx:] if idx != -1 else link


# Statuses in which an instance leaves the VM line and its boot disk is re-admitted to
# Non-OS Disk instead, so it counts once at the disk weight (footnote 26).
#
# THE CSP TERMINATION TRAP: on GCP a *stopped* instance reads TERMINATED. There is no STOPPED
# status at all (confirmed against the Compute Engine instance-lifecycle reference, which
# lists PENDING, PROVISIONING, STAGING, RUNNING, PENDING_STOP, STOPPING, TERMINATED,
# REPAIRING, SUSPENDING, SUSPENDED). AWS uses "terminated" for destroyed, so porting the AWS
# state list across clouds by name does the exact opposite of what is intended here. Deleted
# GCP instances simply vanish from the API, so nothing has to exclude them.
#
# SUSPENDED is included by the same reasoning footnote 26 applies to Azure's two stopped
# states: what matters under a resource count is that the instance is not running and its
# disks persist. A suspended instance bills for storage and runs no agent. Vetoable.
#
# STOPPING is included although transitional states count as running on AWS and Azure: a
# GCP-side bug over-reports instances in STOPPING, which confused customers, so the vCPU
# model excludes it on GCP alone and the sizing count follows (ruled 2026-10-26, OI-50).
# GCP only and STOPPING only; PENDING_STOP and SUSPENDING still count as running.
#
# Membership is tested rather than a counted-states allow-list on purpose: a status GCP adds
# later then counts as a VM (a visible over-count) instead of vanishing from both lines (a
# silent under-count), consistent with preferring filters that fail loudly.
STOPPED_INSTANCE_STATUSES = {"TERMINATED", "SUSPENDED", "STOPPING"}

_INSTANCE_STATUS_WARNED = False

# Boot disks of stopped instances, from the present-day pass only. The count does not need
# them -- a disk missing from the counted boot set is already admitted -- but the disk-reason
# breakdown does, to tell a re-admitted boot disk from an ordinary data disk.
_STOPPED_BOOT_LINKS = set()


def is_counted_instance(asset):
    """False for instances that have left the VM line under footnote 26.

    is_counted_instance and boot_disk_links must agree: a boot disk is excluded from Non-OS
    Disk exactly when its instance is counted here. If they drift, an instance with only a
    boot disk scores zero on both lines and disappears from the count while agentless still
    scans it.
    """
    global _INSTANCE_STATUS_WARNED
    if asset.asset_type != "compute.googleapis.com/Instance":
        return True
    status = asset_data(asset).get("status")
    if not status:
        # Counting it is the loud direction: an over-count is visible on the VM row, whereas
        # dropping every instance with an unreadable status is not.
        if not _INSTANCE_STATUS_WARNED:
            print("  WARNING: compute instance assets carry no 'status' field; stopped "
                  "instances cannot be separated and are counted on the VM line",
                  file=sys.stderr)
            _INSTANCE_STATUS_WARNED = True
        DIAG.measure("instance_status_unreadable", True)
        return True
    return status not in STOPPED_INSTANCE_STATUSES


def boot_disk_links(client, parent, read_time=None, tally=True):
    """Self-links of boot disks belonging to instances that count on the VM line.

    Only counted instances contribute, which is what re-admits a stopped instance's boot disk
    to Non-OS Disk: it appears in no instance's boot set, so is_data_disk treats it as data.
    The exclusion is justified by the VM line already accounting for that disk, so it lapses
    the moment the VM is not counted (footnote 26).
    """
    # The boot set must be rebuilt at each sampled instant. Which disks were boot disks 30
    # days ago is not what they are today, and classifying a historical disk against today's
    # boot set silently misfiles it -- on the one line footnote 26 made load-bearing.
    request = asset_v1.ListAssetsRequest(
        parent=parent,
        asset_types=["compute.googleapis.com/Instance"],
        content_type=ContentType.RESOURCE,
        page_size=CAI_PAGE_SIZE,
        **({"read_time": read_time} if read_time else {}),
    )
    links = set()
    for asset in client.list_assets(request, retry=CAI_RETRY):
        # This pass is the only place the run sees stopped instances: the VM count filters
        # them out, and the stopped fraction is one of the things field runs exist to measure.
        if not is_counted_instance(asset):
            if tally:
                DIAG.bump("instances_stopped")
                for disk in asset_data(asset).get("disks") or []:
                    if disk.get("boot") and disk.get("source"):
                        _STOPPED_BOOT_LINKS.add(normalize_link(disk["source"]))
            continue
        if tally:
            # Tallied only on the present-day pass. These measures feed the disk-ratio canary
            # and the stopped fraction, and counting them once per sample would multiply both
            # by the number of samples.
            DIAG.bump("instances_counted")
        for disk in asset_data(asset).get("disks") or []:
            if disk.get("boot") and disk.get("source"):
                links.add(normalize_link(disk["source"]))
    return links


def is_data_disk(asset, boot_links):
    """True unless the disk is some instance's boot disk."""
    self_link = asset_data(asset).get("selfLink") or ""
    return normalize_link(self_link) not in boot_links


# The GKE persistent-disk provisioners (in-tree and the PD CSI driver) write the claim that
# created a disk into its description as JSON keyed kubernetes.io/created-for/... . Unverified
# on a live estate; a miss only moves a disk to another reason, never in or out of the count.
_KUBERNETES_DISK_MARKER = "kubernetes.io/created-for/"


def disk_reason(asset, boot_links):
    """Why a disk is or is not in Non-OS Disk, for the nonos_disk_reasons breakdown.

    The first test is the count's own (is_data_disk), so boot_disk_excluded is exactly the
    disks the count leaves out and every other reason sums to the present-day figure. The rest
    only split the counted disks: Kubernetes volumes, attached data disks, and unattached
    disks, the last divided by whether the disk was made from an image, which is what a boot
    disk left behind by a deleted instance looks like.
    """
    data = asset_data(asset)
    link = normalize_link(data.get("selfLink") or "")
    if link in boot_links:
        return "boot_disk_excluded"
    if link in _STOPPED_BOOT_LINKS:
        return "boot_disk_stopped_instance"
    if _KUBERNETES_DISK_MARKER in (data.get("description") or ""):
        return "kubernetes_volume"
    if data.get("users"):
        return "data_disk_attached"
    if data.get("sourceImage"):
        return "unattached_from_image"
    return "data_disk_unattached"


def tally_disk_reason(reason):
    reasons = DIAG.measures.setdefault("nonos_disk_reasons", {})
    reasons[reason] = reasons.get(reason, 0) + 1


def is_standalone_run_service(asset):
    """False for Cloud Run services that only exist to back a gen2 Cloud Function.

    Those are already counted under Serverless Function; Cloud Functions tags the
    services it provisions with goog-managed-by=cloudfunctions.
    """
    if asset.asset_type != "run.googleapis.com/Service":
        return True
    data = asset_data(asset)
    labels = data.get("labels") or (data.get("metadata") or {}).get("labels") or {}
    DIAG.bump("run_services_seen")
    if labels.get("goog-managed-by") == "cloudfunctions":
        # Tallied because this filter fails silently: if the label stops matching, every gen2
        # function is counted a second time as a Cloud Run service and the total still looks
        # plausible.
        DIAG.bump("run_services_filtered_as_functions")
        return False
    return True


_SQL_SHAPE_WARNED = False

# Cloud SQL instance types that are copies of a primary counted in its own right: read
# replicas (cross-region, cascading and DR replicas included), Enterprise Plus read pools,
# and the green instance of a blue-green deployment. ON_PREMISES_INSTANCE is not a copy, it
# represents an external source database, and is left as it was.
_SQL_COPY_TYPES = {"READ_REPLICA_INSTANCE", "READ_POOL_INSTANCE", "GREEN_INSTANCE"}


def is_primary_sql_instance(asset):
    """False for Cloud SQL copies of a primary that is already counted.

    A managed database counts once regardless of how many copies back it (footnote 23):
    replica count is a scaling knob, not a distinct workload. Regional (HA) Cloud SQL needs
    no handling here, since the standby is not a separate instance in the API.

    If neither field the API documents is present the shape has changed, so this warns
    once and counts the instance. Over-counting loudly beats under-counting silently.
    """
    global _SQL_SHAPE_WARNED
    data = asset_data(asset)
    if "instanceType" in data:
        return data.get("instanceType") not in _SQL_COPY_TYPES
    if "masterInstanceName" in data:
        return not data.get("masterInstanceName")
    if not _SQL_SHAPE_WARNED:
        print(
            "  WARNING: Cloud SQL assets carry neither instanceType nor masterInstanceName; "
            "read replicas cannot be identified and are being counted as separate databases",
            file=sys.stderr,
        )
        _SQL_SHAPE_WARNED = True
    DIAG.measure("sql_replica_shape_unreadable", True)
    return True


def _is_secondary(data, role_field, role_key, fallback):
    """True when a cross-region replication config names this resource a SECONDARY.

    The role is an output-only field, so whether Cloud Asset Inventory carries it is
    unverified; the fallback is a field set only on a secondary. A resource carrying neither
    is counted, which is how it was counted before.
    """
    config = data.get(role_field) or {}
    if config.get(role_key):
        return config[role_key] == "SECONDARY"
    return bool(config.get(fallback))


def is_primary_database(asset):
    """False for a managed database that is a copy of one already counted (footnote 23).

    Cloud SQL replicas, AlloyDB secondary clusters, and the secondary of a Memorystore for
    Redis Cluster or Memorystore for Valkey cross-region pair each exist as an asset of
    their own. Spanner, Bigtable and Firestore keep their replicas inside one asset, and
    Memorystore for Redis instances keep theirs inside the instance, so they need nothing.
    """
    data = asset_data(asset)
    if asset.asset_type == "sqladmin.googleapis.com/Instance":
        return is_primary_sql_instance(asset)
    if asset.asset_type == "alloydb.googleapis.com/Cluster":
        if data.get("clusterType"):
            return data.get("clusterType") != "SECONDARY"
        return not data.get("secondaryConfig")
    if asset.asset_type == "redis.googleapis.com/Cluster":
        return not _is_secondary(data, "crossClusterReplicationConfig", "clusterRole",
                                 "primaryCluster")
    if asset.asset_type == "memorystore.googleapis.com/Instance":
        return not _is_secondary(data, "crossInstanceReplicationConfig", "instanceRole",
                                 "primaryInstance")
    return True


BIGQUERY_DATASET = "bigquery.googleapis.com/Dataset"


def warehouse_key(asset, tally=True):
    """The location that identifies a BigQuery dataset's warehouse within its project.

    The billed unit is the warehouse, not the dataset (footnote 10). A dataset is a schema-level
    namespace, and customers create them per source, per team or per export, so counting them
    prices how a warehouse is organised rather than how many there are. The project is where
    BigQuery's compute, quotas and billing attach, and location is a hard boundary inside it:
    a query runs in one location and reads only data stored there. So one warehouse is one
    (project, location) pair, the same footing as a Redshift cluster in one region.

    Location is lower-cased, since BigQuery reports multi-regions as "US"/"EU" and regions in
    lower case. The US multi-region and us-central1 stay distinct, as they are to BigQuery.

    Datasets with no readable location share one "unknown" warehouse per project rather than
    being dropped. That direction is loud: if the field ever goes missing, every project with
    datasets still counts at least once, and the canary says the split was not possible.
    """
    location = getattr(getattr(asset, "resource", None), "location", None) \
        or asset_data(asset).get("location") or ""
    if tally:
        # Tallied on the present-day pass only, like the instance measures, so that a smoothed
        # run does not multiply them by the number of samples.
        DIAG.bump("bigquery_datasets_seen")
        # Whether Cloud Asset Inventory returns hidden ("_"-prefixed) datasets is undocumented,
        # and only field runs can say. Recorded as an explicit zero, because anonymous
        # query-result datasets exist in nearly every project that has run a query: many
        # datasets and zero hidden ones is itself the answer.
        DIAG.bump("bigquery_hidden_datasets_seen", by=0)
        if asset.name.rsplit("/datasets/", 1)[-1].startswith("_"):
            DIAG.bump("bigquery_hidden_datasets_seen")
        if not location:
            DIAG.bump("bigquery_datasets_without_location")
    return location.lower() or "unknown"


UNATTRIBUTED = "unattributed"


def project_of(asset):
    """Return the project *number* an asset belongs to, or None.

    Cloud Asset Inventory populates `ancestors` with the full hierarchy
    (projects/<number>, folders/<id>, organizations/<id>) at no extra permission cost;
    `resource.parent` carries the same project number and serves as a fallback.

    Numbers stay the key throughout, because every asset carries one for free. The project
    IDs people recognise are looked up once per run by project_directory() and attached to
    the output alongside them.
    """
    for ancestor in (getattr(asset, "ancestors", None) or []):
        if ancestor.startswith("projects/"):
            return ancestor.split("/", 1)[1]
    parent = getattr(getattr(asset, "resource", None), "parent", "") or ""
    idx = parent.find("/projects/")
    if idx != -1:
        return parent[idx + len("/projects/"):].split("/")[0]
    return None


PROJECT_ASSET_TYPE = "cloudresourcemanager.googleapis.com/Project"


def project_directory(client, parents):
    """{project number: project ID} for every project under the scanned scopes.

    Read from Cloud Asset Inventory's own Project asset, so roles/cloudasset.viewer covers it
    and no Resource Manager role is needed. That extra role was the reason the first pass
    used numbers. It also lists projects that own nothing counted, which lets the output
    tell an empty project apart from one outside the scope.

    Read at the present instant only. A project ID never changes, so history adds nothing,
    and a project deleted during the window simply stays unresolved and is shown by number.
    A failure here is not fatal: the run warns and falls back to numbers, which is what it
    reported before this lookup existed.
    """
    directory = {}
    for parent in parents:
        request = asset_v1.ListAssetsRequest(
            parent=parent,
            asset_types=[PROJECT_ASSET_TYPE],
            content_type=ContentType.RESOURCE,
            page_size=CAI_PAGE_SIZE,
        )
        try:
            for asset in client.list_assets(request, retry=CAI_RETRY):
                # The asset name ends in the project number whichever Resource Manager
                # version the resource data follows; both versions carry projectId.
                number = asset.name.rsplit("/", 1)[-1]
                project_id = asset_data(asset).get("projectId")
                if number and project_id:
                    directory[number] = project_id
        except Exception as e:
            DIAG.record_denial("project_ids", type(e).__name__)
            print(f"  WARNING: could not read project IDs under {parent}, so projects there are "
                  f"shown by number only: {e}", file=sys.stderr)
    return directory


def project_label(number, directory):
    """'acme-prod (482913756201)' when the ID is known, else the number alone."""
    project_id = directory.get(number)
    return f"{project_id} ({number})" if project_id else number


def count_assets(client, parent, asset_types, region_filter=None, dry_run=False, asset_filter=None,
                 read_time=None, tally=True):
    """Return {(project_number, asset_type): count} from the paginated list_assets response.

    BigQuery datasets count once per distinct (project, location), not once per asset; see
    warehouse_key. `tally` is False on historical samples so per-run measures are not
    multiplied by the sample count.
    """
    if dry_run:
        for at in asset_types:
            print(f"  [dry-run] list_assets parent={parent} asset_types=[{at}]")
        return {}

    counts = {}
    warehouses = set()
    request = asset_v1.ListAssetsRequest(
        parent=parent,
        asset_types=asset_types,
        content_type=ContentType.RESOURCE,
        page_size=CAI_PAGE_SIZE,
        **({"read_time": read_time} if read_time else {}),
    )

    for asset in client.list_assets(request, retry=CAI_RETRY):
        # Post-filter by location if requested
        if region_filter:
            location = getattr(getattr(asset, "resource", None), "location", None) or ""
            if location != region_filter and not location.startswith(region_filter + "-"):
                continue

        if asset_filter and not asset_filter(asset):
            continue

        key = (project_of(asset) or UNATTRIBUTED, asset.asset_type)
        if asset.asset_type == BIGQUERY_DATASET:
            warehouse = (key, warehouse_key(asset, tally=tally))
            if warehouse in warehouses:
                continue
            warehouses.add(warehouse)
            if tally:
                DIAG.bump("bigquery_warehouses_counted")
        counts[key] = counts.get(key, 0) + 1

    return counts


def probe_asset_history(client, parent, read_time):
    """Can Cloud Asset Inventory answer for a past instant under this parent?

    A refusal and an empty window are different answers and must not be conflated. CAI keeps
    35 days, so a 30-day window is inside retention -- but a recently created project
    genuinely has no history there, and that is indistinguishable from an instrument that
    will not answer. The ambiguous case returns UNKNOWN so it falls to the floor rung and
    shows up in the canary rather than quietly producing a smoothed number from one sample.
    """
    try:
        request = asset_v1.ListAssetsRequest(
            parent=parent,
            asset_types=["compute.googleapis.com/Instance"],
            content_type=ContentType.RESOURCE,
            read_time=read_time,
            page_size=1,
        )
        # Retried too: a quota refusal here would otherwise read as "no history" and quietly
        # drop the whole run to point-in-time.
        first = next(iter(client.list_assets(request, retry=CAI_RETRY)), None)
    except Exception as e:
        DIAG.record_denial("read_time", type(e).__name__)
        return PROBE_UNAVAILABLE
    return PROBE_AVAILABLE if first is not None else PROBE_UNKNOWN


def resolve_sample_times(client, parents, args):
    """Decide which instants to enumerate, and under which rung.

    Returns (read_times, rung, points). A read_time of None means "now", which is the floor
    rung and the behaviour this script had before the ladder.

    Availability is resolved across all parents together and all-or-nothing. A run that
    smoothed the parents it could and snapshotted the rest would put a blended number in a
    single row, which is the least explainable thing that can appear on a sizing artifact.
    """
    if args.dry_run or args.no_smoothing:
        return [None], RUNG_POINT_IN_TIME, None

    offsets = sample_offsets(WINDOW_DAYS, args.sample_every)
    if not sample_is_weekday_balanced(args.sample_every, len(offsets)):
        print(
            f"WARNING: --sample-every {args.sample_every} gives {len(offsets)} samples that do "
            f"not cover the week evenly, so the average will be biased toward the weekdays it "
            f"lands on; {SAMPLE_STRIDE_DAYS} is the balanced stride for a {WINDOW_DAYS}-day window",
            file=sys.stderr,
        )

    now = datetime.now(timezone.utc)
    oldest = now - timedelta(days=max(offsets))
    verdicts = [probe_asset_history(client, parent, oldest) for parent in parents]
    for verdict in verdicts:
        if verdict == PROBE_UNKNOWN:
            DIAG.bump("ladder_probes_unknown")

    if all(v == PROBE_AVAILABLE for v in verdicts):
        return ([now - timedelta(days=o) for o in offsets], RUNG_ASSET_HISTORY, len(offsets))
    return [None], RUNG_POINT_IN_TIME, None


def average_samples(samples):
    """Mean count per (project, asset type) across the sampled instants.

    A key absent from one sample counts as zero there rather than as missing. The resource
    genuinely did not exist at that instant, and skipping the sample instead would bias every
    average upward by ignoring exactly the days a resource was gone.
    """
    if len(samples) == 1:
        return dict(samples[0])
    totals = {}
    for sample in samples:
        for key, value in sample.items():
            totals[key] = totals.get(key, 0.0) + value
    return {key: value / len(samples) for key, value in totals.items()}


def shape_breakdown(counts, asset_types, projects=None):
    """Collapse {(project, asset_type): count} into {asset_type: count}.

    Restricted to `projects` when given, so the same helper builds both the run totals
    and each individual project's block.
    """
    breakdown = {at: 0 for at in asset_types}
    for (project, at), cnt in counts.items():
        if projects is not None and project not in projects:
            continue
        if at in breakdown:
            breakdown[at] += cnt
    # Averaging across samples yields fractions; a count is whole resources. Rounding happens
    # once, here, so that per-type rounding never drifts away from the category total.
    return {at: int(round(v)) for at, v in breakdown.items()}


def _format_table(results, col1):
    """Render the Category | Count | Time Range | Method table.

    Resource types are deliberately absent: they are the widest column by far, they are
    already defined in resource-type-mapping.md, and they stay in the JSON for anything that
    needs them. Window and instrument are two columns rather than one string because they
    sort and scan separately.
    """
    c3 = max([len(str(r.get("time_range", ""))) for r in results] + [len("Time Range")])
    c4 = max([len(str(r.get("method", ""))) for r in results] + [len("Method")])
    lines = [
        f"{'Category':<{col1}} | {'Count':>6} | {'Time Range':<{c3}} | {'Method':<{c4}}",
        f"{'-' * col1}-|{'-' * 8}|{'-' * (c3 + 2)}|{'-' * (c4 + 1)}",
    ]
    for r in results:
        lines.append(
            f"{r['category']:<{col1}} | {r['count']:>6} | "
            f"{str(r.get('time_range', '')):<{c3}} | {str(r.get('method', '')):<{c4}}"
        )
    # Trailing padding is stripped: these lines get copy-pasted through several hands when a
    # file cannot be retrieved from the customer environment.
    return [line.rstrip() for line in lines]


def evaluate_canaries(totals, inaccessible):
    """Assert the things whose failure mode is a plausible-looking number rather than an error.

    Every check here guards a filter or a join that quietly returns an acceptable-looking
    count when it breaks, which is why they run on every invocation instead of behind a flag.
    """
    by_cat = {r["category"]: r for r in totals}
    vm = by_cat.get("VM", {}).get("count", 0)
    disks = by_cat.get("Non-OS Disk", {}).get("count", 0)
    functions = by_cat.get("Serverless Function", {}).get("count", 0)
    stopped = DIAG.measures.get("instances_stopped", 0)

    unmeasured = {at for r in totals for at, v in r["breakdown"].items() if v is None}
    DIAG.canary(
        "resource_types_all_measured", not unmeasured, len(unmeasured),
        "asset type(s) could not be measured, so their categories are undercounts",
    )
    DIAG.canary(
        "scopes_all_readable", not inaccessible, len(inaccessible),
        "scope(s) could not be read, so the totals are an undercount",
    )

    # The loud-degradation rule made visible. An ambiguous probe means the historical window
    # may have been available and was skipped, which shows up as a quietly worse Method label
    # rather than as an error.
    unknown_probes = DIAG.measures.get("ladder_probes_unknown", 0)
    DIAG.canary(
        "ladder_probes_unambiguous", unknown_probes == 0, unknown_probes,
        "scope(s) returned no assets at the far edge of the window, which is indistinguishable "
        "from having no history there, so the run fell back to a point-in-time count",
    )
    DIAG.canary(
        "boot_disks_identified", not DIAG.measures.get("boot_disk_identification_failed"),
        bool(DIAG.measures.get("boot_disk_identification_failed")),
        "the boot-disk scan failed, so every OS disk is being counted in Non-OS Disk",
    )
    DIAG.canary(
        "instance_status_readable", not DIAG.measures.get("instance_status_unreadable"),
        bool(DIAG.measures.get("instance_status_unreadable")),
        "instance status was unreadable, so stopped instances are counted as running and "
        "their boot disks are still excluded",
    )
    DIAG.canary(
        "sql_replica_shape_readable", not DIAG.measures.get("sql_replica_shape_unreadable"),
        bool(DIAG.measures.get("sql_replica_shape_unreadable")),
        "Cloud SQL read replicas could not be identified and are counted as separate databases",
    )

    # Datasets with no readable location share one "unknown" warehouse per project, which may
    # merge real warehouses or add a spurious one alongside them. Neither shows in the totals.
    # bigquery_datasets_seen over bigquery_warehouses_counted is also the datasets-per-warehouse
    # figure, the number this unit change rests on.
    datasets = DIAG.measures.get("bigquery_datasets_seen", 0)
    no_location = DIAG.measures.get("bigquery_datasets_without_location", 0)
    if not datasets:
        DIAG.canary("bigquery_dataset_locations_readable", None, 0,
                    "no BigQuery datasets present, so nothing to check")
    else:
        DIAG.canary(
            "bigquery_dataset_locations_readable", no_location == 0, no_location,
            "BigQuery dataset(s) carried no location, so the warehouse count could not be "
            "split by location for them",
        )

    # The gen2 double-count guard. A non-firing label is invisible in the totals, so the only
    # evidence is whether anything matched. Caveat for whoever reads this: a customer running
    # only gen1 functions legitimately produces zero matches, because gen1 does not create a
    # Cloud Run service -- so a failure here means "worth looking at", not "certainly broken".
    matched = DIAG.measures.get("run_services_filtered_as_functions", 0)
    seen = DIAG.measures.get("run_services_seen", 0)
    if not seen or not functions:
        DIAG.canary("cloudfunctions_label_firing", None, matched,
                    "no Cloud Run services or no functions present, so nothing to check")
    else:
        DIAG.canary(
            "cloudfunctions_label_firing", matched > 0, matched,
            "functions and Cloud Run services both exist but the goog-managed-by label "
            "matched nothing, so gen2 functions may be counted twice (gen1-only estates "
            "match zero legitimately)",
        )

    # A boot disk is excluded from Non-OS Disk only while its instance is counted, so after
    # footnote 26 the honest denominator is every instance that still exists, running or
    # stopped -- otherwise a mostly-stopped fleet trips this on correct numbers. Roughly one
    # disk per instance is what the boot-disk fallback looks like; the measured fleet-wide
    # figure across 668 customers is about 0.4.
    instances = vm + stopped
    if instances < CANARY_MIN_INSTANCES:
        DIAG.canary(
            "nonos_disks_per_instance", None, None,
            f"fewer than {CANARY_MIN_INSTANCES} instances, so the ratio is not meaningful",
        )
    else:
        ratio = round(disks / instances, 3)
        DIAG.canary(
            "nonos_disks_per_instance", ratio < 1.0, ratio,
            "at least one Non-OS Disk per instance suggests boot disks are no longer being "
            "excluded; the measured fleet-wide figure is about 0.4. nonos_disk_reasons says "
            "which disks make up the count",
        )
        DIAG.measure("stopped_instance_fraction", round(stopped / instances, 3))

    # Always measured, not only when the ratio fails: the reasons are what a reader needs to
    # tell a disk-heavy estate from a leak, and a passing run is the baseline to compare with.
    # When the boot-disk scan failed every boot disk would read as a data disk, so the
    # breakdown is withheld rather than reported wrong; boot_disks_identified says why.
    if DIAG.measures.get("boot_disk_identification_failed"):
        DIAG.measure("nonos_disk_reasons", None)
    else:
        DIAG.measure("nonos_disk_reasons", DIAG.measures.get("nonos_disk_reasons", {}))


# Width of the encoded block's lines. Narrow enough to survive terminal reflow and an
# over-broad mouse selection, which is what actually damages a pasted blob in the field.
ENCODED_OUTPUT_WIDTH = 76


def print_encoded_output(payload):
    """Print the whole JSON as a gzip+base64 block, for environments where no file can be
    retrieved.

    Some customers run this in a cloud shell where getting a written file back out is
    impractical, and the artifact then has to survive a copy/paste chain from the customer to
    a field engineer to the CSE team to Product.

    Gzip is not only about size. JSON compresses roughly tenfold, but the checksum matters
    more: a truncated paste fails loudly on decode instead of yielding a plausible partial
    document. The decode command is printed with it so the customer can read their own data
    before it leaves, which is what makes pasting an opaque blob reasonable to ask for. It
    uses Python rather than base64(1) because GNU wants -d while macOS wants -D, and whoever
    just ran this script demonstrably has Python.
    """
    raw = json.dumps(payload, separators=(",", ":")).encode()
    blob = base64.b64encode(gzip.compress(raw, mtime=0)).decode()
    print("\nEncoded copy of the JSON above: gzip, then base64. Decode it with:")
    print('  python3 -c "import base64,gzip,sys;'
          'print(gzip.decompress(base64.b64decode(sys.stdin.read())).decode())"')
    print(f"--- BEGIN FORTICNAPP RESOURCE COUNTS ({len(raw)} bytes) ---")
    for i in range(0, len(blob), ENCODED_OUTPUT_WIDTH):
        print(blob[i:i + ENCODED_OUTPUT_WIDTH])
    print("--- END FORTICNAPP RESOURCE COUNTS ---")


def print_table(results, heading=None):
    col1 = max([len(r["category"]) for r in results] + [len("Category")])
    if heading:
        print(f"\n=== {heading} ===")
    for line in _format_table(results, col1):
        print(line)


def main():
    parser = argparse.ArgumentParser(description="Count GCP resources per FortiCNAPP billing category.")
    parser.add_argument(
        "--scope", nargs="+", metavar="SCOPE",
        help="One or more asset scopes: projects/PROJECT_ID, folders/FOLDER_ID, or "
             "organizations/ORG_ID. Each is queried separately and the results combined. "
             "Defaults to the active project from GOOGLE_CLOUD_PROJECT or gcloud config.",
    )
    parser.add_argument(
        "--exclude-project", nargs="+", default=[], metavar="PROJECT",
        help="Project(s) to exclude from the counts, by project ID or project number.",
    )
    parser.add_argument("--region", help="Scope to a single GCP region/location (post-fetch filter)")
    parser.add_argument("--output-dir", default=".", help="Directory to write JSON output (default: .)")
    parser.add_argument("--dry-run", action="store_true", help="Print API calls without executing them")
    parser.add_argument(
        "--sample-every", type=int, default=SAMPLE_STRIDE_DAYS, metavar="DAYS",
        help=f"Days between historical samples (default: {SAMPLE_STRIDE_DAYS}). The default is "
             f"chosen so {WINDOW_DAYS // SAMPLE_STRIDE_DAYS + 1} samples cover every day of the "
             f"week exactly once; a stride sharing a factor with 7 (notably 7 itself) lands "
             f"every sample on the same weekday and biases the average.",
    )
    parser.add_argument(
        "--no-smoothing", action="store_true",
        help="Skip the historical window and report a point-in-time count, which is faster "
             "because each sample is a full enumeration of the org.",
    )
    parser.add_argument(
        "--encoded-output", action="store_true",
        help="Also print the JSON as a gzip+base64 block, for environments where the written "
             "file cannot be retrieved (a cloud shell, a locked-down jump host).",
    )
    parser.add_argument(
        "--diagnostics", action="store_true",
        help="Add the heavier diagnostics bundle (failure ledger, environment) to the JSON. "
             "Canaries and measures are always included.",
    )
    args = parser.parse_args()

    # stdout is block-buffered when piped while stderr is not, so a "| tee" transcript would
    # otherwise show the tables and the progress/warning lines badly out of order. That
    # transcript is the field's diagnostic artifact, so it has to read in true sequence.
    sys.stdout.reconfigure(line_buffering=True)
    print(f"FortiCNAPP resource counts (GCP) - script version {script_version()}")
    run_start = time.time()

    valid_prefixes = ("projects/", "folders/", "organizations/")
    if args.scope:
        for scope in args.scope:
            matched = next((p for p in valid_prefixes if scope.startswith(p)), None)
            if not matched or scope == matched:
                print(
                    f"FATAL: invalid --scope {scope!r}; must be projects/PROJECT_ID, "
                    "folders/FOLDER_ID, or organizations/ORG_ID",
                    file=sys.stderr,
                )
                sys.exit(1)
        parents = list(dict.fromkeys(args.scope))
    else:
        project_id = resolve_project_id()
        if not project_id:
            print(
                "FATAL: could not resolve GCP project ID. "
                "Set GOOGLE_CLOUD_PROJECT, configure 'gcloud config set project <id>', "
                "or pass --scope projects/PROJECT_ID.",
                file=sys.stderr,
            )
            sys.exit(1)
        parents = [f"projects/{project_id}"]

    prefixes = {p.split("/", 1)[0] for p in parents}
    mode = prefixes.pop() if len(prefixes) == 1 else "mixed"
    if not args.scope:
        mode = "default"
    requested_exclusions = sorted(set(args.exclude_project or []))

    try:
        client = asset_v1.AssetServiceClient()
    except Exception as e:
        print(f"FATAL: could not initialize Cloud Asset client: {e}", file=sys.stderr)
        sys.exit(1)

    directory = {} if args.dry_run else project_directory(client, parents)
    by_id = {pid: number for number, pid in directory.items()}
    # IDs are translated to numbers here because numbers are what every asset is keyed by.
    # An ID that matches nothing is kept as typed, so the "no counted resources" warning below
    # names it rather than dropping it silently.
    excluded = sorted({by_id.get(x, x) for x in requested_exclusions})

    sample_times, rung, sample_points = resolve_sample_times(client, parents, args)
    if rung == RUNG_ASSET_HISTORY:
        print(f"Smoothing over {WINDOW_DAYS} days: {sample_points} samples every "
              f"{args.sample_every} days, covering every weekday once. Each sample is a full "
              f"enumeration, so this run makes roughly {sample_points}x the API calls.",
              file=sys.stderr)

    # One boot set per sampled instant. Rebuilding it is not optional: which disks were boot
    # disks 30 days ago is not what they are today, and reusing today's set would misfile
    # historical disks on the Non-OS Disk line.
    boot_links_by_time = {}
    if not args.dry_run:
        for idx, read_time in enumerate(sample_times):
            links = set()
            for parent in parents:
                try:
                    links |= boot_disk_links(client, parent, read_time=read_time, tally=(idx == 0))
                except Exception as e:
                    # Fall back to counting every disk rather than failing the category.
                    # Recorded as well as printed: this fallback inflates Non-OS Disk by
                    # roughly one disk per instance, and the number still looks ordinary.
                    DIAG.measure("boot_disk_identification_failed", True)
                    DIAG.record_denial("boot_disk_scan", type(e).__name__)
                    print(f"  WARNING: could not identify boot disks under {parent}, OS disks will be "
                          f"included in Non-OS Disk: {e}", file=sys.stderr)
            boot_links_by_time[read_time] = links

    inaccessible = []
    raw_by_category = {}
    for cat_def in CATEGORIES:
        category = cat_def["category"]
        asset_types = cat_def["asset_types"]

        def filter_for(boot_links, tally, category=category):
            # Rebuilt per sample because the Non-OS Disk filter closes over that instant's
            # boot set; the others are stateless and simply returned again.
            if category == "VM":
                return is_counted_instance
            if category == "Serverless Container":
                return is_standalone_run_service
            if category == "Managed DB (PaaS)":
                return is_primary_database
            if category == "Non-OS Disk":
                keep = (lambda a: is_data_disk(a, boot_links)) if boot_links else None
                if not tally:
                    return keep

                # Present-day pass only, so the reasons describe today's disks rather than an
                # average, and each disk is classified once after the region filter, exactly
                # as the count sees it.
                def keep_and_classify(a):
                    tally_disk_reason(disk_reason(a, boot_links))
                    return keep(a) if keep else True
                return keep_and_classify
            return None

        per_sample = []
        failed = False
        for idx, read_time in enumerate(sample_times):
            asset_filter = filter_for(boot_links_by_time.get(read_time, set()), idx == 0)
            sample_counts = {}
            for parent in parents:
                try:
                    for key, cnt in count_assets(
                        client, parent, asset_types, region_filter=args.region,
                        dry_run=args.dry_run, asset_filter=asset_filter, read_time=read_time,
                        tally=(idx == 0),
                    ).items():
                        sample_counts[key] = sample_counts.get(key, 0) + cnt
                except Exception as e:
                    DIAG.record_denial(parent.split("/", 1)[0], type(e).__name__)
                    print(f"  WARNING: {category} under {parent}: {e}", file=sys.stderr)
                    failed = True
                    if parent not in inaccessible:
                        inaccessible.append(parent)
            per_sample.append(sample_counts)
        raw_by_category[category] = (average_samples(per_sample), failed)

    discovered = sorted({p for counts, _ in raw_by_category.values() for p, _ in counts})
    not_in_scope = [p for p in excluded if p not in discovered]
    if not_in_scope:
        print(
            f"WARNING: --exclude-project listed {len(not_in_scope)} project(s) with no counted "
            f"resources anyway: {', '.join(project_label(p, directory) for p in not_in_scope)}",
            file=sys.stderr,
        )
    projects = [p for p in discovered if p not in set(excluded)]
    dropped = [p for p in discovered if p in set(excluded)]
    if dropped:
        print(f"Excluding {len(dropped)} project(s) on request: "
              f"{', '.join(project_label(p, directory) for p in dropped)}", file=sys.stderr)
    # Projects in scope that own nothing counted. Before the directory these were
    # indistinguishable from projects outside the scope.
    empty = sorted(set(directory) - set(discovered) - set(excluded))
    if empty:
        print(f"{len(empty)} project(s) in scope have no counted resources: "
              f"{', '.join(project_label(p, directory) for p in empty)}", file=sys.stderr)
    unresolved = [p for p in projects if p != UNATTRIBUTED and p not in directory]
    DIAG.measure("project_ids_resolved", len(projects) - len(unresolved)
                 - (1 if UNATTRIBUTED in projects else 0))
    DIAG.measure("project_ids_unresolved", len(unresolved))
    if UNATTRIBUTED in projects:
        print(
            "WARNING: some assets carry no project ancestor and are grouped under "
            f"'{UNATTRIBUTED}'; they are included in the totals",
            file=sys.stderr,
        )

    # Cloud Asset Inventory answers for every asset type or none, so the rung resolves once
    # per run here rather than per type -- but it is still recorded per type, because that is
    # the shape the field data has to arrive in to be comparable across clouds.
    _time_range = TIME_RANGE_WINDOW if rung == RUNG_ASSET_HISTORY else TIME_RANGE_NOW
    provenance_by_type = {
        at: Measurement(None, rung=rung, time_range=_time_range, points=sample_points).provenance()
        for cat_def in CATEGORIES for at in cat_def["asset_types"]
    }
    for at in provenance_by_type:
        DIAG.rung_trace(at, rung, PROBE_AVAILABLE if rung == RUNG_ASSET_HISTORY else PROBE_UNAVAILABLE)

    def categories_for(subset):
        out = []
        for cat_def in CATEGORIES:
            counts, failed = raw_by_category[cat_def["category"]]
            asset_types = cat_def["asset_types"]
            breakdown = ({at: None for at in asset_types} if failed
                         else shape_breakdown(counts, asset_types, subset))
            # Provenance is per asset type so that a rung which only speaks for some types
            # labels only those; the category label is then derived rather than asserted.
            provenance = {at: provenance_by_type[at] for at in asset_types}
            time_range, method = category_provenance(provenance, breakdown)
            out.append({
                "category": cat_def["category"],
                "count": sum(v for v in breakdown.values() if v is not None),
                "time_range": time_range,
                "method": method,
                "resource_types": asset_types,
                "breakdown": breakdown,
                "provenance": provenance,
            })
        return out

    totals = categories_for(set(projects))
    per_project = {p: categories_for({p}) for p in projects}

    heading = f"TOTALS ({len(projects)} project(s))" if len(projects) != 1 else None
    print_table(totals, heading=heading)
    if len(projects) > 1:
        for p in projects:
            print_table(per_project[p], heading=f"project {project_label(p, directory)}")

    DIAG.measure("run_seconds", round(time.time() - run_start, 1))
    evaluate_canaries(totals, inaccessible)
    DIAG.report()

    # Write JSON
    output = {
        "csp": "gcp",
        "scope": {
            "mode": mode,
            "requested": parents,
            "scanned": projects,
            "excluded": dropped,
            "inaccessible": inaccessible,
            # Projects under the scope that own nothing counted, by number.
            "empty": empty,
            # Number -> ID for every project the run could name. A scanned project missing
            # from here was not resolvable, typically because it was deleted in the window.
            "project_ids": {p: directory[p] for p in sorted(directory)
                            if p in set(projects) | set(dropped) | set(empty)},
        },
        "script_version": script_version(),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "diagnostics": DIAG.to_json(verbose=args.diagnostics),
        "totals": totals,
        "accounts": [
            {"account_id": p, "project_id": directory.get(p), "categories": per_project[p]}
            for p in projects
        ],
    }
    out_path = Path(args.output_dir) / "gcp_resource_counts.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nJSON written to {out_path}")

    if args.encoded_output:
        print_encoded_output(output)


if __name__ == "__main__":
    main()
