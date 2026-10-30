#!/usr/bin/env python3
"""Count Azure resources per FortiCNAPP billing category using Resource Graph."""

import argparse
import base64
import gzip
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from azure.core import PipelineClient
from azure.core.pipeline.policies import BearerTokenCredentialPolicy, RetryPolicy
from azure.core.rest import HttpRequest
from azure.identity import DefaultAzureCredential
from azure.mgmt.resourcegraph import ResourceGraphClient
from azure.mgmt.resourcegraph.models import QueryRequest, QueryRequestOptions


# Bumped by hand whenever a change can move a count. A count is only interpretable
# alongside the version that produced it: the counting rules change as mapping decisions
# land, so without this there is no way to tell a change in a customer's estate from a
# change in this script.
SCRIPT_VERSION = "2026.10.26.3"


def script_version():
    """SCRIPT_VERSION, plus the live git SHA when the script is running inside its own clone.

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
# next best, labelling every count with the instrument that produced it.
#
# Azure's historical instrument is unlike the other two clouds'. AWS Cost Explorer hands back a
# series and GCP's read_time hands back a past instant; Azure hands back a stream of *changes*,
# and the count at a past moment has to be reconstructed by walking those changes backwards
# from the present. Two consequences shape everything below.
#
# First, the window is 14 days, not 30. Resource Graph retains change data for 14 days, and no
# query can reach past it -- so this rung can never satisfy the product's monthly metric. It is
# deliberately a lesser rung: a labelled 14-day average still beats an unlabelled snapshot.
#
# Second, Create and Delete records carry no properties. Microsoft documents this directly --
# "snapshots are not currently supported for deleted resources", and changesCount is 0 for both
# Create and Delete because in one case every property changed and in the other none remain. So
# a change record says *that* a resource of some type appeared or vanished, never what its
# properties were. Any count whose rule inspects properties therefore cannot be reconstructed,
# which is what CHANGE_RECONSTRUCTABLE below is about.

WINDOW_DAYS = 14

TIME_RANGE_NOW = "now"
TIME_RANGE_WINDOW = f"{WINDOW_DAYS}d"

RUNG_RESOURCE_CHANGES = "resource-changes"
RUNG_POINT_IN_TIME = "point-in-time"

METHOD_MIXED = "mixed"

PROBE_AVAILABLE = "available"
PROBE_UNAVAILABLE = "unavailable"
PROBE_UNKNOWN = "unknown"


class Measurement:
    """One resource type's count, with how it was obtained."""

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
    """Derive one category's Time Range and Method from the types that composed it."""
    used = [p for rt, p in provenance.items() if breakdown.get(rt)]
    if not used:
        used = [p for rt, p in provenance.items() if breakdown.get(rt) is not None]
    if not used:
        return TIME_RANGE_NOW, RUNG_POINT_IN_TIME
    rungs = {p["rung"] for p in used}
    ranges = {p["time_range"] for p in used}
    return (ranges.pop() if len(ranges) == 1 else METHOD_MIXED,
            rungs.pop() if len(rungs) == 1 else METHOD_MIXED)


def category_is_reconstructable(cat_def):
    """True when every query behind a category is a plain type match on the Resources table.

    A change record identifies a resource only by type, so it can stand in for a count whose
    rule is "resources of these types" and for nothing else. Two categories are excluded by
    this and it is worth being explicit about which and why:

      VM -- filtered on power state (and joined against AVD session hosts, and partly read from
      the ComputeResources table). A Delete record cannot say whether the VM was running, so
      reconstruction would drift from the definition rather than from the data.

      Non-OS Disk -- filtered on disk state and joined to stopped VMs under footnote 26. Same
      problem: the change feed knows a disk went away, not whether it was an OS disk.

    Bucket Storage is excluded for a narrower reason: one of its queries reads
    RecoveryServicesResources, which resourcechanges does not cover, so half the category would
    smooth and half would not.
    """
    queries = _queries_for(cat_def)
    return bool(queries) and all(
        table == "Resources" and not pipeline for table, _filter, pipeline in queries
    )


# Minimum VM count before the disk-ratio canary is worth evaluating. Below this a demo-sized
# environment produces ratios that swing wildly on one disk, which is how a canary earns a
# reputation for crying wolf and gets ignored.
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
    codes only.** No resource names, resource IDs, subscription GUIDs or location detail that
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

    def rung_trace(self, resource_type, rung, verdict):
        """Tally one probe verdict by (resource type, rung, verdict). Counts only, never which
        subscription: this block travels to Product through several pairs of hands."""
        key = f"{resource_type}/{rung}/{verdict}"
        self.rungs[key] = self.rungs.get(key, 0) + 1

    def record_denial(self, where, code):
        """Tally one failure by (where, exception type). Types only -- never messages, which
        quote resource IDs and subscription GUIDs."""
        key = f"{where}/{code}"
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
                "azure-mgmt-resourcegraph": _pkg_version("azure-mgmt-resourcegraph"),
                "azure-identity": _pkg_version("azure-identity"),
                "script_version": script_version(),
            }
        return out


DIAG = Diagnostics()


def _kql_filter_from_types(resource_types):
    """Derive a KQL type filter from a list of ARM resource type strings."""
    quoted = ", ".join(f"'{rt}'" for rt in resource_types)
    return f"type in~ ({quoted})"


# AVD session hosts are ordinary VMs (or scale set instances) that happen to be
# registered to a host pool, so they must be excluded explicitly. Session hosts live
# in the DesktopVirtualizationResources table, and Resource Graph has no leftanti
# join, so this is a leftouter join filtered to the non-matching rows.
_AVD_SESSION_HOST_EXCLUSION = (
    " | extend vmId = tolower(id)"
    " | join kind=leftouter ("
    "DesktopVirtualizationResources"
    " | where type =~ 'microsoft.desktopvirtualization/hostpools/sessionhosts'"
    " | extend vmId = tolower(tostring(properties.resourceId))"
    " | project vmId, isSessionHost = 1"
    ") on vmId"
    " | where isnull(isSessionHost)"
)

# Every Azure SQL logical server has a system database named master. Database names are unique
# per server, so no customer database can carry that name, and excluding it by name can never
# drop a real database. Whether Resource Graph returns it among
# microsoft.sql/servers/databases was never settled from the docs, so the exclusion is applied
# regardless, and probe_exclusions() counts what it removed (footnote 7, OI-14).
_SQL_MASTER_EXCLUSION = "not(type =~ 'microsoft.sql/servers/databases' and name =~ 'master')"
# The change feed has no name column, so there the same exclusion matches on the resource ID.
_SQL_MASTER_CHANGE_EXCLUSION = (
    "not(targetType == 'microsoft.sql/servers/databases'"
    " and targetId endswith '/databases/master')"
)


# Copies of a managed database that Resource Graph returns as resources of their own, each
# marked on the copy itself. A database counts once however many copies back it (footnote 23):
#   SQL Database      - properties.secondaryType is set (Geo, Named or Standby) on a geo-replica,
#                       a failover-group secondary, a Hyperscale named replica or a standby
#                       replica, and absent on a primary. Hyperscale HA replicas are not
#                       resources at all.
#   PostgreSQL flex   - replicationRole or replica.role is AsyncReplica or GeoAsyncReplica, and
#                       sourceServerResourceId is returned only on a read replica.
#   MySQL flex        - replicationRole is Replica.
#   Single servers    - MySQL, PostgreSQL and MariaDB: replicationRole Replica, or masterServerId
#                       set ("" on a standalone).
#   Cosmos DB for PG  - sourceResourceId is set only on a read-replica cluster.
# Not handled, because the role lives on a sub-resource Resource Graph does not index: SQL
# Managed Instance failover groups, Azure Cache for Redis geo-replication, Redis Enterprise
# active geo-replication and Data Explorer follower clusters. Those still count per copy.
# Every marker is read straight off the resource, so a missing property keeps the copy counted:
# the failure is an over-count, never a silent drop.
_MANAGED_DB_COPY = (
    "(type =~ 'microsoft.sql/servers/databases'"
    " and isnotempty(tostring(properties.secondaryType)))"
    " or (type in~ ('microsoft.dbforpostgresql/flexibleservers', 'microsoft.dbformysql/flexibleservers',"
    " 'microsoft.dbforpostgresql/servers', 'microsoft.dbformysql/servers',"
    " 'microsoft.dbformariadb/servers')"
    " and (tostring(properties.replicationRole) in~ ('Replica', 'AsyncReplica', 'GeoAsyncReplica')"
    " or tostring(properties.replica.role) in~ ('AsyncReplica', 'GeoAsyncReplica')"
    " or (type =~ 'microsoft.dbforpostgresql/flexibleservers'"
    " and isnotempty(tostring(properties.sourceServerResourceId)))"
    " or isnotempty(tostring(properties.masterServerId))))"
    " or (type =~ 'microsoft.dbforpostgresql/servergroupsv2'"
    " and isnotempty(tostring(properties.sourceResourceId)))"
)
_MANAGED_DB_EXCLUSION = f"{_SQL_MASTER_EXCLUSION} and not({_MANAGED_DB_COPY})"
# The change feed carries no properties, so a copy's own Create and Delete records cannot be
# recognised there. Those of copies that still exist are dropped by ID instead; a copy deleted
# inside the window cannot be recognised at all, and inflates the days before its deletion by one.
_MANAGED_DB_COPY_CHANGE_EXCLUSION = (
    " | join kind=leftouter (Resources"
    f" | where {_MANAGED_DB_COPY}"
    " | project targetId = tolower(id), _isCopy = 1) on targetId"
    " | where isnull(_isCopy)"
)


# Power states in which a VM leaves the VM line, its boot disk being re-admitted to Non-OS
# Disk instead, so it counts once at the disk weight (resource-type-mapping.md footnote 26).
# Both count as stopped: under a resource count -- unlike the old vCPU consumption model --
# what matters is that the machine is not running and its disks persist. Only 'deallocated'
# releases compute billing, which is why the old model distinguished them and this one does
# not. Transitional states (starting, stopping, deallocating) are absent, so they stay
# counted as still-a-VM.
_STOPPED_POWER_STATES = "('PowerState/stopped', 'PowerState/deallocated')"

# properties.extended.instanceView.powerState.code is the documented Resource Graph path and
# is what Microsoft's own published sample queries use for microsoft.compute/virtualmachines.
# The un-extended path is read as a fallback because NO published sample covers power state on
# ComputeResources scale-set instance rows, so their shape is unverified -- see the canary
# note below.
#
# An unreadable power state leaves the VM counted. That keeps a shape surprise as a visible
# over-count on the VM row rather than instances vanishing from the VM and disk lines at once,
# and it means the worst case on ComputeResources is exactly today's behaviour.
_POWER_STATE_EXTEND = (
    " | extend _ps1 = tostring(properties.extended.instanceView.powerState.code)"
    " | extend _ps2 = tostring(properties.instanceView.powerState.code)"
    " | extend _powerState = iff(isempty(_ps1), _ps2, _ps1)"
)
_RUNNING_VM_ONLY = _POWER_STATE_EXTEND + f" | where _powerState !in~ {_STOPPED_POWER_STATES}"

# CANARY TARGET: the fraction of VM rows whose power state reads empty. Non-zero on the
# Resources table means the documented path moved; 100% on ComputeResources means scale-set
# instances carry it somewhere else and stopped AKS nodes are still being counted as running.

# The other half of footnote 26: an OS disk is re-admitted when the VM line is not accounting
# for it. Azure is structurally unlike the other two clouds here -- it has never joined disks
# to VMs at all, excluding OS disks purely on the disk's own properties.osType -- so this join
# is new rather than a filter change.
#
# Deliberately narrow. The disk is re-admitted only when managedBy resolves to a plain
# microsoft.compute/virtualmachines row in a stopped state. Everything else keeps today's
# behaviour, which matters in two cases that look similar but are not:
#   - Scale-set-owned OS disks: managedBy points at a VMSS instance, which is not in the
#     right-hand set, so no match and no re-admission. Correct, because that instance IS
#     counted on the VM line via ComputeResources -- re-admitting would double-count every
#     AKS node's OS disk.
#   - Unattached OS disks: no owner, so the join does not match. They are counted anyway, by
#     the Unattached disk-state route below, as AWS and GCP count a detached former boot disk.
# Disk states in which an OS disk is not serving a VM that the VM line counts, taken straight
# from the documented DiskState enum:
#   Unattached      - "not being used and can be attached to a VM"
#   Reserved        - "attached to a stopped-deallocated VM"
#   Frozen          - "attached to a VM which is in hibernated state"
#   ActiveSASFrozen - hibernated, with an active SAS URI
# This is Azure stating the relationship on the disk itself, which is more direct than
# reconstructing it through managedBy. It does NOT replace the power-state join: Reserved is
# documented specifically as stop-*deallocated*, so a VM merely stopped while still allocated
# keeps an Attached disk and is only caught by the join. The two are a union, not alternatives.
_UNOWNED_DISK_STATES = "('Unattached', 'Reserved', 'Frozen', 'ActiveSASFrozen')"

# Shared by the count and by probe_nonos_disk_reasons, so the probe classifies exactly the
# disks the count admits.
_DISK_OWNER_JOIN = (
    " | extend _osType = tostring(properties.osType)"
    " | extend _ownerId = tolower(tostring(managedBy))"
    " | extend _diskState = tostring(properties.diskState)"
    " | join kind=leftouter ("
    "Resources"
    " | where type =~ 'microsoft.compute/virtualmachines'"
    " | extend _vmPs1 = tostring(properties.extended.instanceView.powerState.code)"
    " | extend _vmPs2 = tostring(properties.instanceView.powerState.code)"
    " | project _vmId = tolower(id),"
    " _vmPower = iff(isempty(_vmPs1), _vmPs2, _vmPs1)"
    ") on $left._ownerId == $right._vmId"
)

_STOPPED_VM_BOOT_DISK_READMISSION = (
    _DISK_OWNER_JOIN
    + " | where isempty(_osType)"
    f" or _vmPower in~ {_STOPPED_POWER_STATES}"
    f" or (_diskState in~ {_UNOWNED_DISK_STATES}"
    " and _ownerId !contains '/virtualmachinescalesets/')"
)


def _queries_for(cat_def):
    """Normalize a category into a list of (table, kql_filter, pipeline) tuples."""
    if "queries" in cat_def:
        return [
            (q.get("table", "Resources"), q["filter"], q.get("pipeline", ""))
            for q in cat_def["queries"]
        ]
    kql_filter = cat_def.get("kql_filter") or _kql_filter_from_types(cat_def["resource_types"])
    if cat_def.get("exclusion"):
        kql_filter = f"({kql_filter}) and {cat_def['exclusion']}"
    return [("Resources", kql_filter, "")]


CATEGORIES = [
    {
        "category": "VM",
        "resource_types": [
            "microsoft.compute/virtualmachines",
            "microsoft.compute/virtualmachinescalesets/virtualmachines",
            # Counted by count_ml_computes(), not by Resource Graph, which does not index them.
            "microsoft.machinelearningservices/workspaces/computes",
        ],
        "queries": [
            {
                "table": "Resources",
                "filter": "type =~ 'microsoft.compute/virtualmachines'",
                "pipeline": _AVD_SESSION_HOST_EXCLUSION + _RUNNING_VM_ONLY,
            },
            {
                # Uniform-mode scale set instances (which is what AKS node pools use)
                # exist ONLY in ComputeResources — the Resources table does not carry
                # them, so querying Resources for this type silently returns zero.
                # Flexible-mode instances surface in Resources as
                # microsoft.compute/virtualmachines and are counted by the query above,
                # so the two tables are disjoint and this does not double-count.
                "table": "ComputeResources",
                "filter": "type =~ 'microsoft.compute/virtualmachinescalesets/virtualmachines'",
                "pipeline": _AVD_SESSION_HOST_EXCLUSION + _RUNNING_VM_ONLY,
            },
        ],
    },
    {
        "category": "VM Image",
        # galleries/images is the image *definition* (metadata container for versions);
        # counting it alongside its versions double-counts the same image.
        "resource_types": [
            "microsoft.compute/images",
            "microsoft.compute/galleries/images/versions",
        ],
    },
    {
        "category": "Serverless Container",
        # Container Apps Jobs count once per job, not per execution. Executions are not
        # Resource Graph resources in any case (footnote 3).
        "resource_types": ["microsoft.app/containerapps", "microsoft.containerinstance/containergroups",
                           "microsoft.app/jobs"],
    },
    {
        "category": "Container Image",
        "resource_types": ["microsoft.containerregistry/registries"],
    },
    {
        "category": "Serverless Function",
        "resource_types": ["microsoft.web/sites"],
        # kind filter cannot be derived from resource_types alone
        "kql_filter": "type =~ 'microsoft.web/sites' and kind contains 'functionapp'",
    },
    {
        "category": "Managed DB (PaaS)",
        "resource_types": [
            "microsoft.sql/servers/databases",
            "microsoft.sql/managedinstances",
            "microsoft.documentdb/databaseaccounts",
            "microsoft.dbformysql/flexibleservers",
            "microsoft.dbformysql/servers",
            "microsoft.dbforpostgresql/flexibleservers",
            "microsoft.dbforpostgresql/servers",
            "microsoft.cache/redis",
            # Enterprise tiers and Azure Managed Redis carry their own type; the
            # /databases sub-resource is not counted (the cluster represents it)
            "microsoft.cache/redisenterprise",
            "microsoft.dbformariadb/servers",
            "microsoft.kusto/clusters",
            # Cosmos DB for PostgreSQL: one cluster (coordinator plus workers) counts once;
            # its nodes are properties, not resources.
            "microsoft.dbforpostgresql/servergroupsv2",
        ],
        # Still a plain Resources match with no pipeline, so the change feed can still
        # smooth this row; fetch_change_events applies the same exclusions.
        "exclusion": _MANAGED_DB_EXCLUSION,
    },
    {
        "category": "Data Warehouse",
        # A Fabric capacity is a presence marker, not a warehouse count: the warehouses
        # themselves are reachable only through the Fabric REST API (footnote 8).
        "resource_types": ["microsoft.databricks/workspaces", "microsoft.synapse/workspaces",
                           "microsoft.fabric/capacities"],
    },
    {
        "category": "Bucket Storage",
        # Blob containers, not storage accounts: a container is the unit that matches an S3 or
        # GCS bucket. Counted by count_blob_containers() through the Storage resource provider,
        # because Resource Graph does not index containers (footnote 21).
        "resource_types": [
            "microsoft.storage/storageaccounts/blobservices/containers",
            "microsoft.recoveryservices/vaults",
            "microsoft.dataprotection/backupvaults",
        ],
        "queries": [
            {
                # Both vault types are in RecoveryServicesResources, not Resources —
                # querying the default table returns zero for them.
                "table": "RecoveryServicesResources",
                "filter": (
                    "type in~ ('microsoft.recoveryservices/vaults', "
                    "'microsoft.dataprotection/backupvaults')"
                ),
            },
        ],
    },
    {
        "category": "Non-OS Disk",
        # A stopped Azure ML compute instance keeps its disk and runs no VM, so it counts here,
        # the same as a stopped VM (footnote 26). The type name is ours, not an ARM type.
        "resource_types": ["microsoft.compute/disks",
                           "microsoft.machinelearningservices/workspaces/computes (stopped)"],
        # OS disks carry properties.osType; data disks leave it null. The test moved out of
        # the where clause and into the pipeline because the join has to happen first: a
        # stopped VM's OS disk is re-admitted here (footnote 26).
        "queries": [
            {
                "table": "Resources",
                "filter": "type =~ 'microsoft.compute/disks'",
                "pipeline": _STOPPED_VM_BOOT_DISK_READMISSION,
            },
        ],
    },
]


# Resource Graph caps the number of subscriptions accepted per request. Requests above
# the cap are rejected outright rather than truncated, so large tenants are batched.
_MAX_SUBS_PER_QUERY = 1000

_SUB_ID_RE = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)


def _chunk(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def _run_arg_query(client, query, subscriptions=None, management_groups=None):
    """Execute one Resource Graph query, following skip tokens, and return all rows."""
    options = QueryRequestOptions(result_format="objectArray")
    kwargs = {"query": query, "options": options}
    if subscriptions:
        kwargs["subscriptions"] = list(subscriptions)
    if management_groups:
        kwargs["management_groups"] = list(management_groups)

    try:
        request = QueryRequest(**kwargs)
    except TypeError as e:
        # management_groups arrived in a later API version than subscriptions; if the
        # installed SDK predates it, say so instead of failing with a bare TypeError.
        raise RuntimeError(
            "the installed azure-mgmt-resourcegraph does not accept 'management_groups'. "
            "Upgrade the package, or use --all-subscriptions with --exclude-subscription "
            "to approximate the same scope."
        ) from e

    rows = []
    skip_token = None
    while True:
        if skip_token:
            request.options.skip_token = skip_token
        response = client.resources(request)
        rows.extend(response.data or [])

        skip_token = getattr(response, "skip_token", None)

        # Resource Graph reports truncation separately from paging. Truncated *with* a
        # continuation token is normal and the loop keeps going; truncated *without* one
        # means rows were dropped and no amount of paging will recover them, which would
        # otherwise show up only as a quietly low count.
        truncated = getattr(response, "result_truncated", None)
        truncated = str(getattr(truncated, "value", truncated) or "").lower() == "true"
        if truncated and not skip_token:
            DIAG.bump("resource_graph_truncations")
            print(
                f"  WARNING: Resource Graph truncated results for "
                f"'{query.split('|')[0].strip()}' and returned no continuation token — "
                "counts derived from this query are INCOMPLETE.",
                file=sys.stderr,
            )

        if not skip_token:
            break
    return rows


def run_query(client, subscription_ids, table, kql_filter, pipeline="", location_filter=None, dry_run=False):
    """
    Run a Resource Graph query and return a {(subscription_id, resource_type): count} breakdown.
    Uses KQL summarize to get per-type counts efficiently; summarizing by subscriptionId as
    well costs nothing extra and is what makes per-account attribution possible in one call.
    """
    location_clause = ""
    if location_filter:
        location_clause = f" and location =~ '{location_filter}'"

    query = (
        f"{table} | where ({kql_filter}){location_clause}{pipeline}"
        " | summarize count() by type, subscriptionId"
    )

    if dry_run:
        print(f"  [dry-run] Resource Graph query: {query}")
        return {}

    breakdown = {}
    for batch in _chunk(subscription_ids, _MAX_SUBS_PER_QUERY):
        for row in _run_arg_query(client, query, subscriptions=batch):
            key = (
                str(row.get("subscriptionId", "")).lower(),
                str(row.get("type", "unknown")).lower(),
            )
            breakdown[key] = breakdown.get(key, 0) + row.get("count_", 0)

    return breakdown


def _parse_change_time(value):
    """Resource Graph returns timestamps as ISO strings; tolerate a datetime too."""
    if value is None or isinstance(value, datetime):
        return value
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def probe_change_feed(client, subscription_ids):
    """Can Resource Graph answer for the change feed under these subscriptions?

    An empty result is NOT ambiguous here, which is why this probe has no UNKNOWN verdict.
    Were the table unreachable the query would raise; a query that succeeds and returns nothing
    means the estate genuinely did not change, and a flat reconstruction is then the right
    answer rather than a guess. Contrast GCP, where an empty window and an unavailable
    instrument look identical and the probe has to decline.
    """
    try:
        for batch in _chunk(subscription_ids, _MAX_SUBS_PER_QUERY):
            _run_arg_query(client, "resourcechanges | limit 1", subscriptions=batch)
            break
    except Exception as e:
        DIAG.record_denial("resourcechanges", type(e).__name__)
        print(f"  WARNING: change history unavailable, falling back to a point-in-time count: {e}",
              file=sys.stderr)
        return PROBE_UNAVAILABLE
    return PROBE_AVAILABLE


def fetch_change_events(client, subscription_ids, resource_types, dry_run=False):
    """Daily Create/Delete counts per (subscription, type) across the retention window.

    Binned by day rather than returned per event: the reconstruction is a daily series, so
    finer resolution would be discarded anyway, and binning keeps the response small on estates
    that churn.
    """
    types_clause = ", ".join("'" + t.lower() + "'" for t in sorted(resource_types))
    query = (
        "resourcechanges"
        " | extend changeTime = todatetime(properties.changeAttributes.timestamp),"
        " changeType = tostring(properties.changeType),"
        " targetType = tolower(tostring(properties.targetResourceType)),"
        " targetId = tolower(tostring(properties.targetResourceId))"
        f" | where changeTime > ago({WINDOW_DAYS}d)"
        " | where changeType in ('Create', 'Delete')"
        f" | where targetType in ({types_clause})"
        f" | where {_SQL_MASTER_CHANGE_EXCLUSION}"
        + _MANAGED_DB_COPY_CHANGE_EXCLUSION +
        " | summarize count() by changeType, targetType, subscriptionId, bin(changeTime, 1d)"
    )
    if dry_run:
        print(f"  [dry-run] Resource Graph query: {query}")
        return {}

    events = {}
    for batch in _chunk(subscription_ids, _MAX_SUBS_PER_QUERY):
        for row in _run_arg_query(client, query, subscriptions=batch):
            key = (str(row.get("subscriptionId", "")).lower(),
                   str(row.get("targetType", "")).lower())
            events.setdefault(key, []).append((
                _parse_change_time(row.get("changeTime")),
                str(row.get("changeType", "")),
                int(row.get("count_", 0) or 0),
            ))
    return events


def reconstruct_series(current, events, now):
    """Daily counts across the window, walking the change feed backwards from the present.

    count(T) = what exists now, plus everything deleted since T, minus everything created
    since T. A resource created inside the window was absent before it; one deleted inside the
    window was present before that. Undated events are ignored rather than guessed at.
    """
    window_start = now - timedelta(days=WINDOW_DAYS)
    series = []
    for offset in range(WINDOW_DAYS + 1):
        day = window_start + timedelta(days=offset)
        count = current
        for when, kind, many in events:
            if when is None or when <= day:
                continue
            count += many if kind == "Delete" else -many
        series.append(count)
    return series


def enumerate_tenant_subscriptions(credential):
    """List every enabled subscription the credential can see. Returns [(id, name)]."""
    from azure.mgmt.resource import SubscriptionClient

    client = SubscriptionClient(credential)
    subs = []
    for s in client.subscriptions.list():
        state = str(getattr(s.state, "value", s.state) or "")
        if state.lower() == "enabled" and s.subscription_id:
            subs.append((s.subscription_id.lower(), s.display_name or s.subscription_id))
    return subs


def enumerate_management_group_subscriptions(client, mg_id):
    """
    List enabled subscriptions under a management group, recursively.

    Uses Resource Graph's own management-group scope rather than walking the hierarchy
    client-side, so nesting depth costs nothing and no extra SDK dependency is needed.
    """
    query = (
        "ResourceContainers"
        " | where type =~ 'microsoft.resources/subscriptions'"
        " | where tostring(properties.state) =~ 'Enabled'"
        " | project subscriptionId, name"
    )
    subs = []
    for row in _run_arg_query(client, query, management_groups=[mg_id]):
        sid = row.get("subscriptionId")
        if sid:
            subs.append((str(sid).lower(), row.get("name") or str(sid)))
    return subs


def probe_accessible(client, subscription_ids):
    """
    Return the subset of subscription_ids Resource Graph will actually return data for.

    Resource Graph silently drops subscriptions the identity cannot read — it does not
    error — so a run that is missing a subscription looks identical to one where that
    subscription is empty. Probing up front is what turns that into a visible warning.
    """
    query = (
        "ResourceContainers"
        " | where type =~ 'microsoft.resources/subscriptions'"
        " | project subscriptionId"
    )
    seen = set()
    for batch in _chunk(subscription_ids, _MAX_SUBS_PER_QUERY):
        for row in _run_arg_query(client, query, subscriptions=batch):
            sid = str(row.get("subscriptionId", "")).lower()
            if sid:
                seen.add(sid)
    return seen


def _shape_breakdown(raw_by_type, resource_types):
    """Map returned ARM types back onto the spec list, keeping any extras that appear."""
    breakdown = {rt: raw_by_type.get(rt.lower(), 0) for rt in resource_types}
    for raw_type, cnt in raw_by_type.items():
        if raw_type not in breakdown:
            breakdown[raw_type] = cnt
    return breakdown


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


def probe_power_states(client, subscription_ids):
    """Bucket VMs by power-state readability, in both tables that carry them.

    This is the canary for the weakest assumption in footnote 26's Azure implementation. The
    properties.extended.instanceView.powerState.code path is documented and appears in
    Microsoft's own sample queries for microsoft.compute/virtualmachines, but NO published
    sample covers scale-set instances in ComputeResources -- so that shape is inferred, and
    an unreadable power state deliberately leaves the VM counted. A table whose rows are
    entirely 'unreadable' says the inference was wrong and stopped instances there are still
    being counted as running.

    Deliberately not filtered by the AVD exclusion, so these numbers are not expected to
    match the VM row. The question being asked is whether the field is readable, not what the
    count is.
    """
    out = {}
    for table, rtype in (
        ("resources", "microsoft.compute/virtualmachines"),
        ("computeresources", "microsoft.compute/virtualmachinescalesets/virtualmachines"),
    ):
        query = (
            f"{'Resources' if table == 'resources' else 'ComputeResources'}"
            f" | where type =~ '{rtype}'"
            + _POWER_STATE_EXTEND
            + " | summarize count() by bucket = case(isempty(_powerState), 'unreadable',"
            f" _powerState in~ {_STOPPED_POWER_STATES}, 'stopped', 'running')"
        )
        buckets = {"running": 0, "stopped": 0, "unreadable": 0}
        try:
            for batch in _chunk(subscription_ids, _MAX_SUBS_PER_QUERY):
                for row in _run_arg_query(client, query, subscriptions=batch):
                    name = str(row.get("bucket", "")).lower()
                    if name in buckets:
                        buckets[name] += row.get("count_", 0)
        except Exception as e:
            DIAG.record_denial(f"power_state_probe/{table}", type(e).__name__)
            out[table] = None
            continue
        out[table] = buckets
    return out


# Two counts need Azure Resource Manager directly, because Resource Graph does not index what
# they count. Both are ordinary control-plane reads that Reader covers, made with the run's own
# credential through azure-core, which azure-mgmt-resource already installs. No SDK is added
# for one list call each. API versions are GA releases checked against Microsoft's REST
# reference on 2026-09-25.
ARM_ENDPOINT = "https://management.azure.com"
ARM_SCOPE = ARM_ENDPOINT + "/.default"
STORAGE_API_VERSION = "2023-05-01"
ML_API_VERSION = "2024-10-01"
CONTAINER_TYPE = "microsoft.storage/storageaccounts/blobservices/containers"
ML_COMPUTE_TYPE = "microsoft.machinelearningservices/workspaces/computes"
ML_COMPUTE_STOPPED_TYPE = ML_COMPUTE_TYPE + " (stopped)"


def arm_client(credential):
    return PipelineClient(ARM_ENDPOINT, policies=[
        RetryPolicy(),  # honours Retry-After on ARM throttling
        BearerTokenCredentialPolicy(credential, ARM_SCOPE),
    ])


def _arm_list(arm, path, api_version, query=""):
    """Yield every item of a paged ARM list.

    nextLink is followed only while it stays on management.azure.com. The request carries a
    management token, and Microsoft's own sample for the container list shows a nextLink on
    another host, so a link anywhere else is refused rather than trusted. The refusal is
    counted, and the caller treats that resource as unread.
    """
    url = f"{ARM_ENDPOINT}{path}?api-version={api_version}{query}"
    while url:
        resp = arm.send_request(HttpRequest("GET", url))
        resp.raise_for_status()
        body = resp.json()
        yield from body.get("value") or []
        url = body.get("nextLink")
        if url and not url.startswith(ARM_ENDPOINT + "/"):
            DIAG.bump("arm_nextlink_refused")
            raise RuntimeError("ARM returned a nextLink outside management.azure.com")


# Read scope: does the credential read the whole subscription, or only part of it?
#
# Resource Graph returns whatever the caller may read "without any indication that the result
# might be partial", so a login holding Reader on one resource group reads as a small but
# complete subscription. Found in the first field run (OI-42): one subscription came back with a
# single blob container where an earlier run with another login had seen 325 VMs.
#
# Microsoft documents no call that returns the caller's effective permissions at subscription
# scope (the Permissions API stops at resource group). The documented route is the caller's own
# role assignments at the subscription: atScope() returns those at the subscription and above
# (management groups, root) but not on child resource groups, and assignedTo() follows group
# membership. Reader's */read includes roleAssignments/read. A refusal is itself the answer: a
# caller that cannot read authorization at subscription scope does not hold Reader there.
#
# Known blind spots, which is why this warns rather than stops the run: Azure Lighthouse
# delegations are not listed, so delegated access reads as partial; deny assignments are not
# checked; an assignment with an ABAC condition is not relied on. Classic co-administrators are
# not role assignments and also read as partial.
ROLE_API_VERSION = "2022-04-01"
READ_WHOLE, READ_PARTIAL, READ_UNKNOWN = "whole", "partial", "unknown"


def _caller_object_id(credential):
    """The oid claim of the caller's management token, or None if it cannot be read."""
    try:
        token = credential.get_token(ARM_SCOPE).token
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        return json.loads(base64.urlsafe_b64decode(payload)).get("oid")
    except Exception:
        return None


def _role_grants_read(definition):
    """True when a role definition grants read on every resource type, as Reader does."""
    for block in (definition.get("properties") or {}).get("permissions") or []:
        actions = {a.lower() for a in block.get("actions") or []}
        not_actions = {a.lower() for a in block.get("notActions") or []}
        if actions & {"*", "*/read"} and not not_actions & {"*", "*/read"}:
            return True
    return False


def probe_read_scope(arm, credential, subscription_ids):
    """{subscription: whole | partial | unknown} for the caller's read access.

    whole: a role assignment at the subscription or above, with no condition, grants */read.
    partial: the list was refused, or nothing listed grants */read there. The credential may
    still read some of the subscription, which is exactly the case the counts cannot show.
    unknown: the caller's identity or a role definition could not be read, so nothing is said.
    """
    oid = _caller_object_id(credential)
    if not oid:
        DIAG.record_denial("read_scope/oid", "unreadable")
        return {sid: READ_UNKNOWN for sid in subscription_ids}
    definitions, out = {}, {}
    for sid in subscription_ids:
        path = f"/subscriptions/{sid}/providers/Microsoft.Authorization/roleAssignments"
        try:
            assignments = list(_arm_list(arm, path, ROLE_API_VERSION,
                                         f"&$filter=atScope()+and+assignedTo('{oid}')"))
        except Exception as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status == 403:
                out[sid] = READ_PARTIAL
            else:
                DIAG.record_denial("read_scope/roleAssignments", type(e).__name__)
                out[sid] = READ_UNKNOWN
            continue
        verdict = READ_PARTIAL
        for assignment in assignments:
            props = assignment.get("properties") or {}
            role_id = props.get("roleDefinitionId")
            if props.get("condition") or not role_id:
                continue
            if role_id not in definitions:
                try:
                    resp = arm.send_request(HttpRequest(
                        "GET", f"{ARM_ENDPOINT}{role_id}?api-version={ROLE_API_VERSION}"))
                    resp.raise_for_status()
                    definitions[role_id] = _role_grants_read(resp.json())
                except Exception as e:
                    DIAG.record_denial("read_scope/roleDefinitions", type(e).__name__)
                    definitions[role_id] = None
            if definitions[role_id]:
                verdict = READ_WHOLE
                break
            if definitions[role_id] is None:
                verdict = READ_UNKNOWN
        out[sid] = verdict
    return out


def _resource_ids(rg_client, subscription_ids, rtype, location_filter=None):
    """[(subscription id, resource id, location)] for one type, from Resource Graph."""
    where = f"type =~ '{rtype}'"
    if location_filter:
        where += f" and location =~ '{location_filter}'"
    query = f"Resources | where {where} | project id, subscriptionId, location"
    out = []
    for batch in _chunk(subscription_ids, _MAX_SUBS_PER_QUERY):
        for row in _run_arg_query(rg_client, query, subscriptions=batch):
            out.append((str(row.get("subscriptionId", "")).lower(), str(row.get("id", "")),
                        str(row.get("location", "")).lower()))
    return out


def count_blob_containers(arm, rg_client, subscription_ids, location_filter=None):
    """{(subscription, CONTAINER_TYPE): count} across every storage account in scope.

    One Storage resource-provider call per account. Its permission, blobServices/containers/
    read, is a control-plane Action that Reader covers, and storage firewall rules apply only
    to the data plane, so private endpoints do not block it (footnote 21).

    Containers whose names start with "$" are Azure's own ($logs, $web) and are skipped.
    Soft-deleted containers are not listed unless asked for, so they never count. An account
    with no containers counts as 1: it can still hold file shares, queues or tables, and 1 is
    exactly what it counted before containers were, so switching units never makes an account
    disappear. An account whose list fails also counts as 1, and is tallied so the canary
    reports it.
    """
    counts, failed, floored = {}, 0, 0
    accounts = _resource_ids(rg_client, subscription_ids, "microsoft.storage/storageaccounts",
                             location_filter)
    for sid, account_id, _loc in accounts:
        try:
            n = sum(1 for c in _arm_list(arm, f"{account_id}/blobServices/default/containers",
                                         STORAGE_API_VERSION)
                    if not str(c.get("name", "")).startswith("$"))
        except Exception as e:
            DIAG.record_denial("storage_containers", type(e).__name__)
            failed += 1
            n = 0
        if n == 0:
            floored += 1
            n = 1
        key = (sid, CONTAINER_TYPE)
        counts[key] = counts.get(key, 0) + n
    DIAG.measure("storage_accounts_scanned", len(accounts))
    DIAG.measure("storage_accounts_counted_as_one", floored)
    DIAG.measure("storage_accounts_unlisted", failed)
    return counts


def count_ml_computes(arm, rg_client, subscription_ids, location_filter=None):
    """{(subscription, type): count} of Azure ML compute instances, running and stopped.

    One ARM call per workspace (Compute - List), which Reader covers. Only ComputeInstance
    counts. AmlCompute clusters are autoscaling constructs, and AKS, Kubernetes, Databricks
    and VirtualMachine computes are attached resources already counted where they live
    (footnote 19).

    Stopped counts at the disk weight, like a stopped VM (footnote 26). Any other state,
    transitional or unreadable, stays on the VM line, as an unreadable VM power state does.

    Deduplicated on (region, name). Microsoft requires compute instance names to be unique
    within a region, so this is safe, and it means a hub and a project workspace in Azure AI
    Foundry listing the same instance cannot count it twice.
    """
    counts, seen, failed = {}, set(), 0
    workspaces = _resource_ids(rg_client, subscription_ids,
                               "microsoft.machinelearningservices/workspaces", location_filter)
    for sid, ws_id, ws_loc in workspaces:
        try:
            items = list(_arm_list(arm, f"{ws_id}/computes", ML_API_VERSION))
        except Exception as e:
            DIAG.record_denial("ml_computes", type(e).__name__)
            failed += 1
            continue
        for c in items:
            props = c.get("properties") or {}
            if props.get("computeType") != "ComputeInstance":
                continue
            ident = ((c.get("location") or ws_loc).lower(), str(c.get("name", "")).lower())
            if ident in seen:
                DIAG.bump("ml_compute_instances_deduplicated")
                continue
            seen.add(ident)
            state = (props.get("properties") or {}).get("state")
            if not state:
                DIAG.bump("ml_compute_state_unreadable")
            rtype = ML_COMPUTE_STOPPED_TYPE if state == "Stopped" else ML_COMPUTE_TYPE
            counts[(sid, rtype)] = counts.get((sid, rtype), 0) + 1
    DIAG.measure("ml_workspaces_scanned", len(workspaces))
    DIAG.measure("ml_workspaces_unlisted", failed)
    return counts


def _count_query(client, subscription_ids, query):
    """Sum a single `summarize count()` query across subscription batches."""
    total = 0
    for batch in _chunk(subscription_ids, _MAX_SUBS_PER_QUERY):
        for row in _run_arg_query(client, query, subscriptions=batch):
            total += row.get("count_", 0) or 0
    return total


def probe_exclusions(client, subscription_ids):
    """Measure what the AVD and SQL master exclusions actually removed.

    Both exclusions fail silently. If the AVD join stops matching, session hosts re-enter the
    VM count and the number still looks ordinary. So the run counts, independently of the VM
    query, the session hosts that exist and the VMs that match one. Hosts with no match is the
    broken-join signature, and the canary reports it. This is how the join gets validated from
    field runs, since no demo estate has AVD (OI-13).

    Counts only. No host, VM or database is named, because this block travels to Product.
    Each measure is None when its query failed, so a denial never reads as zero.
    """
    session_hosts_kql = (
        "DesktopVirtualizationResources"
        " | where type =~ 'microsoft.desktopvirtualization/hostpools/sessionhosts'"
    )
    queries = {
        "avd_session_hosts": session_hosts_kql + " | summarize count()",
        "sql_master_databases_excluded": (
            "Resources | where type =~ 'microsoft.sql/servers/databases' and name =~ 'master'"
            " | summarize count()"
        ),
        "managed_db_copies_excluded": f"Resources | where {_MANAGED_DB_COPY} | summarize count()",
    }
    for table, rtype in (
        ("Resources", "microsoft.compute/virtualmachines"),
        ("ComputeResources", "microsoft.compute/virtualmachinescalesets/virtualmachines"),
    ):
        queries[f"avd_vms_excluded/{table}"] = (
            f"{table} | where type =~ '{rtype}' | extend vmId = tolower(id)"
            f" | join kind=inner ({session_hosts_kql}"
            " | extend vmId = tolower(tostring(properties.resourceId)) | project vmId) on vmId"
            " | summarize count()"
        )
    raw = {}
    for name, query in queries.items():
        try:
            raw[name] = _count_query(client, subscription_ids, query)
        except Exception as e:
            DIAG.record_denial(f"exclusion_probe/{name.split('/')[0]}", type(e).__name__)
            raw[name] = None
    parts = [raw["avd_vms_excluded/Resources"], raw["avd_vms_excluded/ComputeResources"]]
    return {
        "avd_session_hosts": raw["avd_session_hosts"],
        "avd_vms_excluded": None if None in parts else sum(parts),
        "sql_master_databases_excluded": raw["sql_master_databases_excluded"],
        "managed_db_copies_excluded": raw["managed_db_copies_excluded"],
    }


def probe_os_disk_states(client, subscription_ids):
    """Bucket OS disks by their documented DiskState, excluding scale-set-owned ones.

    A free cross-check on the half of footnote 26 that could not be verified offline. Azure
    states the disk-to-VM relationship on the disk itself, so if disks report Reserved or
    Frozen -- "attached to a stopped-deallocated VM" and "attached to a VM which is in
    hibernated state" -- then stopped VMs demonstrably exist, and the power-state join had
    better have found them. A mismatch says the VM-line power-state test is missing a state.

    Scale-set-owned disks are excluded for the same reason they are excluded from
    re-admission: their instance is counted through ComputeResources, where power state is
    unverified, so counting both would double-count a stopped AKS node.
    """
    query = (
        "Resources | where type =~ 'microsoft.compute/disks'"
        " | where isnotempty(tostring(properties.osType))"
        " | extend _ownerId = tolower(tostring(managedBy))"
        " | where _ownerId !contains '/virtualmachinescalesets/'"
        " | summarize count() by bucket = tostring(properties.diskState)"
    )
    buckets = {}
    try:
        for batch in _chunk(subscription_ids, _MAX_SUBS_PER_QUERY):
            for row in _run_arg_query(client, query, subscriptions=batch):
                name = str(row.get("bucket") or "unknown")
                buckets[name] = buckets.get(name, 0) + row.get("count_", 0)
    except Exception as e:
        DIAG.record_denial("os_disk_state_probe", type(e).__name__)
        return None
    return buckets


def probe_nonos_disk_reasons(client, subscription_ids):
    """Break the Non-OS Disk count down by why each disk is in it, and check the OS-disk test.

    nonos_disks_per_vm says only that disks outnumber VMs. A genuinely disk-heavy estate (SQL
    or SAP VMs with several data disks, orphaned disks nobody deleted, AKS persistent volumes)
    trips it on correct numbers, so on a failure the reasons are what tell a real estate from
    a broken filter. The first case is the same join and the same tests as the count, in the
    same order, so the counted reasons sum to the microsoft.compute/disks figure.

    The second query tests the one assumption the filter cannot check itself: that every OS
    disk carries properties.osType. A disk imported or migrated without one, and then attached
    as a VM's OS disk, has no osType and is counted as a data disk. AWS and GCP decide boot
    disks by attachment, so only Azure can fail this way. The VM names its own OS disk in
    storageProfile, which is the attachment-side answer to compare against.
    """
    reasons_query = (
        "Resources | where type =~ 'microsoft.compute/disks'"
        + _DISK_OWNER_JOIN
        + " | extend reason = case("
        f"isnotempty(_osType) and _vmPower in~ {_STOPPED_POWER_STATES}, 'os_disk_stopped_vm',"
        f" isnotempty(_osType) and _diskState in~ {_UNOWNED_DISK_STATES}"
        " and _ownerId !contains '/virtualmachinescalesets/', 'os_disk_unowned',"
        " isnotempty(_osType), 'os_disk_excluded',"
        " isempty(_ownerId), 'data_disk_unattached',"
        " _ownerId contains '/virtualmachinescalesets/', 'data_disk_scale_set',"
        " _ownerId contains '/virtualmachines/', 'data_disk_vm',"
        " 'data_disk_other')"
        " | summarize count() by reason"
    )
    unmarked_query = (
        "Resources | where type =~ 'microsoft.compute/virtualmachines'"
        " | project _diskId = tolower(tostring(properties.storageProfile.osDisk.managedDisk.id))"
        " | where isnotempty(_diskId)"
        " | join kind=inner (Resources | where type =~ 'microsoft.compute/disks'"
        " | project _diskId = tolower(id), _osType = tostring(properties.osType)) on _diskId"
        " | where isempty(_osType) | summarize count()"
    )
    reasons = {}
    try:
        for batch in _chunk(subscription_ids, _MAX_SUBS_PER_QUERY):
            for row in _run_arg_query(client, reasons_query, subscriptions=batch):
                name = str(row.get("reason") or "unknown")
                reasons[name] = reasons.get(name, 0) + row.get("count_", 0)
    except Exception as e:
        DIAG.record_denial("nonos_disk_reason_probe", type(e).__name__)
        reasons = None
    try:
        unmarked = _count_query(client, subscription_ids, unmarked_query)
    except Exception as e:
        DIAG.record_denial("os_disk_marker_probe", type(e).__name__)
        unmarked = None
    return {"nonos_disk_reasons": reasons, "vm_os_disks_without_ostype": unmarked}


def evaluate_canaries(totals, inaccessible, power_states, os_disk_states=None, exclusions=None,
                      disk_probe=None, read_scope=None):
    """Assert the things whose failure mode is a plausible-looking number rather than an error.

    Every check here guards a filter or a join that quietly returns an acceptable-looking
    count when it breaks, which is why they run on every invocation instead of behind a flag.
    """
    by_cat = {r["category"]: r for r in totals}
    vm = by_cat.get("VM", {}).get("count", 0)
    disks = by_cat.get("Non-OS Disk", {}).get("count", 0)

    # The change feed and the present must agree. Reconstruction walks backwards from what
    # exists now, so a day that comes out negative means more creations were seen than the
    # current count can account for -- the walk has lost track, and every smoothed number in
    # this run is suspect. This is the integrity check that makes a reconstruct-backwards rung
    # safe to trust: it self-checks against a count measured a different way.
    drift = DIAG.measures.get("change_reconstruction_negative", 0)
    DIAG.canary(
        "change_reconstruction_consistent", drift == 0, drift,
        "the change feed implies more resources were created than exist now, so the smoothed "
        "counts have drifted from what the present can account for",
    )

    unmeasured = {rt for r in totals for rt, v in r["breakdown"].items() if v is None}
    DIAG.canary(
        "resource_types_all_measured", not unmeasured, len(unmeasured),
        "resource type(s) could not be measured, so their categories are undercounts",
    )
    DIAG.canary(
        "subscriptions_all_readable", not inaccessible, len(inaccessible),
        "subscription(s) returned no data at all -- either empty, or the credential cannot "
        "read them, which Resource Graph does not distinguish",
    )
    # Counts only: which subscriptions is in the run's scope block, not in diagnostics.
    read_scope = read_scope or {}
    buckets = {v: sum(1 for x in read_scope.values() if x == v)
               for v in (READ_WHOLE, READ_PARTIAL, READ_UNKNOWN)}
    DIAG.measure("subscription_read_scope", buckets)
    judged = buckets[READ_WHOLE] + buckets[READ_PARTIAL]
    DIAG.canary(
        "subscriptions_read_whole", None if not judged else buckets[READ_PARTIAL] == 0,
        buckets[READ_PARTIAL],
        "subscription(s) where the credential holds no role granting read on the whole "
        "subscription, so their counts may cover only some resource groups",
    )
    exclusions = exclusions or {}
    for name in ("avd_session_hosts", "avd_vms_excluded", "sql_master_databases_excluded",
                 "managed_db_copies_excluded"):
        if name in exclusions:
            DIAG.measure(name, exclusions[name])
    hosts, matched = exclusions.get("avd_session_hosts"), exclusions.get("avd_vms_excluded")
    if not hosts or matched is None:
        DIAG.canary("avd_session_host_exclusion_firing", None, matched,
                    "no AVD session hosts found, or the probe could not run, so nothing to check")
    else:
        DIAG.canary(
            "avd_session_host_exclusion_firing", matched > 0, matched,
            "AVD session hosts exist but matched no VM, so session hosts may be counted as VMs "
            "(a host pool whose VMs sit in a subscription outside the scan also reads this way)",
        )

    for name, measure, scanned, what in (
        ("storage_containers_all_listed", "storage_accounts_unlisted", "storage_accounts_scanned",
         "storage account(s) could not list their containers and were counted as 1 each"),
        ("ml_computes_all_listed", "ml_workspaces_unlisted", "ml_workspaces_scanned",
         "ML workspace(s) could not list their computes, so their compute instances are missing"),
    ):
        unlisted = DIAG.measures.get(measure)
        if not DIAG.measures.get(scanned) or unlisted is None:
            DIAG.canary(name, None, unlisted, "nothing of this type in scope, or it was not run")
        else:
            DIAG.canary(name, unlisted == 0, unlisted, what)

    truncations = DIAG.measures.get("resource_graph_truncations", 0)
    DIAG.canary(
        "resource_graph_not_truncated", truncations == 0, truncations,
        "Resource Graph dropped rows with no continuation token, so those counts are too low",
    )

    stopped = 0
    readable_any = False
    for table, buckets in (power_states or {}).items():
        if not buckets:
            DIAG.canary(f"power_state_probe_{table}", None, None,
                        "the power-state probe could not run against this table")
            continue
        total = sum(buckets.values())
        stopped += buckets.get("stopped", 0)
        if not total:
            DIAG.canary(f"power_state_readable_{table}", None, 0,
                        "no VMs of this shape exist, so readability cannot be judged")
            continue
        readable = total - buckets.get("unreadable", 0)
        readable_any = readable_any or readable > 0
        DIAG.canary(
            f"power_state_readable_{table}", buckets.get("unreadable", 0) == 0,
            round(buckets.get("unreadable", 0) / total, 3),
            "VM power state was unreadable for this fraction of rows, so those VMs are "
            "counted as running and their OS disks stay excluded even when stopped",
        )
    DIAG.measure("power_states", power_states)
    DIAG.measure("os_disk_states", os_disk_states)

    # Cross-check the power-state join against what the disks themselves report. Reserved and
    # Frozen mean the owning VM is stopped-deallocated or hibernated, so if any exist then
    # stopped VMs exist, and the join should have classified some. Disks in those states while
    # the probe saw no stopped VM at all means the VM-line test is missing a power state --
    # the one gap this implementation could not close from documentation alone.
    if os_disk_states is None:
        DIAG.canary("stopped_vm_detection_agrees_with_disks", None, None,
                    "the OS disk state probe could not run")
    else:
        unowned = sum(v for k, v in os_disk_states.items()
                      if k.lower() in ("reserved", "frozen", "activesasfrozen"))
        DIAG.canary(
            "stopped_vm_detection_agrees_with_disks",
            not (unowned and not stopped), unowned,
            "OS disks report their VM as stopped-deallocated or hibernated, but the VM "
            "power-state test found no stopped VMs, so that test is missing a power state",
        )

    # An OS disk is re-admitted to Non-OS Disk only while its VM is not counted, so after
    # footnote 26 the honest denominator is every VM that still exists, running or stopped --
    # otherwise a mostly-stopped estate trips this on correct numbers. Roughly one disk per VM
    # is what "OS disks are being counted" looks like; the measured fleet-wide figure across
    # 668 customers is about 0.4.
    instances = vm + stopped
    if instances < CANARY_MIN_INSTANCES:
        DIAG.canary(
            "nonos_disks_per_vm", None, None,
            f"fewer than {CANARY_MIN_INSTANCES} VMs, so the ratio is not meaningful",
        )
    else:
        ratio = round(disks / instances, 3)
        DIAG.canary(
            "nonos_disks_per_vm", ratio < 1.0, ratio,
            "at least one Non-OS Disk per VM suggests OS disks are no longer being excluded; "
            "the measured fleet-wide figure is about 0.4. nonos_disk_reasons says which disks "
            "make up the count",
        )
        DIAG.measure("stopped_vm_fraction", round(stopped / instances, 3))

    # Always measured, not only when the ratio fails: the reasons are what a reader needs to
    # tell a disk-heavy estate from a leak, and a passing run is the baseline to compare with.
    disk_probe = disk_probe or {}
    DIAG.measure("nonos_disk_reasons", disk_probe.get("nonos_disk_reasons"))
    unmarked = disk_probe.get("vm_os_disks_without_ostype")
    DIAG.canary(
        "vm_os_disks_marked", None if unmarked is None else unmarked == 0, unmarked,
        "VM OS disk(s) carry no properties.osType, so the filter counts them as Non-OS Disks "
        "although their VM is counted",
    )


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


def _print_table(results, heading=None):
    if heading:
        print(f"\n{heading}")
    col1 = max([len(r["category"]) for r in results] + [len("Category")])
    for line in _format_table(results, col1):
        print(line)


def resolve_targets(args, credential, rg_client):
    """
    Work out which subscriptions to scan. Returns (mode, targets, excluded, unmatched).

    Every mode resolves to an explicit subscription list rather than leaning on Resource
    Graph's implicit scoping. That costs one enumeration call but is what lets exclusions,
    the accessibility probe and per-account output all work off the same known set.
    """
    if args.subscription:
        mode = "subscriptions"
        targets = [(s.lower(), s) for s in args.subscription]
    elif args.all_subscriptions:
        mode = "all-subscriptions"
        if args.dry_run:
            print("  [dry-run] would enumerate all enabled subscriptions in the tenant")
            targets = []
        else:
            targets = enumerate_tenant_subscriptions(credential)
    elif args.management_group:
        mode = "management-group"
        if args.dry_run:
            print(f"  [dry-run] would enumerate subscriptions under {args.management_group}")
            targets = []
        else:
            targets = enumerate_management_group_subscriptions(rg_client, args.management_group)
    else:
        mode = "default"
        subscription_id = os.environ.get("AZURE_SUBSCRIPTION_ID")
        if not subscription_id:
            print(
                "FATAL: no subscription selected. Set AZURE_SUBSCRIPTION_ID, or pass "
                "--subscription, --all-subscriptions or --management-group.",
                file=sys.stderr,
            )
            sys.exit(1)
        targets = [(subscription_id.lower(), subscription_id)]

    excluded_set = {s.lower() for s in args.exclude_subscription}
    resolved_ids = {sid for sid, _ in targets}
    excluded = sorted(sid for sid in resolved_ids if sid in excluded_set)
    unmatched = sorted(excluded_set - resolved_ids)
    targets = [(sid, name) for sid, name in targets if sid not in excluded_set]

    return mode, targets, excluded, unmatched


def main():
    parser = argparse.ArgumentParser(description="Count Azure resources per FortiCNAPP billing category.")
    scope = parser.add_mutually_exclusive_group()
    scope.add_argument(
        "--all-subscriptions",
        action="store_true",
        help="Scan every enabled subscription the credential can see in the tenant",
    )
    scope.add_argument(
        "--management-group",
        metavar="MG_ID",
        help="Scan every enabled subscription under a management group, recursively",
    )
    scope.add_argument(
        "--subscription",
        nargs="+",
        metavar="ID",
        help="Scan one or more specific subscription IDs",
    )
    parser.add_argument(
        "--exclude-subscription",
        nargs="+",
        default=[],
        metavar="ID",
        help="Subscription IDs to drop from whichever set the scope flags resolve to",
    )
    parser.add_argument("--location", help="Scope to a single Azure location (e.g. eastus)")
    parser.add_argument("--output-dir", default=".", help="Directory to write JSON output (default: .)")
    parser.add_argument("--dry-run", action="store_true", help="Print queries without executing them")
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
    print(f"FortiCNAPP resource counts (Azure) - script version {script_version()}")
    run_start = time.time()

    if args.location and not re.fullmatch(r"[a-zA-Z0-9\-]+", args.location):
        print("FATAL: --location must contain only letters, digits, and hyphens.", file=sys.stderr)
        sys.exit(1)

    # A malformed ID would otherwise just contribute nothing and vanish into the totals.
    for sid in list(args.subscription or []) + list(args.exclude_subscription):
        if not _SUB_ID_RE.fullmatch(sid):
            print(f"FATAL: '{sid}' is not a valid subscription ID (expected a GUID).", file=sys.stderr)
            sys.exit(1)

    try:
        credential = DefaultAzureCredential()
        rg_client = ResourceGraphClient(credential)
    except Exception as e:
        print(f"FATAL: could not initialize Azure credentials: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        mode, targets, excluded, unmatched = resolve_targets(args, credential, rg_client)
    except Exception as e:
        print(f"FATAL: could not resolve subscriptions to scan: {e}", file=sys.stderr)
        sys.exit(1)

    for sid in unmatched:
        print(
            f"  WARNING: --exclude-subscription {sid} matched nothing in the resolved scope "
            "(already absent, or a typo)",
            file=sys.stderr,
        )

    if not targets and not args.dry_run:
        print("FATAL: no subscriptions left to scan after exclusions.", file=sys.stderr)
        sys.exit(1)

    target_ids = [sid for sid, _ in targets]
    names = dict(targets)

    inaccessible = []
    if target_ids and not args.dry_run:
        try:
            accessible = probe_accessible(rg_client, target_ids)
            inaccessible = [sid for sid in target_ids if sid not in accessible]
        except Exception as e:
            print(f"  WARNING: could not verify subscription access: {e}", file=sys.stderr)
        for sid in inaccessible:
            print(
                f"  WARNING: subscription {sid} returned no data — it is either empty or the "
                "credential cannot read it. Its resources are NOT included in the totals below.",
                file=sys.stderr,
            )

    read_scope = {}
    if target_ids and not args.dry_run:
        read_scope = probe_read_scope(arm_client(credential), credential,
                                      [sid for sid in target_ids if sid not in inaccessible])
        for sid, verdict in read_scope.items():
            if verdict == READ_PARTIAL:
                print(
                    f"  WARNING: subscription {sid} ({names.get(sid, sid)}): this credential has no "
                    "role granting read on the whole subscription, so its counts may cover only "
                    "the resource groups it can see. Re-run with Reader at subscription scope or "
                    "above. (Access delegated through Azure Lighthouse also shows up this way.)",
                    file=sys.stderr,
                )
    partial_read = [sid for sid, v in read_scope.items() if v == READ_PARTIAL]

    if not args.dry_run:
        print(f"Scanning {len(target_ids)} subscription(s) [{mode}]")

    # Resolve the change-feed rung once: availability is a property of Resource Graph and the
    # credential, not of any one resource type. Which *types* it can speak for is a separate
    # question, answered per category by category_is_reconstructable.
    smoothable_types = sorted({
        rt.lower() for cat_def in CATEGORIES if category_is_reconstructable(cat_def)
        for rt in cat_def["resource_types"]
    })
    change_verdict = PROBE_UNAVAILABLE
    change_events = {}
    now = datetime.now(timezone.utc)
    if not args.dry_run and smoothable_types:
        change_verdict = probe_change_feed(rg_client, target_ids)
        if change_verdict == PROBE_AVAILABLE:
            print(f"Smoothing over {WINDOW_DAYS} days where the change feed can speak for a "
                  f"type; Resource Graph retains {WINDOW_DAYS} days of changes, so this window "
                  "is shorter than the other clouds'", file=sys.stderr)
            try:
                change_events = fetch_change_events(rg_client, target_ids, smoothable_types)
            except Exception as e:
                DIAG.record_denial("resourcechanges", type(e).__name__)
                print(f"  WARNING: could not read the change feed: {e}", file=sys.stderr)
                change_verdict = PROBE_UNAVAILABLE

    # Counts that come from Resource Manager rather than Resource Graph, merged into their
    # categories below. Neither category is smoothed, so they need no change-feed history.
    extra = {}
    if args.dry_run:
        print("  [dry-run] would list blob containers per storage account and compute instances "
              "per ML workspace through Azure Resource Manager")
    else:
        arm = arm_client(credential)
        for category, fn in (("Bucket Storage", count_blob_containers),
                             ("VM", count_ml_computes)):
            try:
                found = fn(arm, rg_client, target_ids, args.location)
            except Exception as e:
                DIAG.record_denial(f"arm/{fn.__name__}", type(e).__name__)
                print(f"  WARNING: {category}: {fn.__name__} failed: {e}", file=sys.stderr)
                continue
            for (sid, rtype), cnt in found.items():
                target = "Non-OS Disk" if rtype == ML_COMPUTE_STOPPED_TYPE else category
                bucket = extra.setdefault(target, {})
                bucket[(sid, rtype)] = bucket.get((sid, rtype), 0) + cnt

    per_account = {sid: [] for sid in target_ids}
    totals = []

    for cat_def in CATEGORIES:
        category = cat_def["category"]
        resource_types = cat_def["resource_types"]

        raw = {}
        for table, kql_filter, pipeline in _queries_for(cat_def):
            try:
                part = run_query(
                    rg_client,
                    target_ids,
                    table,
                    kql_filter,
                    pipeline=pipeline,
                    location_filter=args.location,
                    dry_run=args.dry_run,
                )
            except Exception as e:
                DIAG.record_denial(table, type(e).__name__)
                print(f"  WARNING: {category} ({table}): {e}", file=sys.stderr)
                continue
            for key, cnt in part.items():
                raw[key] = raw.get(key, 0) + cnt
        for key, cnt in extra.get(category, {}).items():
            raw[key] = raw.get(key, 0) + cnt

        smoothed = (change_verdict == PROBE_AVAILABLE
                    and category_is_reconstructable(cat_def) and not args.dry_run)
        rung = RUNG_RESOURCE_CHANGES if smoothed else RUNG_POINT_IN_TIME
        time_range = TIME_RANGE_WINDOW if smoothed else TIME_RANGE_NOW
        points = WINDOW_DAYS + 1 if smoothed else None
        for rt in resource_types:
            DIAG.rung_trace(rt, RUNG_RESOURCE_CHANGES,
                            PROBE_AVAILABLE if smoothed else PROBE_UNAVAILABLE)

        if smoothed:
            own_types = {rt.lower() for rt in resource_types}
            # A type deleted to zero inside the window has no current row, so its history would
            # be invisible if the keys came from the present alone.
            keys = set(raw) | {k for k in change_events if k[1] in own_types}
            averaged = {}
            for key in keys:
                series = reconstruct_series(raw.get(key, 0), change_events.get(key, []), now)
                if min(series) < 0:
                    # More creations than the present can account for: the feed and the current
                    # count disagree, so the walk has lost track somewhere.
                    DIAG.bump("change_reconstruction_negative")
                    # By type, never by subscription: the bare count says something drifted but
                    # not where to look, and the type is enough to find it in the feed.
                    by_type = DIAG.measures.setdefault("change_reconstruction_negative_types", {})
                    by_type[key[1]] = by_type.get(key[1], 0) + 1
                    series = [max(v, 0) for v in series]
                averaged[key] = sum(series) / len(series)
            raw = averaged

        provenance = {rt: Measurement(None, rung=rung, time_range=time_range,
                                      points=points).provenance()
                      for rt in resource_types}

        totals_by_type = {}
        for (_sid, rtype), cnt in raw.items():
            totals_by_type[rtype] = totals_by_type.get(rtype, 0) + cnt
        breakdown = {k: int(round(v)) for k, v in
                     _shape_breakdown(totals_by_type, resource_types).items()}
        cat_range, cat_method = category_provenance(provenance, breakdown)
        totals.append({
            "category": category,
            "count": sum(breakdown.values()),
            "time_range": cat_range,
            "method": cat_method,
            "resource_types": resource_types,
            "breakdown": breakdown,
            "provenance": provenance,
        })

        for sid in target_ids:
            sub_by_type = {
                rtype: cnt for (s, rtype), cnt in raw.items() if s == sid
            }
            sub_breakdown = {k: int(round(v)) for k, v in
                             _shape_breakdown(sub_by_type, resource_types).items()}
            sub_range, sub_method = category_provenance(provenance, sub_breakdown)
            per_account[sid].append({
                "category": category,
                "count": sum(sub_breakdown.values()),
                "time_range": sub_range,
                "method": sub_method,
                "resource_types": resource_types,
                "breakdown": sub_breakdown,
                "provenance": provenance,
            })

    _print_table(totals, heading=f"TOTALS ({len(target_ids)} subscription(s))" if len(target_ids) != 1 else None)

    if len(target_ids) > 1:
        for sid in target_ids:
            label = names.get(sid, sid)
            suffix = "  [no data returned]" if sid in inaccessible else (
                "  [partial read access]" if sid in partial_read else "")
            _print_table(per_account[sid], heading=f"{label} ({sid}){suffix}")

    power_states = {} if args.dry_run else probe_power_states(rg_client, target_ids)
    exclusions = {} if args.dry_run else probe_exclusions(rg_client, target_ids)
    DIAG.measure("run_seconds", round(time.time() - run_start, 1))
    os_disk_states = None if args.dry_run else probe_os_disk_states(rg_client, target_ids)
    disk_probe = None if args.dry_run else probe_nonos_disk_reasons(rg_client, target_ids)
    evaluate_canaries(totals, inaccessible, power_states, os_disk_states, exclusions, disk_probe,
                      read_scope)
    DIAG.report()

    output = {
        "csp": "azure",
        "scope": {
            "mode": mode,
            "management_group": args.management_group,
            "scanned": target_ids,
            "excluded": excluded,
            "inaccessible": inaccessible,
            "partial_read": partial_read,
        },
        "script_version": script_version(),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "diagnostics": DIAG.to_json(verbose=args.diagnostics),
        "totals": totals,
        "accounts": [
            {
                "account_id": sid,
                "display_name": names.get(sid, sid),
                "categories": per_account[sid],
            }
            for sid in target_ids
        ],
    }
    out_path = Path(args.output_dir) / "azure_resource_counts.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nJSON written to {out_path}")

    if args.encoded_output:
        print_encoded_output(output)


if __name__ == "__main__":
    main()
