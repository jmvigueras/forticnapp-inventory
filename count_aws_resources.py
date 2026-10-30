#!/usr/bin/env python3
"""Count AWS resources per FortiCNAPP billing category."""

import argparse
import base64
import gzip
import json
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError, ProfileNotFound

# Bumped by hand whenever a change can move a count. A count is only interpretable
# alongside the version that produced it: the counting rules change as mapping decisions
# land, so without this there is no way to tell a change in a customer's estate from a
# change in this script.
SCRIPT_VERSION = "2026.10.27"


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


VERBOSE = False

# Bound how long any single API call can hang, so one bad region/endpoint can't stall the whole run.
CLIENT_CONFIG = Config(connect_timeout=10, read_timeout=30, retries={"max_attempts": 2, "mode": "standard"})

# Used only by the up-front region probe. max_attempts=0 is one genuine attempt: botocore maps
# max_attempts=N to N+1 total attempts in every retry mode, so 1 here would cost two round trips
# against a region that is already known to be slow.
PROBE_CONFIG = Config(connect_timeout=3, read_timeout=5, retries={"max_attempts": 0})


def configure_session(session):
    """Apply the shared connect/read timeout config to every client created from this session."""
    session._session.set_default_client_config(CLIENT_CONFIG)
    return session


def progress(msg):
    """Lightweight progress line, always shown, written to stderr so it never pollutes JSON/stdout."""
    print(msg, file=sys.stderr, flush=True)


def debug(msg):
    """Fine-grained per-call progress, only shown with --verbose."""
    if VERBOSE:
        print(msg, file=sys.stderr, flush=True)


CATEGORIES = [
    {
        "category": "VM",
        # sagemaker:space is a Studio space with an app running on it. Spaces and notebook
        # instances that are not running move to Non-OS Disk, the same as a stopped EC2
        # instance (footnote 26).
        "resource_types": ["ec2:instance", "lightsail:instance", "sagemaker:notebook-instance",
                           "sagemaker:space"],
    },
    {
        "category": "VM Image",
        "resource_types": ["ec2:image"],
    },
    {
        "category": "Serverless Container",
        # ecs:service and ecs:standalone-task are one counting rule -- Fargate workloads at
        # supervisor granularity -- reported as two resource types. A service supervises its
        # replicas so the service is the workload; a RunTask or scheduled task has nothing
        # above it, so the task is its own supervisor. They are separated because they are
        # reachable by different instruments: ECS::Service is an AWS Config resource type and
        # a bare task is not, so keeping them fused would force the whole row onto the floor
        # rung or blend two instruments inside one number.
        "resource_types": ["apprunner:service", "eks:fargate-profile", "ecs:service",
                           "ecs:standalone-task", "lightsail:container-service"],
    },
    {
        "category": "Container Image",
        "resource_types": ["ecr:image"],
    },
    {
        "category": "Serverless Function",
        "resource_types": ["lambda:function"],
    },
    {
        "category": "Managed DB (PaaS)",
        "resource_types": [
            "rds:db",
            "rds:cluster",
            "dynamodb:table",
            "elasticache:cachecluster",
            "elasticache:replication-group",
            "memorydb:cluster",
            "docdb:cluster",
            "neptune:db-cluster",
            "opensearch:domain",
            "cassandra:keyspace",
            "timestream:database",
            # A cluster is counted once, and only instances outside any cluster count on their
            # own line: the rds:cluster / rds:db split.
            "timestream-influxdb:db-cluster",
            "timestream-influxdb:db-instance",
        ],
    },
    {
        "category": "Data Warehouse",
        "resource_types": [
            "redshift:cluster",
            "redshift-serverless:namespace",
            "emr:cluster",
            "emr-serverless:application",
        ],
    },
    {
        "category": "Bucket Storage",
        "resource_types": ["s3:bucket", "glacier:vault", "backup:backup-vault"],
    },
    {
        "category": "Non-OS Disk",
        # The two sagemaker types are ML environments with nothing running: their storage
        # volume persists in an AWS-managed account, where no ec2:volume can see it.
        "resource_types": ["ec2:volume", "sagemaker:stopped-notebook-instance",
                           "sagemaker:idle-space"],
    },
]


def probe_region(session, region):
    """Return True if the region's EC2 endpoint answers at all.

    A ClientError means AWS replied — the region is up and the call was merely refused
    (most often AccessDenied), so the region stays in the scan. Only a transport-level
    failure marks a region unreachable, which is what an offline region looks like.
    """
    try:
        ec2 = session.client("ec2", region_name=region, config=PROBE_CONFIG)
        ec2.describe_availability_zones()
        return True
    except ClientError:
        return True
    except BotoCoreError:
        return False


def get_regions(session, ec2_client, region_filter=None, skip_regions=None):
    """Return (scanned, skipped, unreachable) region lists for this account.

    Regions are probed once up front rather than discovered to be dead 28 resource types
    later: under CLIENT_CONFIG an offline region costs three attempts of up to 40s on every
    call, which is minutes per account. Unreachable regions are reported rather than silently
    dropped, so a network blip that wrongly condemns a region is visible in the output.
    """
    if region_filter:
        return [region_filter], [], []

    resp = ec2_client.describe_regions(Filters=[{"Name": "opt-in-status", "Values": ["opt-in-not-required", "opted-in"]}])
    all_regions = sorted(r["RegionName"] for r in resp["Regions"])

    skip = skip_regions or set()
    skipped = [r for r in all_regions if r in skip]
    if skipped:
        progress(f"  skipping region(s) on request: {', '.join(skipped)}")

    candidates = [r for r in all_regions if r not in skip]
    progress(f"  probing {len(candidates)} region(s) for reachability...")
    scanned, unreachable = [], []
    for region in candidates:
        if probe_region(session, region):
            scanned.append(region)
        else:
            unreachable.append(region)

    if unreachable:
        print(
            f"  WARNING: {len(unreachable)} region(s) did not respond and will NOT be counted: "
            f"{', '.join(unreachable)}",
            file=sys.stderr,
        )
    return scanned, skipped, unreachable


_SERVICE_REGION_CACHE = {}

# resource_type prefixes that don't match their boto3 client id
_SERVICE_NAME_ALIASES = {
    "timestream": "timestream-write",
    "cassandra": "keyspaces",
}


# Services botocore's bundled endpoint data does not list yet. Without an entry here a service
# like this is skipped in every region and reports null forever. The list is used only while
# botocore returns nothing, so upgrading boto3 takes over once its data catches up.
# timestream-influxdb: https://docs.aws.amazon.com/general/latest/gr/timestream.html,
# checked 2026-09-25 (botocore 1.43.71 listed no regions).
_SERVICE_REGION_FALLBACK = {
    "timestream-influxdb": {
        "us-east-1", "us-east-2", "us-west-2", "af-south-1", "ap-east-1", "ap-south-1",
        "ap-south-2", "ap-southeast-1", "ap-southeast-2", "ap-southeast-3", "ap-southeast-4",
        "ap-southeast-7", "ap-northeast-1", "ap-northeast-2", "ap-northeast-3", "ca-central-1",
        "eu-central-1", "eu-central-2", "eu-west-1", "eu-west-2", "eu-west-3", "eu-south-1",
        "eu-south-2", "eu-north-1", "il-central-1", "me-central-1", "mx-central-1", "sa-east-1",
    },
}


def get_service_regions(session, resource_type, candidate_regions):
    """Filter candidate_regions to those where the resource type's service has an endpoint.

    Uses botocore's bundled endpoint data (updated with every boto3/botocore release)
    rather than a hardcoded region list, so newly-added region support is picked up
    just by upgrading the boto3 dependency.
    """
    service_name = resource_type.split(":", 1)[0]
    service_name = _SERVICE_NAME_ALIASES.get(service_name, service_name)
    if service_name not in _SERVICE_REGION_CACHE:
        _SERVICE_REGION_CACHE[service_name] = (set(session.get_available_regions(service_name))
                                               or _SERVICE_REGION_FALLBACK.get(service_name, set()))
    supported = _SERVICE_REGION_CACHE[service_name]
    return [r for r in candidate_regions if r in supported]


_MANIFEST_LIST_MEDIA_TYPES = {
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
}

# Keyspaces reports these in every account where the API is reachable, whether or
# not the customer has ever created a keyspace.
_SYSTEM_KEYSPACES = {
    "system",
    "system_schema",
    "system_schema_mcs",
    "system_multiregion_info",
}

# DocumentDB and Neptune share the RDS control plane, so describe_db_clusters can
# return their clusters alongside real RDS ones; they're counted by docdb:cluster
# and neptune:db-cluster instead.
_NON_RDS_ENGINES = {"docdb", "neptune"}

# Instance states that count on the VM line. "stopped" is deliberately absent: a stopped
# instance leaves the VM line and its boot disk is re-admitted to Non-OS Disk instead, so it
# counts once, at the disk weight (resource-type-mapping.md footnote 26). "terminated" means
# destroyed on AWS -- not stopped -- and has never been counted.
#
# Transitional states count as still-a-VM: the instance stays allocated until it settles, and
# treating them otherwise makes counts flap during ordinary operations.
#
# get_root_devices() reads this same list, and that is load-bearing rather than tidy. The two
# halves of footnote 26 have to stay in sync -- a boot disk is excluded from Non-OS Disk
# exactly when its instance is counted here -- or a stopped instance with only a root volume
# scores zero on both lines and vanishes from the count entirely.
COUNTED_INSTANCE_STATES = ["pending", "running", "stopping", "shutting-down"]

# EMR cluster states that represent a live cluster. Shared by the point-in-time query and the
# Config rung for the same reason COUNTED_INSTANCE_STATES is shared: if the two lists drift,
# the two instruments silently count different things and only a side-by-side run would show it.
EMR_COUNTED_STATES = ("STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING")

# Notebook instance states with no instance running. Footnote 26 applied to SageMaker: these
# leave the VM line and count at the disk weight, because the ML storage volume persists. Every
# other state, transitional ones included, stays a VM, for the same reason as
# COUNTED_INSTANCE_STATES.
SAGEMAKER_NOTEBOOK_OFF_STATES = ("Stopped", "Failed")

# App states that mean a Studio space has compute behind it. Deleting is included for the same
# reason EC2's shutting-down is: the instance stays allocated until it settles.
SAGEMAKER_APP_RUNNING_STATES = ("InService", "Pending", "Deleting")


# ---------------------------------------------------------------------------
# Measurement ladder
# ---------------------------------------------------------------------------
#
# Governing principle: get the best measurement available, probe for it, and fall back to the
# next best -- per (resource type, account), not once per run. "Is this instrument available"
# is genuinely a per-type question: a customer can have Config recording five resource types
# and nothing else, and Cost Explorer can only speak for types billed by resource-time. Every
# count carries the rung that produced it, so the field can judge how far a number may sit
# from measured product usage instead of having to assume.
#
# The metric being reconstructed is the product's own: daily snapshots averaged over the
# month. Both AWS historical rungs satisfy that without sampling -- Cost Explorer returns a
# daily series in one call, and Config returns timestamps from which the series is arithmetic
# -- so there is no sampling cadence to choose in this script. The GCP script does have one,
# because read_time is a point parameter rather than a range.

WINDOW_DAYS = 30

TIME_RANGE_NOW = "now"
TIME_RANGE_WINDOW = f"{WINDOW_DAYS}d"

# Rung names double as the Method column's value, so they are short and name the instrument.
RUNG_CONFIG = "config"
RUNG_COST_EXPLORER = "cost-explorer"
RUNG_POINT_IN_TIME = "point-in-time"

# Used when the resource types inside one category did not all come from the same rung. The
# count is still their sum; the per-type detail stays in the JSON's provenance block, on the
# same reading of the parity rule that already keeps resource types out of the table.
METHOD_MIXED = "mixed"

# Probe verdicts. UNKNOWN exists for the loud-degradation rule: a probe that cannot tell "this
# instrument is unavailable here" from "this instrument returned nothing" must be read as
# neither. It does not promote the rung and it does not quietly demote it -- the rung is
# skipped, the ambiguity is tallied, and a canary reports it, so a probe nobody could read
# never silently decides a customer's number.
PROBE_AVAILABLE = "available"
PROBE_UNAVAILABLE = "unavailable"
PROBE_UNKNOWN = "unknown"


class Measurement:
    """One resource type's count in one account, with how it was obtained.

    `count` keeps exactly the meaning it had before the ladder existed: an integer, or None
    when the type could not be measured at all. Provenance rides alongside the number rather
    than wrapping it, so everything downstream that sums or canaries these counts is
    unaffected by which rung produced them.
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


class RungAbstained(Exception):
    """Raised by a rung that finds, only once measuring, that it cannot answer honestly.

    A probe is cheap and therefore has to be approximate. Some disqualifying facts -- how many
    resources of this type were deleted inside the window, say -- only emerge from the
    measurement itself. Abstaining hands the type to the next rung, which is a worse
    instrument but an honest one, rather than returning a number the rung does not stand
    behind.
    """


class Rung:
    """One instrument: a probe saying whether it can speak for a resource type here, and a
    measure producing the count when it can.

    Probe and measure are deliberately separate. An unavailable instrument then costs one
    cheap call rather than one failed expensive one, and the reason a rung was skipped can be
    recorded rather than inferred from an exception.
    """

    name = None
    time_range = TIME_RANGE_NOW

    def probe(self, rt, ctx):
        raise NotImplementedError

    def measure(self, rt, ctx):
        raise NotImplementedError


class PointInTimeRung(Rung):
    """The floor of the ladder: enumerate what exists at the instant of the run.

    Always available -- it is what the script did before the ladder existed, and it is what
    every other rung falls back to. It counts stopped and otherwise idle resources by
    construction, which is why it stays a defensible answer rather than merely a last resort.
    """

    name = RUNG_POINT_IN_TIME
    time_range = TIME_RANGE_NOW

    def probe(self, rt, ctx):
        return PROBE_AVAILABLE

    def measure(self, rt, ctx):
        return Measurement(count_type_point_in_time(rt, ctx), rung=self.name,
                           time_range=self.time_range)


# Cost Explorer speaks in usage types, not resource types, and it is admissible only where
# the billing unit is resource-time *for the thing being counted*, so that dividing by the
# period recovers that count. That is far narrower than "types AWS bills by the hour":
# node-hours under an ElastiCache cluster or an OpenSearch domain recover a node count, not a
# cluster or domain count, and swapping one for the other is the same unit mismatch that
# findings 12 and 13 were about.
#
# Managed DB is excluded by ruling, not by oversight: RDS instance-hours are exactly what CE
# meters, and unlike VMs there is no second row for a stopped database to land on, so that
# row's ladder is Config -> point-in-time with no CE tier. That ruling deleted most of this
# table, which is why it is three lines rather than the ten first sketched.
#
# The table is deliberately conservative. A type absent from it falls through to the next rung
# rather than being approximated, and unmatched_usage_types_hourly reports usage types billed
# in hours that no pattern claimed -- which is how a usage type AWS adds later surfaces as a
# visible line instead of a silent undercount.
CE_USAGE_RULES = {
    "ec2:instance": {
        "service": "Amazon Elastic Compute Cloud - Compute",
        # Reserved and Savings-Plan instances still meter as BoxUsage; ServerlessV2-style
        # capacity units and EBS volume usage are billed in other units and are filtered out
        # by the Hrs check rather than by these patterns.
        "patterns": ("BoxUsage", "SpotUsage", "DedicatedUsage"),
    },
}

# Cost Explorer trails real usage by roughly a day, so the window ends on the last *complete*
# day rather than today. Otherwise the final bucket is a partial day that drags the average
# down by a fraction nobody can see.
CE_LAG_DAYS = 1

# Hours of resource-time that equal one resource present for one day. Dividing by this is what
# turns a meter reading back into a count, and is the entire basis on which billing is
# admissible here at all.
CE_HOURS_PER_DAY = 24


class CostExplorerRung(Rung):
    """Average concurrent count recovered from metered resource-time.

    CE returns the whole daily series in a single call, so this rung satisfies the product's
    daily-snapshot-averaged-over-the-month metric exactly, with no sampling.

    It reports only running resources. For the VM row that is now correct by definition rather
    than a limitation: stopped instances were moved onto the disk line, so a meter that cannot
    see them is measuring precisely the right set.
    """

    name = RUNG_COST_EXPLORER
    time_range = TIME_RANGE_WINDOW

    def probe(self, rt, ctx):
        rule = CE_USAGE_RULES.get(rt)
        if rule is None:
            return PROBE_UNAVAILABLE
        if ctx.args.dry_run:
            return PROBE_UNAVAILABLE

        cached = ctx.ce_probe_cache.get(rule["service"])
        if cached is not None:
            return cached
        verdict = self._probe_service(rule, ctx)
        ctx.ce_probe_cache[rule["service"]] = verdict
        return verdict

    def _probe_service(self, rule, ctx):
        """Ask for one complete day and decide what the answer means.

        Three outcomes, and the third is the one that matters: a refusal is UNAVAILABLE, rows
        billed in hours are AVAILABLE, and *no rows at all* is UNKNOWN -- because an account
        with no running instances and an account where Cost Explorer holds no data are
        indistinguishable from here, and guessing either way would silently decide a number.
        """
        end = datetime.now(timezone.utc).date() - timedelta(days=CE_LAG_DAYS)
        start = end - timedelta(days=1)
        try:
            resp = self._query(ctx, rule, start, end)
        except (ClientError, BotoCoreError) as e:
            code = getattr(e, "response", {}).get("Error", {}).get("Code", type(e).__name__)
            DIAG.record_denial("ce:GetCostAndUsage", code)
            # A refusal is unambiguous; anything else (a throttle, a transport failure) leaves
            # the instrument's availability genuinely unknown rather than absent.
            return PROBE_UNAVAILABLE if "AccessDenied" in code or "Unauthorized" in code else PROBE_UNKNOWN
        hourly, _ = self._split_groups(resp, rule)
        return PROBE_AVAILABLE if hourly else PROBE_UNKNOWN

    def _query(self, ctx, rule, start, end, token=None):
        # CE is a global endpoint; us-east-1 is where it answers.
        ce = ctx.session.client("ce", region_name="us-east-1", config=CLIENT_CONFIG)
        kwargs = {
            "TimePeriod": {"Start": start.isoformat(), "End": end.isoformat()},
            "Granularity": "DAILY",
            "Metrics": ["UsageQuantity"],
            "Filter": {"Dimensions": {"Key": "SERVICE", "Values": [rule["service"]]}},
            "GroupBy": [{"Type": "DIMENSION", "Key": "USAGE_TYPE"}],
        }
        if token:
            kwargs["NextPageToken"] = token
        return ce.get_cost_and_usage(**kwargs)

    @staticmethod
    def _split_groups(resp, rule):
        """Partition one response's groups into matched hours per day, and unmatched hourly
        usage types.

        Only rows billed in hours are considered at all. That is what keeps EBS volume-months
        and serverless capacity units from ever reaching the pattern match, and it is also
        what makes the unmatched set meaningful: a usage type billed in hours under a counted
        service that no pattern claims is a real gap, not noise.
        """
        per_day = {}
        unmatched = set()
        for bucket in resp.get("ResultsByTime", []):
            day = bucket.get("TimePeriod", {}).get("Start")
            for group in bucket.get("Groups", []):
                usage_type = (group.get("Keys") or [""])[0]
                metric = group.get("Metrics", {}).get("UsageQuantity", {})
                if metric.get("Unit") != "Hrs":
                    continue
                amount = float(metric.get("Amount") or 0.0)
                if any(pat in usage_type for pat in rule["patterns"]):
                    per_day[day] = per_day.get(day, 0.0) + amount
                elif amount > 0:
                    unmatched.add(usage_type)
        return per_day, unmatched

    def measure(self, rt, ctx):
        rule = CE_USAGE_RULES[rt]
        end = datetime.now(timezone.utc).date() - timedelta(days=CE_LAG_DAYS)
        start = end - timedelta(days=WINDOW_DAYS)

        per_day = {}
        unmatched = set()
        token = None
        while True:
            resp = self._query(ctx, rule, start, end, token)
            day_hours, missed = self._split_groups(resp, rule)
            for day, hours in day_hours.items():
                per_day[day] = per_day.get(day, 0.0) + hours
            unmatched |= missed
            token = resp.get("NextPageToken")
            if not token:
                break

        if unmatched:
            DIAG.bump("ce_unmatched_hourly_usage_types", len(unmatched))

        if not per_day:
            # The probe already established that this account meters hours under this service,
            # so an empty window here is a real zero rather than an absent instrument.
            return Measurement(0, rung=self.name, time_range=self.time_range, points=0)

        # Hours of resource-time per day / 24 = resources present that day; the mean of those
        # is the average concurrent count, which is the product's own metric.
        daily_counts = [h / CE_HOURS_PER_DAY for h in per_day.values()]
        average = sum(daily_counts) / len(daily_counts)
        return Measurement(round(average), rung=self.name, time_range=self.time_range,
                           points=len(daily_counts))


def _dig(row, path):
    """Read a dotted path out of one advanced-query result row."""
    node = row
    for part in path.split("."):
        if not isinstance(node, dict):
            return None
        node = node.get(part)
    return node


def _select_all(client, expression):
    """Page one advanced query to exhaustion, yielding parsed rows."""
    token = None
    while True:
        kwargs = {"Expression": expression, "Limit": CONFIG_PAGE_SIZE}
        if token:
            kwargs["NextToken"] = token
        resp = client.select_resource_config(**kwargs)
        for raw in resp.get("Results", []):
            yield json.loads(raw)
        token = resp.get("NextToken")
        if not token:
            return


def _config_root_devices(client):
    """instanceId -> rootDeviceName, for instances that count on the VM line.

    The Config equivalent of get_root_devices(), and restricted the same way: only counted
    instances contribute, which is what re-admits a stopped instance's boot disk to Non-OS
    Disk under footnote 26.
    """
    roots = {}
    expression = ("SELECT resourceId, configuration.rootDeviceName, configuration.state.name "
                  "WHERE resourceType = 'AWS::EC2::Instance'")
    for row in _select_all(client, expression):
        if _dig(row, "configuration.state.name") in COUNTED_INSTANCE_STATES:
            roots[row.get("resourceId")] = _dig(row, "configuration.rootDeviceName")
    return roots


def _instance_state_filter(client):
    return lambda row: _dig(row, "configuration.state.name") in COUNTED_INSTANCE_STATES


def _emr_state_filter(client):
    return lambda row: _dig(row, "configuration.status.state") in EMR_COUNTED_STATES


def _ecs_active_filter(client):
    return lambda row: _dig(row, "configuration.status") == "ACTIVE"


def _volume_root_filter(client):
    roots = _config_root_devices(client)

    def _keep(row):
        for attachment in _dig(row, "configuration.attachments") or []:
            if roots.get(attachment.get("instanceId")) == attachment.get("device"):
                return False
        return True
    return _keep


def _ecs_fargate_filter(row):
    """The Config-shaped twin of _is_fargate, on a service's recorded configuration."""
    configuration = _dig(row, "configuration") or {}
    if configuration.get("launchType") == "FARGATE":
        return True
    for item in configuration.get("capacityProviderStrategy") or []:
        if item.get("capacityProvider") in FARGATE_CAPACITY_PROVIDERS:
            return True
    return False


def _dynamodb_counting_region(own_region, replica_regions, scanned_regions):
    """The one region in which a DynamoDB table is counted.

    A global table is a set of replica tables, one per region, each a full table in its own
    region's ListTables. It counts once (footnote 23), in the alphabetically first region of
    its replica set that this run actually read -- a rule every replica evaluates the same way,
    so exactly one of them keeps it. A table with no replicas is counted in its own region.
    """
    regions = {own_region}
    regions.update(r for r in replica_regions if r)
    return min(regions & set(scanned_regions))


def _dynamodb_config_replica_filter(region, scanned_regions):
    def _keep(row):
        replicas = _dig(row, "configuration.replicas") or []
        named = [r.get("regionName") for r in replicas if isinstance(r, dict)]
        return _dynamodb_counting_region(region, named, scanned_regions) == region
    return _keep


def _global_secondary_filter(session, region, service):
    secondaries = global_secondary_arns(session, region, service)
    return lambda row: row.get("arn") not in secondaries


def _dynamodb_config_observe_items(row):
    """Tally empty tables among those counted, for the empty-table question (DATA-5374).

    A measure only: it never moves the count. The item count is whatever Config's latest
    configuration item recorded, which may predate data arriving, so this figure is reported
    separately from the live one rather than merged with it.
    """
    items = _dig(row, "configuration.itemCount")
    if items is None:
        DIAG.bump("dynamodb_item_counts_unread_config")
        return
    DIAG.bump("dynamodb_item_counts_read_config")
    if items == 0:
        DIAG.bump("dynamodb_tables_empty_config")


# What Config can and cannot stand in for.
#
# The cheap Config path yields an existence interval per resource plus its *current*
# configuration. A counting filter is therefore reproducible exactly when the property it
# tests is static over the resource's lifetime, and only then. That distinction, not the mere
# presence of a filter, is what decides membership here:
#
#   static, so current config is an exact proxy -- RDS/Neptune engine (an engine type cannot
#   be changed after creation, only its version), RDS cluster membership, EKS Fargate profiles
#   (documented as immutable), Cassandra system keyspace names.
#
#   lifecycle state, which is volatile but handled -- ec2:instance power state, emr:cluster
#   status, ecs:service ACTIVE, and the boot-disk join that ec2:volume inherits from footnote
#   26. These go in "current_filter", which is applied ONLY to resources that still exist. A
#   departed resource's interval already encodes its life, so re-applying a lifecycle filter
#   to its final configuration item would drop every terminated cluster and every deleted
#   instance -- exactly the workloads the window is meant to capture.
#
#   For resources that still exist, a lifecycle filter does carry an assumption: today's state
#   is taken to have held all window. That is the same assumption point-in-time already makes,
#   applied to a real existence history rather than to a single instant, so it is never worse
#   than the rung below.
#
# Absent for mapping reasons, not volatility: ecr:image (Config records repositories, not
# images), s3:bucket (counted from one global endpoint here but recorded regionally there),
# docdb:cluster (whether DocumentDB surfaces as AWS::RDS::DBCluster needs a live account),
# ecs:standalone-task (Config has TaskDefinition, TaskSet and PrimaryTaskSet but no Task --
# which is why it was split out of ecs:service), sagemaker:notebook-instance (its running and
# stopped halves now sit on different lines, and whether Config's configuration item carries
# the notebook's status is unverified; guessing would put stopped notebooks back on the VM
# line), elasticache:cachecluster (replication-group members must be excluded, and whether
# Config's configuration item carries replicationGroupId is unverified; a filter on a field
# that is never recorded would keep every member and silently restore the double count),
# elasticache:replication-group (a Global Datastore secondary must be excluded, for the same
# unverified-field reason), cassandra:keyspace (Config records no replication fields, so a
# multi-region keyspace would count once per region), and the types Config does not support
# at all: memorydb:cluster, glacier:vault,
# timestream:database, redshift-serverless:namespace, lightsail:instance,
# lightsail:container-service, sagemaker:space (Config records Domain and UserProfile, not
# Space or App). The timestream-influxdb types have not been checked against Config.
#
# "select" names extra fields a filter needs; "keep" is a static filter applied to current and
# departed resources alike; "keep_in_region" is the same but built per region from the regions
# being scanned; "current_filter" is a factory built once per region whose predicate applies
# only to resources that still exist; "live_filter" is the same but built from the account's
# session and region, for a filter that needs a live API call rather than Config; "observe"
# sees each counted resource that still exists, for measures, and never changes the count.
# Anything absent from this table falls through.
CONFIG_TYPE_RULES = {
    "ec2:image": {"types": ("AWS::EC2::Image",)},
    "lambda:function": {"types": ("AWS::Lambda::Function",)},
    "backup:backup-vault": {"types": ("AWS::Backup::BackupVault",)},
    # A global table's replicas are each recorded as a table in their own region. Which
    # regions hold replicas can change over a table's life, but every replica sees the same
    # set, so the static rule picks the same one region for all of them (footnote 23).
    "dynamodb:table": {
        "types": ("AWS::DynamoDB::Table",),
        "select": ("configuration.replicas.regionName", "configuration.itemCount"),
        "keep_in_region": _dynamodb_config_replica_filter,
        "observe": _dynamodb_config_observe_items,
    },
    # Legacy domains are still recorded under the Elasticsearch type, and list_domain_names
    # returns both, so both have to be asked for or the two paths disagree.
    "opensearch:domain": {"types": ("AWS::OpenSearch::Domain", "AWS::Elasticsearch::Domain")},
    "redshift:cluster": {"types": ("AWS::Redshift::Cluster",)},
    "emr-serverless:application": {"types": ("AWS::EMRServerless::Application",)},
    "apprunner:service": {"types": ("AWS::AppRunner::Service",)},
    "eks:fargate-profile": {"types": ("AWS::EKS::FargateProfile",)},
    "neptune:db-cluster": {
        "types": ("AWS::Neptune::DBCluster",),
        "select": ("arn",),
        "live_filter": lambda session, region: _global_secondary_filter(session, region, "neptune"),
    },
    "rds:db": {
        "types": ("AWS::RDS::DBInstance",),
        "select": ("configuration.dBClusterIdentifier",
                   "configuration.readReplicaSourceDBInstanceIdentifier"),
        # Cluster members are counted on the cluster line, not here. An instance cannot move
        # between clusters, so its membership is fixed for its lifetime. A read replica is a
        # copy (footnote 23); one promoted inside the window counts for all of it. Config does
        # not record readReplicaSourceDBClusterIdentifier, so an instance replicating from an
        # Aurora cluster still counts on this rung.
        "keep": lambda r: not _dig(r, "configuration.dBClusterIdentifier")
        and not _dig(r, "configuration.readReplicaSourceDBInstanceIdentifier"),
    },
    # Config records a cluster's replication source but not its global-database membership,
    # so secondaries that still exist are found from the live member list; one that left
    # inside the window still counts for its life.
    "rds:cluster": {
        "types": ("AWS::RDS::DBCluster",),
        "select": ("configuration.engine", "configuration.replicationSourceIdentifier", "arn"),
        "keep": lambda r: _dig(r, "configuration.engine") not in _NON_RDS_ENGINES
        and not _dig(r, "configuration.replicationSourceIdentifier"),
        "live_filter": lambda session, region: _global_secondary_filter(session, region, "rds"),
    },
    # Ranked below Cost Explorer, which meters running hours directly and so measures the VM
    # line exactly. This rung only carries it when CE is denied, where it still beats a
    # snapshot: same power-state assumption, real existence history.
    "ec2:instance": {
        "types": ("AWS::EC2::Instance",),
        "select": ("configuration.state.name",),
        "current_filter": _instance_state_filter,
    },
    "ec2:volume": {
        "types": ("AWS::EC2::Volume",),
        "select": ("configuration.attachments",),
        "current_filter": _volume_root_filter,
    },
    "emr:cluster": {
        "types": ("AWS::EMR::Cluster",),
        "select": ("configuration.status.state",),
        "current_filter": _emr_state_filter,
    },
    # Only the supervisor half of the row; ecs:standalone-task has no Config type and stays on
    # the floor rung, which is what splitting the two resource types made possible.
    "ecs:service": {
        "types": ("AWS::ECS::Service",),
        "select": ("configuration.launchType", "configuration.capacityProviderStrategy",
                   "configuration.status"),
        "keep": _ecs_fargate_filter,
        "current_filter": _ecs_active_filter,
    },
}

# Below this share of dated arrivals the reconstruction is leaning on guesswork hard enough to
# say so. Every deleted resource costs one GetResourceConfigHistory call to date; this measures
# how many of those calls actually came back with an answer.
CONFIG_MIN_DATED_FRACTION = 0.8

# Dating deletions costs one call each, so the rung's cost scales with churn -- which is the
# right shape (a stable fleet is nearly free) but needs a ceiling. Past this many deletions for
# one type in one account the rung abstains rather than spending thousands of calls or falling
# back to assuming the arrivals, which on a high-churn type is catastrophically wrong: a fleet
# of two-hour EMR job clusters assumed to predate the window over-counts by orders of magnitude.
CONFIG_MAX_DELETION_LOOKUPS = 500

# AWS documents GetResourceConfigHistory as spanning at most seven days per call. Anchoring the
# window at the known deletion time guarantees the deletion item itself falls inside it.
CONFIG_HISTORY_SPAN_DAYS = 7

# Advanced query pages at 100 rows; so does ListDiscoveredResources.
CONFIG_PAGE_SIZE = 100


class ConfigRung(Rung):
    """Average count over the window, reconstructed from AWS Config.

    A state instrument rather than a meter: it answers what existed, so it includes stopped
    and otherwise idle resources by construction. That is why it is the right rung for Managed
    DB, whose ladder was ruled to be Config then point-in-time with no Cost Explorer tier.

    It reconstructs the series cheaply -- creation times for everything that currently exists
    via one paged advanced query, deletion times for everything that has gone via
    ListDiscoveredResources. The expensive alternative, GetResourceConfigHistory, takes a
    single resourceId and spans at most seven days per call, so an exact series would cost
    roughly five calls per resource; this costs a page per hundred.

    Deleted resources are dated rather than assumed. ListDiscoveredResources gives a deletion
    time and nothing else, but resourceCreationTime is carried on *every* configuration item,
    so one GetResourceConfigHistory call per deleted resource recovers its arrival -- and the
    item carries its configuration too, so a filtered type can classify its deletions instead
    of dropping them. That is one call per deletion, not the five-per-resource an exact series
    would cost, and the cost scales with churn rather than with fleet size.
    """

    name = RUNG_CONFIG
    time_range = TIME_RANGE_WINDOW

    def probe(self, rt, ctx):
        rule = CONFIG_TYPE_RULES.get(rt)
        if rule is None or ctx.args.dry_run:
            return PROBE_UNAVAILABLE

        verdicts = set()
        for region in ctx.regions:
            verdicts.add(self._probe_region(ctx, region, rule["types"]))
        if PROBE_UNKNOWN in verdicts:
            return PROBE_UNKNOWN
        # All-or-nothing across regions. Smoothing the regions Config covers and snapshotting
        # the rest would put a blended number in a single row.
        return PROBE_AVAILABLE if verdicts == {PROBE_AVAILABLE} else PROBE_UNAVAILABLE

    def _probe_region(self, ctx, region, config_types):
        recorders = ctx.config_recorder_cache.get(region)
        if recorders is None:
            recorders = self._describe_recorders(ctx, region)
            ctx.config_recorder_cache[region] = recorders
        if recorders in (PROBE_UNAVAILABLE, PROBE_UNKNOWN):
            return recorders
        return PROBE_AVAILABLE if self._covers(recorders, config_types) else PROBE_UNAVAILABLE

    @staticmethod
    def _describe_recorders(ctx, region):
        """Recorders that are actually recording, with their recording groups.

        "Is Config on" is not a boolean and not even per account: it is per resource type and
        per region. A customer can have a recorder running that records five types.
        """
        client = ctx.session.client("config", region_name=region, config=CLIENT_CONFIG)
        try:
            recorders = client.describe_configuration_recorders().get("ConfigurationRecorders", [])
            status = client.describe_configuration_recorder_status().get(
                "ConfigurationRecordersStatus", [])
        except (ClientError, BotoCoreError) as e:
            code = getattr(e, "response", {}).get("Error", {}).get("Code", type(e).__name__)
            DIAG.record_denial("config:DescribeConfigurationRecorders", code)
            if "AccessDenied" in code or "Unauthorized" in code:
                return PROBE_UNAVAILABLE
            # A recorder that exists but could not be described leaves coverage genuinely
            # unknown, which must not be read as "not recording".
            return PROBE_UNKNOWN
        recording = {s.get("name") for s in status if s.get("recording")}
        return [r for r in recorders if r.get("name") in recording]

    @staticmethod
    def _covers(recorders, config_types):
        for recorder in recorders:
            group = recorder.get("recordingGroup") or {}
            if group.get("allSupported"):
                excluded = set((group.get("exclusionByResourceTypes") or {}).get("resourceTypes") or [])
                if not any(t in excluded for t in config_types):
                    return True
            elif set(config_types) <= set(group.get("resourceTypes") or []):
                # Every mapped type must be recorded. A partially recorded type is a silent
                # undercount, which is worse than declining to the next rung.
                return True
        return False

    def measure(self, rt, ctx):
        rule = CONFIG_TYPE_RULES[rt]
        now = datetime.now(timezone.utc)
        window_start = now - timedelta(days=WINDOW_DAYS)

        intervals, undated = [], 0
        for region in ctx.regions:
            client = ctx.session.client("config", region_name=region, config=CLIENT_CONFIG)
            try:
                found, missed = self._collect(client, rule, window_start, now,
                                              region, ctx.regions, ctx.session)
            except (ClientError, BotoCoreError) as e:
                code = getattr(e, "response", {}).get("Error", {}).get("Code", type(e).__name__)
                DIAG.record_denial("config:SelectResourceConfig", code)
                # The probe said Config covers this type here, so a failure now is a real
                # measurement failure rather than an absent instrument.
                return Measurement(None, rung=self.name, time_range=self.time_range)
            intervals.extend(found)
            undated += missed

        if intervals or undated:
            DIAG.measure("config_arrivals_dated_fraction",
                         round(len(intervals) / (len(intervals) + undated), 3))
        if undated:
            DIAG.bump("config_undated_deletions", undated)

        series = self._daily_series(intervals, window_start, now)
        average = sum(series) / len(series)
        return Measurement(round(average), rung=self.name, time_range=self.time_range,
                           points=len(series))

    def _collect(self, client, rule, window_start, now, region, scanned_regions, session):
        """Existence intervals for one type in one region, as (created, deleted) pairs.

        Returns the intervals it could establish and a count of the deletions it could not
        date, which is what config_arrivals_dated reports on.
        """
        fields = ["resourceId", "resourceName", "resourceCreationTime"]
        fields += list(rule.get("select") or ())
        keep = rule.get("keep")
        if rule.get("keep_in_region"):
            keep = rule["keep_in_region"](region, scanned_regions)
        factory = rule.get("current_filter")
        observe = rule.get("observe")
        # Built once per region: the volume filter runs its own query to map root devices.
        keep_current = factory(client) if factory else None
        if rule.get("live_filter"):
            keep_live = rule["live_filter"](session, region)
            keep_current = (keep_live if keep_current is None
                            else lambda r, a=keep_current, b=keep_live: a(r) and b(r))

        intervals = []
        for config_type in rule["types"]:
            expression = (f"SELECT {', '.join(fields)} "
                          f"WHERE resourceType = '{config_type}'")
            for row in _select_all(client, expression):
                if keep and not keep(row):
                    continue
                if keep_current and not keep_current(row):
                    continue
                if observe:
                    observe(row)
                # Still here, so the interval is open-ended.
                intervals.append((_parse_config_time(row.get("resourceCreationTime")), None))

        # Resources that have gone. ListDiscoveredResources dates their departure but says
        # nothing else, so each one costs a history call to recover its arrival -- and that
        # item carries its configuration, so a filtered type can classify its deletions rather
        # than dropping them.
        departures = []
        for config_type in rule["types"]:
            token = None
            while True:
                kwargs = {"resourceType": config_type, "includeDeletedResources": True,
                          "limit": CONFIG_PAGE_SIZE}
                if token:
                    kwargs["nextToken"] = token
                resp = client.list_discovered_resources(**kwargs)
                for ident in resp.get("resourceIdentifiers", []):
                    gone = _parse_config_time(ident.get("resourceDeletionTime"))
                    # Resources that left before the window never overlap it. Config retention
                    # runs to years, so without this the list is mostly ancient history.
                    if gone is None or gone < window_start or gone > now:
                        continue
                    departures.append((config_type, ident.get("resourceId"), gone))
                token = resp.get("nextToken")
                if not token:
                    break

        if len(departures) > CONFIG_MAX_DELETION_LOOKUPS:
            raise RungAbstained(
                f"{len(departures)} deletions in the window exceeds the "
                f"{CONFIG_MAX_DELETION_LOOKUPS}-call dating budget"
            )

        undated = 0
        for config_type, resource_id, gone in departures:
            born, configuration = self._date_departure(client, config_type, resource_id, gone)
            if born is None:
                # An arrival that cannot be dated must not be invented; omitting it understates
                # a churning estate, which the canary reports, but never overstates one.
                undated += 1
                continue
            if keep is not None:
                if configuration is None:
                    # Cannot tell whether it belonged in this count, and inventing membership
                    # is worse than omitting it.
                    undated += 1
                    continue
                if not keep(configuration):
                    continue
            # keep_current is deliberately NOT applied here. It tests a lifecycle state, and
            # this resource's interval already says when it lived; filtering on the state it
            # held at deletion would discard every terminated cluster and deleted instance.
            intervals.append((born, gone))
        return intervals, undated

    @staticmethod
    def _date_departure(client, config_type, resource_id, gone):
        """Recover a departed resource's creation time and final configuration.

        resourceCreationTime rides on every configuration item, so any item will do; the
        deletion item may carry no configuration, hence asking for a couple and taking the
        most recent one that has any.
        """
        try:
            resp = client.get_resource_config_history(
                resourceType=config_type,
                resourceId=resource_id,
                earlierTime=gone - timedelta(days=CONFIG_HISTORY_SPAN_DAYS),
                laterTime=gone,
                chronologicalOrder="Reverse",
                limit=2,
            )
        except (ClientError, BotoCoreError) as e:
            code = getattr(e, "response", {}).get("Error", {}).get("Code", type(e).__name__)
            DIAG.record_denial("config:GetResourceConfigHistory", code)
            return None, None

        born, configuration = None, None
        for item in resp.get("configurationItems") or []:
            born = born or _parse_config_time(item.get("resourceCreationTime"))
            if configuration is None and item.get("configuration"):
                try:
                    configuration = {
                        "resourceName": item.get("resourceName"),
                        "configuration": json.loads(item["configuration"]),
                    }
                except (ValueError, TypeError):
                    pass
        return born, configuration

    @staticmethod
    def _daily_series(intervals, window_start, now):
        """One count per day across the window from (created, deleted) intervals.

        An open start means Config recorded no creation time, which for a resource still
        present means it predates the window; an open end means it is still there.
        """
        days = (now - window_start).days
        series = []
        for offset in range(days + 1):
            day = window_start + timedelta(days=offset)
            series.append(sum(
                1 for born, gone in intervals
                if (born is None or born <= day) and (gone is None or gone > day)
            ))
        return series


def _parse_config_time(value):
    """Config timestamps arrive as datetimes from botocore and as strings from the query."""
    if value is None or isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


# Best first. Rungs are appended to this list as they are built, and a ladder holding only the
# floor behaves exactly as the script did before the ladder existed -- which is what makes
# adding one rung a change that can be reviewed, and reverted, on its own.
# Cost Explorer outranks Config for the one type it claims. Since footnote 26 moved stopped
# instances off the VM line, a running-hours meter measures that line exactly, whereas Config
# would have to assume today's power state held all window. For every other type CE declines
# immediately and Config is the best instrument available.
LADDER = [CostExplorerRung(), ConfigRung(), PointInTimeRung()]


def measure_type(rt, ctx):
    """Walk the ladder for one (resource type, account) and return a Measurement.

    The walk stops at the first rung whose probe reports AVAILABLE. An UNKNOWN verdict does
    not stop it and does not count as a refusal: the rung is skipped, the ambiguity is
    tallied, and `ladder_probe_unambiguous` reports it.
    """
    for rung in LADDER:
        verdict = rung.probe(rt, ctx)
        DIAG.rung_trace(rt, rung.name, verdict)
        if verdict == PROBE_AVAILABLE:
            try:
                return rung.measure(rt, ctx)
            except RungAbstained as reason:
                DIAG.rung_trace(rt, rung.name, "abstained")
                DIAG.bump("ladder_rungs_abstained")
                debug(f"    {rung.name} abstained on {rt}: {reason}")
                continue
        if verdict == PROBE_UNKNOWN:
            DIAG.bump("ladder_probes_unknown")
    # Unreachable while the floor rung probes unconditionally, but a ladder that falls off its
    # own end must fail loudly rather than hand back a None that reads as "none found".
    raise RuntimeError(f"no rung could measure {rt}")


def category_provenance(provenance, breakdown):
    """Derive one category's Time Range and Method from the types that composed it.

    Types that could not be measured are excluded: they contribute nothing to the count, so
    they must not drag its label either way.
    """
    # Types contributing nothing must not drag the label. A type that failed says nothing
    # about how the number was obtained, and neither does one that measured a genuine zero:
    # a VM row built entirely from Cost Explorer should read "cost-explorer", not "mixed",
    # merely because the account also has no Lightsail instances. Zeroes are only consulted
    # when nothing at all contributed, so an empty category still gets an honest label.
    used = [p for rt, p in provenance.items() if breakdown.get(rt)]
    if not used:
        used = [p for rt, p in provenance.items() if breakdown.get(rt) is not None]
    if not used:
        return TIME_RANGE_NOW, RUNG_POINT_IN_TIME
    rungs = {p["rung"] for p in used}
    ranges = {p["time_range"] for p in used}
    return (ranges.pop() if len(ranges) == 1 else METHOD_MIXED,
            rungs.pop() if len(rungs) == 1 else METHOD_MIXED)


def merge_provenance(entries):
    """Combine one resource type's provenance across accounts.

    The ladder resolves per (type, account), so the same type can come from Config in one
    account and point-in-time in the next. A total that mixes them says so rather than
    claiming the better of the two.
    """
    entries = [e for e in entries if e]
    if not entries:
        return {"rung": RUNG_POINT_IN_TIME, "time_range": TIME_RANGE_NOW}
    rungs = {e["rung"] for e in entries}
    ranges = {e["time_range"] for e in entries}
    return {
        "rung": rungs.pop() if len(rungs) == 1 else METHOD_MIXED,
        "time_range": ranges.pop() if len(ranges) == 1 else METHOD_MIXED,
    }


# Minimum instance count before the disk-ratio canary is worth evaluating. Below this a
# demo-sized environment produces ratios that swing wildly on one disk, which is how a
# canary earns a reputation for crying wolf and gets ignored.
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
    codes only.** No resource names, ARNs, account IDs or region detail that would fingerprint
    a customer. The field engineer has to be able to read this block before forwarding it, and
    it travels through several pairs of hands to reach Product. Nothing is ever sent anywhere
    by the script itself.

    Two tiers, argued and settled: canaries and measures are computed on every run, because a
    diagnostic that has to be switched on only gets switched on by engineers who already
    suspect a problem -- the one sample that says least about how often filters break in the
    wild. The heavier bundle is opt-in.
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
        """Tally one probe verdict by (resource type, rung, verdict).

        Counts of accounts, never which accounts: this block travels to Product through
        several pairs of hands and must not fingerprint a customer. Aggregated this way it
        answers the question the ladder was built to ask -- how often each instrument is
        actually available in the field, per resource type.
        """
        key = f"{resource_type}/{rung}/{verdict}"
        self.rungs[key] = self.rungs.get(key, 0) + 1

    def record_denial(self, resource_type, code):
        """Tally one refusal by (resource type, error code). Codes only -- never messages,
        which quote ARNs and principal names."""
        key = f"{resource_type}/{code}"
        self.denials[key] = self.denials.get(key, 0) + 1

    def canary(self, name, ok, observed, note):
        """Record an assertion. ok=None means it could not be evaluated, which is reported as
        "skipped" rather than passing -- an unevaluated check must never read as a clean one."""
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
                "boto3": _pkg_version("boto3"),
                "botocore": _pkg_version("botocore"),
                "script_version": script_version(),
            }
        return out


DIAG = Diagnostics()


_ROOT_DEVICE_CACHE = {}
# Root devices of stopped instances, filled by the same walk. The count does not need them --
# a volume matching no counted instance is already admitted -- but the disk-reason breakdown
# does, to tell a re-admitted root volume from an ordinary data volume.
_STOPPED_ROOT_DEVICE_CACHE = {}


def get_root_devices(session, region):
    """Map instance ID -> root device name, for instances that count on the VM line.

    Restricting the map to counted instances is what re-admits a stopped instance's boot disk
    to Non-OS Disk: its root volume matches nothing here, so is_data_volume treats it as data.
    The exclusion exists because the VM line already accounts for that disk, so it has to
    lapse the moment the VM is not counted (footnote 26).

    The pass is deliberately unfiltered and partitioned in Python rather than filtered
    server-side, because the same walk yields the stopped-instance count. That number is not
    available anywhere else in the run -- the VM query filters those instances out at the API
    -- and the stopped fraction is one of the things field runs exist to measure. Terminated
    instances are walked as a result, which is a small cost: they linger about an hour.
    """
    # Keyed by session as well as region. An --all-accounts run scans every account in one
    # process, and a region-only key handed the first account's map to every later one, so
    # their running instances' boot disks matched nothing and were counted as data disks.
    # Holding the session in the key keeps it alive, so its identity cannot be reused.
    key = (session, region)
    if key not in _ROOT_DEVICE_CACHE:
        ec2 = session.client("ec2", region_name=region)
        devices = {}
        stopped = {}
        for page in ec2.get_paginator("describe_instances").paginate():
            for res in page["Reservations"]:
                for inst in res["Instances"]:
                    state = (inst.get("State") or {}).get("Name")
                    if state == "terminated":
                        continue
                    if state == "stopped":
                        DIAG.bump("instances_stopped")
                        stopped[inst["InstanceId"]] = inst.get("RootDeviceName")
                        continue
                    DIAG.bump("instances_counted")
                    devices[inst["InstanceId"]] = inst.get("RootDeviceName")
        _ROOT_DEVICE_CACHE[key] = devices
        _STOPPED_ROOT_DEVICE_CACHE[key] = stopped
    return _ROOT_DEVICE_CACHE[key]


def is_data_volume(volume, root_devices):
    """True unless the volume is attached to an instance as its root device."""
    for att in volume.get("Attachments", []):
        if root_devices.get(att.get("InstanceId")) == att.get("Device"):
            return False
    return True


# The EBS CSI driver tags every volume it creates with ebs.csi.aws.com/cluster, and both it
# and the in-tree provisioner tag the claim as kubernetes.io/created-for/pvc/name. Unverified
# on a live estate; a miss only moves a volume to another reason, never in or out of the count.
_KUBERNETES_VOLUME_TAGS = ("ebs.csi.aws.com/cluster", "kubernetes.io/created-for/pvc/name")


def volume_reason(volume, root_devices, stopped_root_devices):
    """Why a volume is or is not in Non-OS Disk, for the nonos_disk_reasons breakdown.

    The first test is the count's own (is_data_volume), so boot_disk_excluded is exactly the
    volumes the count leaves out and every other reason sums to the present-day figure. The
    rest only split the counted volumes: Kubernetes volumes, attached data volumes, and
    unattached volumes, the last divided by whether the volume was made from a snapshot,
    which every root volume is (from its AMI's snapshot) and most data volumes are not.
    """
    if not is_data_volume(volume, root_devices):
        return "boot_disk_excluded"
    attachments = volume.get("Attachments") or []
    for att in attachments:
        if stopped_root_devices.get(att.get("InstanceId")) == att.get("Device"):
            return "boot_disk_stopped_instance"
    tags = {t.get("Key") for t in volume.get("Tags") or []}
    if tags & set(_KUBERNETES_VOLUME_TAGS):
        return "kubernetes_volume"
    if attachments:
        return "data_disk_attached"
    if volume.get("SnapshotId"):
        return "unattached_from_snapshot"
    return "data_disk_unattached"


def probe_nonos_disk_reasons(ctx):
    """Classify the account's volumes by why each is or is not in Non-OS Disk, as of now.

    nonos_disks_per_instance says only that volumes outnumber instances. A disk-heavy estate
    (several data volumes per database host, volumes nobody deleted, EKS persistent volumes)
    trips it on correct numbers, so the reasons are what tell a real estate from a broken
    filter. A walk of its own rather than a by-product of the count, because the volume count
    may have come from AWS Config, which sees no tags; the extra cost is one paginated
    DescribeVolumes per region. A region that fails withholds the whole breakdown, since a
    partial one would no longer sum to the count.
    """
    if DIAG.measures.get("nonos_disk_reasons", {}) is None:
        return
    reasons = {}
    for region in get_service_regions(ctx.session, "ec2:volume", ctx.regions):
        try:
            root_devices = get_root_devices(ctx.session, region)
            stopped = _STOPPED_ROOT_DEVICE_CACHE.get((ctx.session, region), {})
            ec2 = ctx.session.client("ec2", region_name=region)
            for page in ec2.get_paginator("describe_volumes").paginate():
                for volume in page.get("Volumes", []):
                    reason = volume_reason(volume, root_devices, stopped)
                    reasons[reason] = reasons.get(reason, 0) + 1
        except (ClientError, BotoCoreError) as e:
            code = getattr(e, "response", {}).get("Error", {}).get("Code", type(e).__name__)
            DIAG.record_denial("nonos_disk_reason_probe", code)
            DIAG.measure("nonos_disk_reasons", None)
            return
    totals = DIAG.measures.setdefault("nonos_disk_reasons", {})
    for reason, n in reasons.items():
        totals[reason] = totals.get(reason, 0) + n


FARGATE_CAPACITY_PROVIDERS = ("FARGATE", "FARGATE_SPOT")


def _is_fargate(obj):
    """True if an ECS service or task runs on Fargate.

    Both launchType and capacityProviderStrategy/capacityProviderName have to be
    checked. Service.launchType is documented as omitted when the service was
    created with a capacity provider strategy, so filtering on launchType alone
    silently misses every Fargate service built the console's default way.
    """
    if obj.get("launchType") == "FARGATE":
        return True

    # Fargate reached only through a capacity provider, with launchType absent, is exactly the
    # shape that makes ListServices' server-side launchType filter return a clean wrong
    # answer. Tallying these is what will tell us from field runs whether avoiding that filter
    # was load-bearing or merely cautious.
    def _via_capacity_provider():
        if not obj.get("launchType"):
            DIAG.bump("fargate_via_capacity_provider_only")
        return True

    if obj.get("capacityProviderName") in FARGATE_CAPACITY_PROVIDERS:
        return _via_capacity_provider()
    for item in obj.get("capacityProviderStrategy", []):
        if item.get("capacityProvider") in FARGATE_CAPACITY_PROVIDERS:
            return _via_capacity_provider()
    return False


def _count_fargate_services(ecs, cluster_arn):
    """Count ACTIVE Fargate services in one ECS cluster.

    ListServices is deliberately called without its launchType filter -- see
    _is_fargate for why that filter cannot be trusted.
    """
    arns = list(_iter_paginated(ecs, "list_services", "serviceArns", cluster=cluster_arn))
    total = 0
    for i in range(0, len(arns), 10):
        described = ecs.describe_services(cluster=cluster_arn, services=arns[i:i + 10])
        for svc in described.get("services", []):
            if svc.get("status") == "ACTIVE" and _is_fargate(svc):
                total += 1
    return total


def _count_standalone_fargate_tasks(ecs, cluster_arn):
    """Count Fargate tasks in one cluster that no service owns.

    A task launched by RunTask or a scheduled rule has no supervisor above it, so
    at workload granularity the task itself is the workload. Service-owned tasks
    are replicas of a service already counted by _count_fargate_services and are
    skipped here. ECS names the task group "service:<name>" for the former and
    "family:<name>" for the latter.
    """
    arns = list(_iter_paginated(ecs, "list_tasks", "taskArns", cluster=cluster_arn))
    total = 0
    for i in range(0, len(arns), 100):
        described = ecs.describe_tasks(cluster=cluster_arn, tasks=arns[i:i + 100])
        for task in described.get("tasks", []):
            if _is_fargate(task) and not (task.get("group") or "").startswith("service:"):
                total += 1
    return total


_STUDIO_SPACE_CACHE = {}


def get_studio_spaces(session, region):
    """(running, idle) SageMaker Studio spaces in one region.

    A space runs at most one app at a time, on an ML instance in an AWS-managed account, and
    keeps its EBS volume when no app is running. So a space with a running app is a VM, and one
    without is storage: footnote 26, the same split as a stopped EC2 instance.

    Apps with no SpaceName are skipped. Those are Studio Classic apps, which cannot be started
    since 2025-01-31, and Canvas and RStudio apps, which hang off a user profile rather than a
    space. They are a known gap, noted in footnote 19.
    """
    key = (session, region)  # per account, as in get_root_devices()
    if key not in _STUDIO_SPACE_CACHE:
        sm = session.client("sagemaker", region_name=region)
        spaces = {(sp["DomainId"], sp["SpaceName"])
                  for sp in _iter_paginated(sm, "list_spaces", "Spaces")}
        running = set()
        if spaces:
            for app in _iter_paginated(sm, "list_apps", "Apps"):
                if app.get("SpaceName") and app.get("Status") in SAGEMAKER_APP_RUNNING_STATES:
                    running.add((app.get("DomainId"), app["SpaceName"]))
        running &= spaces
        _STUDIO_SPACE_CACHE[key] = (len(running), len(spaces) - len(running))
    return _STUDIO_SPACE_CACHE[key]


_INFLUXDB_CACHE = {}


def get_influxdb_inventory(session, region):
    """(clusters, standalone instances) for Timestream for InfluxDB in one region.

    A cluster counts once, like an Aurora cluster. Its member instances are subtracted from the
    instance list rather than trusted to be absent from it: the docs do not say whether
    ListDbInstances includes cluster members, and its summary carries no cluster id to filter
    on. So membership comes from ListDbInstancesForCluster, and the subtraction is correct
    either way.
    """
    key = (session, region)  # per account, as in get_root_devices()
    if key not in _INFLUXDB_CACHE:
        tsi = session.client("timestream-influxdb", region_name=region)
        clusters = [c for c in _iter_paginated(tsi, "list_db_clusters", "items")
                    if c.get("status") != "DELETED"]
        members = set()
        for c in clusters:
            members.update(i["id"] for i in _iter_paginated(
                tsi, "list_db_instances_for_cluster", "items", dbClusterId=c["id"]))
        standalone = sum(1 for i in _iter_paginated(tsi, "list_db_instances", "items")
                         if i.get("status") != "DELETED" and i["id"] not in members)
        _INFLUXDB_CACHE[key] = (len(clusters), standalone)
    return _INFLUXDB_CACHE[key]


def _copy_excluded(rt, is_copy):
    """Tally a managed database left out as a copy of one counted elsewhere (footnote 23).

    By type only. The measure says how much the rule removed, which is what makes a field
    that stops matching visible; no resource is named.
    """
    if is_copy:
        by_type = DIAG.measures.setdefault("managed_db_copies_excluded", {})
        by_type[rt] = by_type.get(rt, 0) + 1
    return is_copy


def _is_rds_read_replica(db):
    """True for an RDS instance that is a read replica of another instance or a cluster."""
    return _copy_excluded("rds:db", bool(db.get("ReadReplicaSourceDBInstanceIdentifier")
                                         or db.get("ReadReplicaSourceDBClusterIdentifier")))


def _is_counted_cluster(rt_label, cluster, secondaries):
    """False for a cross-region read replica cluster or a global database's secondary."""
    return not _copy_excluded(rt_label, bool(cluster.get("ReplicationSourceIdentifier"))
                              or cluster.get("DBClusterArn") in secondaries)


# Keyed by session, like _ROOT_DEVICE_CACHE, so an --all-accounts run never hands one
# account's member list to the next.
_GLOBAL_SECONDARY_CACHE = {}
_GLOBAL_SECONDARY_WARNED = set()


def global_secondary_arns(session, region, service):
    """ARNs of clusters that are a non-writer member of an Aurora or Neptune global database.

    A global database is one writer cluster plus up to ten (Aurora) or five (Neptune)
    secondary clusters in other regions, each a full cluster in its own region's
    DescribeDBClusters. The writer counts and the secondaries do not (footnote 23). The call
    is made in the secondary's own region, where the global database has a member.

    When it is refused, the set is empty and every secondary is counted, as before, and
    global_database_members_readable fails so the over-count is visible. The warning is printed
    once per account and service rather than once per region.
    """
    key = (session, region, service)
    if key not in _GLOBAL_SECONDARY_CACHE:
        arns = set()
        try:
            client = session.client(service, region_name=region)
            for gc in _iter_paginated(client, "describe_global_clusters", "GlobalClusters"):
                for member in gc.get("GlobalClusterMembers") or []:
                    if not member.get("IsWriter") and member.get("DBClusterArn"):
                        arns.add(member["DBClusterArn"])
        except (ClientError, BotoCoreError) as e:
            code = "BotoCoreError"
            if isinstance(e, ClientError):
                code = e.response.get("Error", {}).get("Code") or "Unknown"
            DIAG.record_denial(f"{service}:DescribeGlobalClusters", code)
            DIAG.bump("global_cluster_lookups_failed")
            if (session, service) not in _GLOBAL_SECONDARY_WARNED:
                _GLOBAL_SECONDARY_WARNED.add((session, service))
                print(f"  WARNING: {service}:DescribeGlobalClusters refused ({code}); global "
                      "database secondaries are counted as separate databases", file=sys.stderr)
        _GLOBAL_SECONDARY_CACHE[key] = arns
    return _GLOBAL_SECONDARY_CACHE[key]


def count_paginated(client, method, result_key, count_fn=None, **kwargs):
    """Paginate a boto3 call and return total count."""
    paginator = client.get_paginator(method)
    total = 0
    for page in paginator.paginate(**kwargs):
        items = page.get(result_key, [])
        total += count_fn(items) if count_fn else len(items)
    return total


def _iter_paginated(client, method, result_key, **kwargs):
    """Yield individual items from a paginated boto3 call."""
    paginator = client.get_paginator(method)
    for page in paginator.paginate(**kwargs):
        yield from page.get(result_key, [])


def count_next_token(client, method, result_key, count_fn=None, **kwargs):
    """Count items from a boto3 call that uses NextToken pagination but has no built-in paginator."""
    fn = getattr(client, method)
    total = 0
    resp = fn(**kwargs)
    while True:
        items = resp.get(result_key, [])
        total += count_fn(items) if count_fn else len(items)
        next_token = resp.get("NextToken")
        if not next_token:
            break
        resp = fn(NextToken=next_token, **kwargs)
    return total


def count_resource(resource_type, session, region, dry_run):
    """Return count for a single resource type in a single region."""
    if dry_run:
        print(f"  [dry-run] would query {resource_type} in {region}")
        return 0

    start = time.time()
    debug(f"    querying {resource_type} in {region}...")
    result = _count_resource_inner(resource_type, session, region)
    debug(f"    {resource_type} in {region}: {result} ({time.time() - start:.1f}s)")
    return result


def _count_resource_inner(resource_type, session, region):
    try:
        if resource_type == "ec2:instance":
            ec2 = session.client("ec2", region_name=region)
            pager = ec2.get_paginator("describe_instances")
            total = 0
            for page in pager.paginate(
                Filters=[{"Name": "instance-state-name", "Values": COUNTED_INSTANCE_STATES}]
            ):
                for res in page["Reservations"]:
                    total += len(res["Instances"])
            return total

        elif resource_type == "ec2:image":
            ec2 = session.client("ec2", region_name=region)
            return count_paginated(ec2, "describe_images", "Images", Owners=["self"])

        elif resource_type == "lightsail:instance":
            ls = session.client("lightsail", region_name=region)
            return count_paginated(ls, "get_instances", "instances")

        elif resource_type in ("sagemaker:notebook-instance", "sagemaker:stopped-notebook-instance"):
            sm = session.client("sagemaker", region_name=region)
            want_off = resource_type == "sagemaker:stopped-notebook-instance"
            return count_paginated(
                sm, "list_notebook_instances", "NotebookInstances",
                count_fn=lambda items: sum(
                    1 for n in items
                    if (n.get("NotebookInstanceStatus") in SAGEMAKER_NOTEBOOK_OFF_STATES) == want_off
                ),
            )

        elif resource_type in ("sagemaker:space", "sagemaker:idle-space"):
            running, idle = get_studio_spaces(session, region)
            return running if resource_type == "sagemaker:space" else idle

        elif resource_type == "lightsail:container-service":
            ls = session.client("lightsail", region_name=region)
            # Not paginated: GetContainerServices returns every service in the region.
            return len(ls.get_container_services().get("containerServices", []))

        elif resource_type == "eks:fargate-profile":
            eks = session.client("eks", region_name=region)
            total = 0
            for cluster in _iter_paginated(eks, "list_clusters", "clusters"):
                try:
                    total += count_paginated(eks, "list_fargate_profiles", "fargateProfileNames", clusterName=cluster)
                except ClientError as e:
                    print(f"  WARNING: eks:fargate-profile for cluster {cluster}: {e}", file=sys.stderr)
            return total

        elif resource_type in ("ecs:service", "ecs:standalone-task"):
            ecs = session.client("ecs", region_name=region)
            count_one = (_count_fargate_services if resource_type == "ecs:service"
                         else _count_standalone_fargate_tasks)
            total = 0
            for cluster_arn in _iter_paginated(ecs, "list_clusters", "clusterArns"):
                # Deliberately NOT caught here. A cluster that cannot be read makes this
                # region's total an undercount, and swallowing the error returned that
                # undercount as a confident number: the denial never reached the ledger, the
                # region still counted as a success, and a missing permission surfaced only as
                # a stderr line that scrolls past. Letting it propagate hands it to the handler
                # at the foot of this function, which records the denial against the resource
                # type and marks the region unmeasured. Found on the 2026-09-18 live run, where
                # ecs:ListServices was denied and Serverless Container reported 0.
                total += count_one(ecs, cluster_arn)
            return total

        elif resource_type == "apprunner:service":
            ar = session.client("apprunner", region_name=region)
            return count_next_token(ar, "list_services", "ServiceSummaryList")

        elif resource_type == "ecr:image":
            ecr = session.client("ecr", region_name=region)
            total = 0
            had_errors = False
            for repo in _iter_paginated(ecr, "describe_repositories", "repositories"):
                try:
                    total += count_paginated(
                        ecr, "describe_images", "imageDetails",
                        count_fn=lambda items: sum(
                            1 for img in items
                            if img.get("imageManifestMediaType", "") not in _MANIFEST_LIST_MEDIA_TYPES
                        ),
                        repositoryName=repo["repositoryName"],
                    )
                except ClientError as e:
                    print(f"  WARNING: ecr:image for repo {repo['repositoryName']}: {e}", file=sys.stderr)
                    had_errors = True
            return None if had_errors else total

        elif resource_type == "lambda:function":
            lm = session.client("lambda", region_name=region)
            return count_paginated(lm, "list_functions", "Functions")

        elif resource_type == "rds:db":
            # Cluster members are counted on the cluster line, and a read replica is a copy of
            # an instance counted in its own right (footnote 23).
            rds = session.client("rds", region_name=region)
            return count_paginated(
                rds, "describe_db_instances", "DBInstances",
                count_fn=lambda items: sum(
                    1 for db in items
                    if not db.get("DBClusterIdentifier") and not _is_rds_read_replica(db)
                ),
            )

        elif resource_type == "rds:cluster":
            rds = session.client("rds", region_name=region)
            secondaries = global_secondary_arns(session, region, "rds")
            return count_paginated(
                rds, "describe_db_clusters", "DBClusters",
                count_fn=lambda items: sum(
                    1 for c in items if c.get("Engine") not in _NON_RDS_ENGINES
                    and _is_counted_cluster(rt_label="rds:cluster", cluster=c,
                                            secondaries=secondaries)
                ),
            )

        elif resource_type == "dynamodb:table":
            ddb = session.client("dynamodb", region_name=region)
            return count_paginated(ddb, "list_tables", "TableNames")

        elif resource_type == "elasticache:cachecluster":
            # Every member node of a Valkey/Redis replication group is returned here as a
            # cluster of its own, and the group is counted on the replication-group line, so
            # only clusters outside any group belong on this one (footnote 23): Memcached and
            # single-node Valkey/Redis.
            ec = session.client("elasticache", region_name=region)
            return count_paginated(
                ec, "describe_cache_clusters", "CacheClusters",
                count_fn=lambda items: sum(1 for c in items if not c.get("ReplicationGroupId")),
            )

        elif resource_type == "elasticache:replication-group":
            # A Global Datastore is a primary replication group plus up to two secondaries in
            # other regions, each a replication group of its own; only the primary counts.
            ec = session.client("elasticache", region_name=region)
            return count_paginated(
                ec, "describe_replication_groups", "ReplicationGroups",
                count_fn=lambda items: sum(
                    1 for g in items
                    if not _copy_excluded("elasticache:replication-group", str(
                        (g.get("GlobalReplicationGroupInfo") or {})
                        .get("GlobalReplicationGroupMemberRole") or "").lower() == "secondary")
                ),
            )

        elif resource_type == "memorydb:cluster":
            mdb = session.client("memorydb", region_name=region)
            return count_paginated(mdb, "describe_clusters", "Clusters")

        elif resource_type == "docdb:cluster":
            # A global cluster's secondaries carry ReplicationSourceIdentifier.
            docdb = session.client("docdb", region_name=region)
            return count_paginated(
                docdb, "describe_db_clusters", "DBClusters",
                count_fn=lambda items: sum(
                    1 for c in items
                    if not _copy_excluded("docdb:cluster", bool(c.get("ReplicationSourceIdentifier")))
                ),
            )

        elif resource_type == "neptune:db-cluster":
            # Neptune does not set ReplicationSourceIdentifier, so a global database's
            # secondaries are known only from its member list.
            neptune = session.client("neptune", region_name=region)
            secondaries = global_secondary_arns(session, region, "neptune")
            return count_paginated(
                neptune, "describe_db_clusters", "DBClusters",
                count_fn=lambda items: sum(
                    1 for c in items
                    if _is_counted_cluster(rt_label="neptune:db-cluster", cluster=c,
                                           secondaries=secondaries)
                ),
            )

        elif resource_type == "opensearch:domain":
            aos = session.client("opensearch", region_name=region)
            resp = aos.list_domain_names()
            return len(resp.get("DomainNames", []))

        elif resource_type == "cassandra:keyspace":
            ks = session.client("keyspaces", region_name=region)
            return count_paginated(
                ks, "list_keyspaces", "keyspaces",
                count_fn=lambda items: sum(
                    1 for k in items if k.get("keyspaceName") not in _SYSTEM_KEYSPACES
                ),
            )

        elif resource_type == "timestream:database":
            tsw = session.client("timestream-write", region_name=region)
            try:
                return count_next_token(tsw, "list_databases", "Databases")
            except ClientError as e:
                # Timestream for LiveAnalytics is closed to new customers. Accounts that
                # never onboarded get AccessDeniedException from the endpoint-discovery
                # call the SDK makes before every request, in every region. That means
                # "this account has no Timestream", i.e. zero — not "could not measure",
                # and it is the normal case for most customers. A genuine IAM denial
                # names the principal and action instead, so it falls through to the
                # generic handler and still records null.
                err = e.response.get("Error", {})
                if (err.get("Code") == "AccessDeniedException"
                        and "existing Timestream" in err.get("Message", "")):
                    return 0
                raise

        elif resource_type in ("timestream-influxdb:db-cluster", "timestream-influxdb:db-instance"):
            clusters, standalone = get_influxdb_inventory(session, region)
            return clusters if resource_type == "timestream-influxdb:db-cluster" else standalone

        elif resource_type == "redshift:cluster":
            rs = session.client("redshift", region_name=region)
            return count_paginated(rs, "describe_clusters", "Clusters")

        elif resource_type == "emr:cluster":
            emr = session.client("emr", region_name=region)
            # Without a state filter this returns terminated clusters for roughly the
            # trailing two months, which massively overcounts transient job clusters.
            return count_paginated(emr, "list_clusters", "Clusters",
                                   ClusterStates=list(EMR_COUNTED_STATES))

        elif resource_type == "redshift-serverless:namespace":
            rss = session.client("redshift-serverless", region_name=region)
            return count_paginated(rss, "list_namespaces", "namespaces")

        elif resource_type == "emr-serverless:application":
            emrs = session.client("emr-serverless", region_name=region)
            return count_paginated(
                emrs, "list_applications", "applications",
                count_fn=lambda items: sum(
                    1 for a in items if a.get("state") != "TERMINATED"
                ),
            )

        elif resource_type == "s3:bucket":
            # S3 is global — caller skips non-us-east-1 regions
            s3 = session.client("s3")
            resp = s3.list_buckets()
            return len(resp.get("Buckets", []))

        elif resource_type == "glacier:vault":
            glac = session.client("glacier", region_name=region)
            return count_paginated(glac, "list_vaults", "VaultList", accountId="-")

        elif resource_type == "backup:backup-vault":
            bk = session.client("backup", region_name=region)
            return count_paginated(bk, "list_backup_vaults", "BackupVaultList")

        elif resource_type == "ec2:volume":
            ec2 = session.client("ec2", region_name=region)
            root_devices = get_root_devices(session, region)
            return count_paginated(
                ec2, "describe_volumes", "Volumes",
                count_fn=lambda items: sum(
                    1 for v in items if is_data_volume(v, root_devices)
                ),
            )

        else:
            print(f"  WARNING: unknown resource type {resource_type}", file=sys.stderr)
            return None

    except (ClientError, BotoCoreError) as e:
        code = "BotoCoreError"
        if isinstance(e, ClientError):
            code = e.response.get("Error", {}).get("Code") or "Unknown"
        DIAG.record_denial(resource_type, code)
        print(f"  WARNING: {resource_type} in {region}: {e}", file=sys.stderr)
        return None


def assume_role_session(base_session, account_id, role_name):
    """Return a new boto3.Session using credentials from assuming role_name in account_id."""
    sts = base_session.client("sts")
    role_arn = f"arn:aws:iam::{account_id}:role/{role_name}"
    creds = sts.assume_role(RoleArn=role_arn, RoleSessionName="count-aws-resources")["Credentials"]
    return configure_session(boto3.Session(
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
    ))


def discover_org_accounts(base_session):
    """Return active account IDs visible to the caller via AWS Organizations."""
    org = base_session.client("organizations")
    accounts = []
    for account in _iter_paginated(org, "list_accounts", "Accounts"):
        if account.get("Status") == "ACTIVE":
            accounts.append(account["Id"])
    return accounts


def build_target_sessions(args):
    """Return (mode, providers, excluded) for the requested account selection.

    providers is a list of (label, get_session) pairs, where get_session is a zero-argument
    callable returning a ready boto3.Session. Role assumption happens inside it, at scan time,
    rather than for every account up front: assume_role credentials last one hour by default
    and do not refresh, so on a large organization the accounts at the end of the list would
    otherwise be scanned with expired credentials.
    """
    excluded = sorted(set(args.exclude_account or []))

    if args.scan_profiles:
        if excluded:
            print(
                "WARNING: --exclude-account has no effect with --scan-profiles, which selects "
                "profiles rather than account IDs; ignoring it",
                file=sys.stderr,
            )
        providers = []
        for profile in args.scan_profiles:
            try:
                session = configure_session(boto3.Session(profile_name=profile))
            except ProfileNotFound as e:
                print(f"FATAL: {e}", file=sys.stderr)
                sys.exit(1)
            providers.append((profile, lambda s=session: s))
        return "profiles", providers, []

    try:
        base_session = configure_session(boto3.Session(profile_name=args.profile))
    except ProfileNotFound as e:
        print(f"FATAL: {e}", file=sys.stderr)
        sys.exit(1)

    if args.all_accounts:
        mode = "all-accounts"
        try:
            account_ids = discover_org_accounts(base_session)
        except (ClientError, BotoCoreError) as e:
            print(f"FATAL: could not discover accounts via AWS Organizations: {e}", file=sys.stderr)
            sys.exit(1)
        if not account_ids:
            print("FATAL: no active accounts discovered", file=sys.stderr)
            sys.exit(1)
    elif args.account_id:
        mode = "accounts"
        account_ids = args.account_id
    else:
        # Default: scan just the base/default session, no role assumption.
        if excluded:
            print("WARNING: --exclude-account has no effect on a single-account run; ignoring it", file=sys.stderr)
        return "default", [(args.profile or "default", lambda: base_session)], []

    # An excluded ID the caller expected to matter but that was never in scope is worth saying
    # out loud: it usually means a typo, and a silently ineffective exclusion is an overcount.
    not_in_scope = [a for a in excluded if a not in account_ids]
    if not_in_scope:
        print(
            f"WARNING: --exclude-account listed {len(not_in_scope)} account(s) that are not in scope "
            f"anyway: {', '.join(not_in_scope)}",
            file=sys.stderr,
        )

    kept = [a for a in account_ids if a not in set(excluded)]
    dropped = [a for a in account_ids if a in set(excluded)]
    if dropped:
        progress(f"excluding {len(dropped)} account(s) on request: {', '.join(dropped)}")
    if not kept:
        print("FATAL: every discovered account was excluded", file=sys.stderr)
        sys.exit(1)

    # The caller's own account is reached with the credentials already in hand. Organizations
    # creates the cross-account role only in *member* accounts, so assuming into the management
    # account fails with an AccessDenied that reads like a misconfiguration, and the account the
    # run is authenticated as drops out of its own org-wide count.
    caller_account = None
    try:
        caller_account = base_session.client("sts").get_caller_identity()["Account"]
    except (ClientError, BotoCoreError) as e:
        print(f"WARNING: could not resolve the calling account's ID ({e}); every account will be "
              f"reached by assuming {args.role_name}", file=sys.stderr)

    providers = []
    for account_id in kept:
        if account_id == caller_account:
            providers.append((account_id, lambda s=base_session: s))
        else:
            providers.append(
                (account_id, lambda a=account_id: assume_role_session(base_session, a, args.role_name))
            )
    return mode, providers, dropped


class ScanContext:
    """Everything a rung needs to measure one resource type in one account.

    Passed to every probe and every measure, so rungs stay stateless and adding one never
    means threading another parameter down through scan_account.
    """

    __slots__ = ("session", "account_id", "regions", "args", "ce_probe_cache",
                 "config_recorder_cache")

    def __init__(self, session, account_id, regions, args):
        self.session = session
        self.account_id = account_id
        self.regions = regions
        self.args = args
        # Whether Cost Explorer answers is an account-level fact, not a per-type one, so the
        # probe is paid once per service per account rather than once per resource type.
        self.ce_probe_cache = {}
        # Recorder configuration is per region and shared by every resource type, so it is
        # described once per region rather than once per type.
        self.config_recorder_cache = {}


def count_type_point_in_time(rt, ctx):
    """Enumerate one resource type across the account's in-scope regions, as of right now.

    Returns None rather than zero when the type failed in every region it was queried in, so
    that a permission denial never rounds down into a plausible-looking zero. This is the
    behaviour the script had before the ladder; it now lives behind the floor rung.
    """
    if rt == "s3:bucket":
        # Global: one endpoint answers for the whole account, so there is nothing to sum.
        return count_resource(rt, ctx.session, "us-east-1", ctx.args.dry_run)

    rt_regions = get_service_regions(ctx.session, rt, ctx.regions)
    if not rt_regions:
        print(f"  WARNING: {rt} has no known endpoint in any queried region; skipping", file=sys.stderr)
    if rt == "dynamodb:table" and not ctx.args.dry_run:
        return count_dynamodb_tables(ctx, rt_regions)
    if rt in _MULTI_REGION_NAME and not ctx.args.dry_run:
        return count_multi_region_once(ctx, rt, rt_regions)
    total = 0
    any_success = False
    for region in rt_regions:
        count = count_resource(rt, ctx.session, region, ctx.args.dry_run)
        if count is not None:
            total += count
            any_success = True
    return total if any_success else None


def count_dynamodb_tables(ctx, regions):
    """DynamoDB tables across the account's regions, with each global table counted once.

    Replicas of a global table share its name, so only a name listed in two or more regions
    can be one; those alone are described, which keeps DescribeTable calls to the tables that
    need them rather than one per table. A replica in another account (multi-account global
    tables) belongs to that account's count and does not move this one.

    When DescribeTable is refused the table is counted in every region it appears in, which
    over-counts global tables exactly as the script did before, and dynamodb_replicas_readable
    fails so the over-count is visible.

    --diagnostics describes every counted table instead, to report how many hold no items. That
    is a measure, never a count change, and it costs one call per table, which is why it rides
    on the heavier bundle rather than running always. A refusal stops further describing rather
    than repeating it per table.
    """
    describe_all = bool(getattr(ctx.args, "diagnostics", False))
    names = {}
    for region in regions:
        ddb = ctx.session.client("dynamodb", region_name=region)
        try:
            names[region] = list(_iter_paginated(ddb, "list_tables", "TableNames"))
        except (ClientError, BotoCoreError) as e:
            code = "BotoCoreError"
            if isinstance(e, ClientError):
                code = e.response.get("Error", {}).get("Code") or "Unknown"
            DIAG.record_denial("dynamodb:table", code)
            print(f"  WARNING: dynamodb:table in {region}: {e}", file=sys.stderr)
    if not names:
        return None

    # Only regions that answered take part in choosing where a global table counts; a replica
    # in a region that failed was not counted there, so it must not take the count away.
    answered = sorted(names)
    regions_per_name = {}
    for region, tables in names.items():
        for name in tables:
            regions_per_name[name] = regions_per_name.get(name, 0) + 1

    total, excluded, unreadable = 0, 0, 0
    items_read, items_unread, empty = 0, 0, 0
    refused = False
    for region, tables in names.items():
        ddb = ctx.session.client("dynamodb", region_name=region)
        for name in tables:
            maybe_replica = regions_per_name[name] >= 2
            if not maybe_replica and not describe_all:
                total += 1
                continue
            table = None
            if not refused:
                try:
                    table = ddb.describe_table(TableName=name).get("Table") or {}
                except (ClientError, BotoCoreError) as e:
                    code = "BotoCoreError"
                    if isinstance(e, ClientError):
                        code = e.response.get("Error", {}).get("Code") or "Unknown"
                    DIAG.record_denial("dynamodb:DescribeTable", code)
                    refused = "AccessDenied" in code or "Unauthorized" in code
            if table is None:
                if maybe_replica:
                    unreadable += 1
                items_unread += 1
                total += 1
                continue
            if maybe_replica:
                replica_regions = [
                    r.get("RegionName") for r in table.get("Replicas") or []
                    if _arn_account(r.get("ReplicaArn")) in (None, ctx.account_id)
                ]
                if _dynamodb_counting_region(region, replica_regions, answered) != region:
                    excluded += 1
                    continue
            total += 1
            if table.get("ItemCount") is None:
                items_unread += 1
            else:
                items_read += 1
                empty += table["ItemCount"] == 0

    DIAG.bump("dynamodb_replicas_excluded", excluded)
    DIAG.bump("dynamodb_replicas_unreadable", unreadable)
    if describe_all:
        DIAG.bump("dynamodb_item_counts_read_live", items_read)
        DIAG.bump("dynamodb_item_counts_unread_live", items_unread)
        DIAG.bump("dynamodb_tables_empty_live", empty)
    return total


# Types whose multi-region form is one resource per region with no primary: a MemoryDB
# multi-region cluster's regional clusters, and a multi-region keyspace, which exists under
# the same name in each of its regions. For each: (service, method, result key, the name of
# the multi-region resource an item belongs to or None, whether the item counts at all).
_MULTI_REGION_NAME = {
    "memorydb:cluster": (
        "memorydb", "describe_clusters", "Clusters",
        lambda c: c.get("MultiRegionClusterName") or None,
        lambda c: True,
    ),
    "cassandra:keyspace": (
        "keyspaces", "list_keyspaces", "keyspaces",
        lambda k: k.get("keyspaceName") if k.get("replicationStrategy") == "MULTI_REGION" else None,
        lambda k: k.get("keyspaceName") not in _SYSTEM_KEYSPACES,
    ),
}


def count_multi_region_once(ctx, rt, regions):
    """Count a type across regions with each multi-region resource counted once (footnote 23).

    All regional members of one are owned by this account and share its name, so a name seen
    in several regions is one resource. Everything else counts once per region, as before.
    """
    service, method, key, multi_name, counts = _MULTI_REGION_NAME[rt]
    total, names, any_success = 0, set(), False
    for region in regions:
        client = ctx.session.client(service, region_name=region)
        try:
            items = list(_iter_paginated(client, method, key))
        except (ClientError, BotoCoreError) as e:
            code = "BotoCoreError"
            if isinstance(e, ClientError):
                code = e.response.get("Error", {}).get("Code") or "Unknown"
            DIAG.record_denial(rt, code)
            print(f"  WARNING: {rt} in {region}: {e}", file=sys.stderr)
            continue
        any_success = True
        for item in items:
            if not counts(item):
                continue
            name = multi_name(item)
            if name is None:
                total += 1
            elif not _copy_excluded(rt, name in names):
                names.add(name)
                total += 1
    return total if any_success else None


def _arn_account(arn):
    """The account field of an ARN, or None when there is no ARN to read."""
    parts = (arn or "").split(":")
    return parts[4] if len(parts) > 5 and parts[4] else None


def scan_account(session, args):
    """Run the full category/resource-type scan for a single account session."""
    try:
        sts = session.client("sts")
        account_id = sts.get_caller_identity()["Account"]
    except (ClientError, BotoCoreError) as e:
        print(f"FATAL: could not resolve AWS account ID: {e}", file=sys.stderr)
        return None

    try:
        ec2_global = session.client("ec2", region_name="us-east-1")
        regions, skipped, unreachable = get_regions(
            session, ec2_global, args.region, set(args.skip_region or [])
        )
    except (ClientError, BotoCoreError) as e:
        print(f"FATAL: could not enumerate regions for account {account_id}: {e}", file=sys.stderr)
        return None

    if not regions:
        print(f"FATAL: no reachable regions for account {account_id}", file=sys.stderr)
        return None

    region_scope = {"scanned": regions, "skipped": skipped, "unreachable": unreachable}

    if args.dry_run:
        print(f"[dry-run] Account: {account_id}")
        print(f"[dry-run] Regions: {regions}")

    progress(f"[{account_id}] scanning {len(regions)} region(s) across {len(CATEGORIES)} categories...")
    account_start = time.time()

    ctx = ScanContext(session, account_id, regions, args)

    results = []
    for cat_idx, cat_def in enumerate(CATEGORIES, 1):
        category = cat_def["category"]
        resource_types = cat_def["resource_types"]
        cat_start = time.time()
        progress(f"[{account_id}] ({cat_idx}/{len(CATEGORIES)}) {category}...")

        measurements = {rt: measure_type(rt, ctx) for rt in resource_types}
        breakdown = {rt: m.count for rt, m in measurements.items()}
        provenance = {rt: m.provenance() for rt, m in measurements.items()}

        total = sum(v for v in breakdown.values() if v is not None)
        time_range, method = category_provenance(provenance, breakdown)
        results.append({
            "category": category,
            "count": total,
            "time_range": time_range,
            "method": method,
            "resource_types": resource_types,
            "breakdown": breakdown,
            "provenance": provenance,
        })
        progress(f"[{account_id}] ({cat_idx}/{len(CATEGORIES)}) {category}: {total} ({time.time() - cat_start:.1f}s)")

    if not args.dry_run:
        probe_nonos_disk_reasons(ctx)

    progress(f"[{account_id}] done ({time.time() - account_start:.1f}s)")
    return account_id, results, region_scope


def sum_categories(per_account):
    """Combine per-account category lists into one totals list of the same shape.

    A breakdown entry stays None only when the resource type failed in every account, so a
    genuine "we could not measure this" never gets rounded down to a plausible-looking zero.
    """
    totals = []
    for idx, cat_def in enumerate(CATEGORIES):
        breakdown = {}
        provenance = {}
        for rt in cat_def["resource_types"]:
            values = [acct[idx]["breakdown"].get(rt) for acct in per_account]
            measured = [v for v in values if v is not None]
            breakdown[rt] = sum(measured) if measured else None
            # Only accounts that actually produced a number get a say in the label: an account
            # where the type failed contributed nothing to the sum.
            provenance[rt] = merge_provenance([
                acct[idx].get("provenance", {}).get(rt)
                for acct, v in zip(per_account, values) if v is not None
            ])
        time_range, method = category_provenance(provenance, breakdown)
        totals.append({
            "category": cat_def["category"],
            "count": sum(v for v in breakdown.values() if v is not None),
            "time_range": time_range,
            "method": method,
            "resource_types": cat_def["resource_types"],
            "breakdown": breakdown,
            "provenance": provenance,
        })
    return totals


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


def evaluate_canaries(totals, accounts, failed_accounts):
    """Assert the things whose failure mode is a plausible-looking number rather than an error.

    Every check here guards a filter or a join that, when it breaks, quietly returns a count
    somebody would accept. That is why they run on every invocation instead of behind a flag.
    """
    by_cat = {r["category"]: r for r in totals}
    vm = by_cat.get("VM", {}).get("count", 0)
    disks = by_cat.get("Non-OS Disk", {}).get("count", 0)
    stopped = DIAG.measures.get("instances_stopped", 0)

    unmeasured = {rt for r in totals for rt, v in r["breakdown"].items() if v is None}
    DIAG.canary(
        "resource_types_all_measured", not unmeasured, len(unmeasured),
        "resource type(s) failed in every scanned account, so their categories are undercounts",
    )

    DIAG.canary(
        "accounts_all_scanned", not failed_accounts, len(failed_accounts),
        "account(s) could not be scanned at all, so the totals are an undercount",
    )

    # The loud-degradation rule made visible. An ambiguous probe means a better instrument may
    # have been available and was skipped, which shows up as a silently worse Method label
    # rather than as an error -- exactly the failure mode canaries exist to catch.
    # Cost Explorer speaks usage types, and AWS adds them without warning. A usage type billed
    # in hours under a service we count, which no pattern claims, is silently missing from the
    # average -- the exact failure mode where a wrong number still looks reasonable.
    unmatched_ce = DIAG.measures.get("ce_unmatched_hourly_usage_types", 0)
    DIAG.canary(
        "ce_usage_types_all_matched", unmatched_ce == 0, unmatched_ce,
        "hourly usage type(s) under a counted service matched no pattern, so the Cost "
        "Explorer average for that category is an undercount",
    )

    # Config dates every arrival it can, including for resources that have gone, by reading one
    # configuration item each. When a large share of those reads come back empty the smoothed
    # number is missing the churn it exists to capture, and that has to be visible rather than
    # inferred from a ratio nobody reads.
    dated = DIAG.measures.get("config_arrivals_dated_fraction")
    if dated is None:
        DIAG.canary(
            "config_arrivals_dated", None, None,
            "the Config rung did not run, so there is nothing to date",
        )
    else:
        DIAG.canary(
            "config_arrivals_dated", dated >= CONFIG_MIN_DATED_FRACTION, dated,
            f"under {int(CONFIG_MIN_DATED_FRACTION * 100)}% of the window's resources could be "
            "dated; deletions whose history could not be read are omitted, so the smoothed "
            "average understates a churning estate",
        )

    # The general form of that bug. A denial recorded against a resource type that still
    # reported a number means the number is an undercount wearing a complete answer's clothes,
    # whatever the mechanism. Keyed on resource types only: the ladder records its denials
    # against action names, and a skipped instrument is not an undercount.
    counted_types = {rt for r in totals for rt, v in r["breakdown"].items() if v is not None}
    undercounted = sorted(
        {key.split("/", 1)[0] for key in DIAG.denials} & counted_types
    )
    DIAG.canary(
        "counts_free_of_denials", not undercounted, len(undercounted),
        "resource type(s) reported a count despite a recorded denial, so those categories are "
        "undercounts that read as complete",
    )

    # A refused DescribeTable leaves a global table counted once per replica region, the
    # over-count footnote 23 exists to prevent. Skipped when the point-in-time path never ran
    # for DynamoDB, which is what happens when the Config rung answered instead.
    unreadable_ddb = DIAG.measures.get("dynamodb_replicas_unreadable")
    DIAG.canary(
        "dynamodb_replicas_readable",
        None if unreadable_ddb is None else unreadable_ddb == 0, unreadable_ddb,
        "DynamoDB table(s) listed in several regions could not be described, so any global "
        "table among them is counted once per replica region",
    )

    # Zero when the lookup never ran (no Aurora or Neptune line was counted live) or always
    # answered; either way nothing was over-counted on its account.
    failed_lookups = DIAG.measures.get("global_cluster_lookups_failed", 0)
    DIAG.canary(
        "global_database_members_readable", failed_lookups == 0, failed_lookups,
        "DescribeGlobalClusters was refused, so any Aurora or Neptune global database "
        "secondary is counted as a separate database",
    )

    unknown_probes = DIAG.measures.get("ladder_probes_unknown", 0)
    DIAG.canary(
        "ladder_probes_unambiguous", unknown_probes == 0, unknown_probes,
        "instrument probe(s) could not tell an unavailable instrument from an empty result, "
        "so a better rung may have been skipped and the Method column understates what was "
        "reachable",
    )

    unreachable = sum(len(a.get("regions", {}).get("unreachable") or []) for a in accounts)
    DIAG.canary(
        "regions_all_reachable", unreachable == 0, unreachable,
        "region(s) failed the reachability probe and were dropped, shrinking the count",
    )

    # A boot disk is excluded from Non-OS Disk only while its instance is counted, so after
    # footnote 26 the honest denominator is every instance that still exists, running or
    # stopped -- otherwise a mostly-stopped fleet trips this on correct numbers. Roughly one
    # disk per instance is what "boot disks are being counted" looks like; the measured
    # fleet-wide figure across 668 customers is about 0.4.
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
            "which volumes make up the count",
        )
        DIAG.measure("stopped_instance_fraction", round(stopped / instances, 3))

    # Always measured, not only when the ratio fails: the reasons are what a reader needs to
    # tell a disk-heavy estate from a leak, and a passing run is the baseline to compare with.
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


def print_table(account_id, results):
    col1 = max([len(r["category"]) for r in results] + [len("Category")])
    print(f"\n=== {account_id} ===")
    for line in _format_table(results, col1):
        print(line)


def main():
    parser = argparse.ArgumentParser(description="Count AWS resources per FortiCNAPP billing category.")
    parser.add_argument("--region", help="Scope to a single AWS region (skips the reachability probe)")
    parser.add_argument(
        "--skip-region", nargs="+", default=[],
        help="Region(s) to skip entirely. Unreachable regions are detected and skipped automatically.",
    )
    parser.add_argument("--output-dir", default=".", help="Directory to write JSON output (default: .)")
    parser.add_argument("--dry-run", action="store_true", help="Print API calls without executing them")
    parser.add_argument(
        "--encoded-output", action="store_true",
        help="Also print the JSON as a gzip+base64 block, for environments where the written "
             "file cannot be retrieved (a cloud shell, a locked-down jump host).",
    )

    account_group = parser.add_mutually_exclusive_group()
    account_group.add_argument(
        "--scan-profiles", nargs="+",
        help="One or more AWS CLI profiles to scan independently (e.g. SSO profiles from ~/.aws/config)",
    )
    account_group.add_argument(
        "--account-id", nargs="+",
        help="One or more target account IDs to scan by assuming --role-name from the base session",
    )
    account_group.add_argument(
        "--all-accounts", action="store_true",
        help="Discover all active accounts in the AWS Organization (via the base session) and scan each",
    )
    parser.add_argument(
        "--exclude-account", nargs="+", default=[], metavar="ID",
        help="Account ID(s) to exclude when scanning org-wide with --all-accounts",
    )
    parser.add_argument(
        "--role-name", default="OrganizationAccountAccessRole",
        help="IAM role to assume in each target account for --account-id/--all-accounts (default: OrganizationAccountAccessRole)",
    )
    parser.add_argument(
        "--profile",
        help="AWS CLI profile used to assume roles into target accounts, or to discover accounts with --all-accounts",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Print per-resource-type/per-region progress as it happens (in addition to per-category progress)",
    )
    parser.add_argument(
        "--diagnostics", action="store_true",
        help="Add the heavier diagnostics bundle (denial ledger, environment) to the JSON, and "
             "describe every DynamoDB table to measure how many are empty (one call per table; "
             "never changes a count). Canaries and measures are always included.",
    )
    args = parser.parse_args()

    global VERBOSE
    VERBOSE = args.verbose

    # stdout is block-buffered when piped while stderr is not, so a "| tee" transcript would
    # otherwise show the tables and the progress/warning lines badly out of order. That
    # transcript is the field's diagnostic artifact, so it has to read in true sequence.
    sys.stdout.reconfigure(line_buffering=True)
    print(f"FortiCNAPP resource counts (AWS) - script version {script_version()}")

    mode, target_sessions, excluded = build_target_sessions(args)
    if not target_sessions:
        print("FATAL: no accounts to scan", file=sys.stderr)
        sys.exit(1)

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc).isoformat()

    all_accounts_output = []
    run_start = time.time()
    failed_accounts = []
    for idx, (label, get_session) in enumerate(target_sessions, 1):
        progress(f"=== account {idx}/{len(target_sessions)}: {label} ===")
        try:
            session = get_session()
        except (ClientError, BotoCoreError) as e:
            print(f"WARNING: could not assume role {args.role_name} in account {label}: {e}", file=sys.stderr)
            failed_accounts.append(label)
            continue
        scanned = scan_account(session, args)
        if scanned is None:
            failed_accounts.append(label)
            print(f"  WARNING: skipping account {label} due to earlier error", file=sys.stderr)
            continue
        account_id, results, region_scope = scanned
        all_accounts_output.append({
            "account_id": account_id,
            "regions": region_scope,
            "categories": results,
        })

    if not all_accounts_output:
        print("FATAL: no accounts were successfully scanned", file=sys.stderr)
        sys.exit(1)

    scanned_ids = [a["account_id"] for a in all_accounts_output]
    totals = sum_categories([a["categories"] for a in all_accounts_output])

    print_table(f"TOTALS ({len(scanned_ids)} account(s))" if len(scanned_ids) != 1 else f"Account {scanned_ids[0]}", totals)
    if len(all_accounts_output) > 1:
        for acct in all_accounts_output:
            print_table(f"Account {acct['account_id']}", acct["categories"])

    DIAG.measure("run_seconds", round(time.time() - run_start, 1))
    evaluate_canaries(totals, all_accounts_output, failed_accounts)
    DIAG.report()

    output = {
        "csp": "aws",
        "scope": {
            "mode": mode,
            "role_name": args.role_name if mode in ("accounts", "all-accounts") else None,
            "scanned": scanned_ids,
            "excluded": excluded,
            "inaccessible": failed_accounts,
        },
        "script_version": script_version(),
        "generated_at": generated_at,
        "totals": totals,
        "accounts": all_accounts_output,
        "diagnostics": DIAG.to_json(verbose=args.diagnostics),
    }
    out_path = out_dir / "aws_resource_counts.json"
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nJSON written to {out_path}")

    if args.encoded_output:
        print_encoded_output(output)

    progress(f"=== all accounts done ({time.time() - run_start:.1f}s) ===")

    # A partial run must not look like a complete one: an account that dropped out silently
    # is an undercount, and the count is what the customer is sized on.
    if failed_accounts:
        print(
            f"\nWARNING: {len(failed_accounts)} of {len(target_sessions)} account(s) could not be "
            f"scanned and are NOT included in these counts: {', '.join(failed_accounts)}",
            file=sys.stderr,
        )
        sys.exit(2)


if __name__ == "__main__":
    main()
