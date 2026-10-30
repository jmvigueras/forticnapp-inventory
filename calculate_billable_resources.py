#!/usr/bin/env python3
"""Convert FortiCNAPP resource counts into billable resources.

Deliberately a separate tool from the counting scripts. Those measure what exists in a
customer's cloud and are auditable against their console; this one applies pricing weights that
are not ours to assert. Keeping them apart means a counting script can never quote a number,
and a stale ratio can never reach a customer through a run that looked like measurement.

It takes whatever the field actually comes back with -- the JSON files the counting scripts
write, or the terminal transcript they were captured in, which carries the same JSON as a
gzip+base64 block when the scripts are run with --encoded-output (the field process mandates
that flag, so a transcript is always enough).

Deployed Agent and Developer counts come from the customer, not from any cloud. They are asked
for here rather than in the counting scripts, because the person running a script in a
customer's cloud shell is usually not the person who knows those numbers, and Developer is not
a cloud metric at all. On-prem figures come from the customer for the same reason, and because
the counting scripts cannot run on-prem: each on-prem host or Kubernetes node adds 1 to VM and 1
to Deployed Agent, and images in on-prem registries add to Container Image (mapping footnotes
13 and 5). An on-prem-only sizing needs no cloud input at all.
"""

import argparse
import base64
import gzip
import json
import math
import re
import subprocess
import sys
from pathlib import Path


SCRIPT_VERSION = "2026.10.26"


def script_version():
    """SCRIPT_VERSION, plus the live git SHA when running inside its own clone.

    Same rule as the counting scripts: the SHA is trusted only when git reports this file as
    tracked, and local edits are reported as "-dirty", so a number can always be traced to the
    code and the ratios that produced it.
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


# N counted resources = 1 billable resource. Canonical source is the "Billable Ratio" column in
# resource-type-mapping.md, with footnote 24 explaining the direction; this table is a copy and
# has to be updated with it. The ratios are set by the ratio owner, not by this project -- if a
# number here disagrees with the mapping doc, the mapping doc is right.
RATIOS = {
    "VM": 1,
    "VM Image": 1,
    "Serverless Container": 10,
    "Container Image": 300,
    "Serverless Function": 50,
    "Managed DB (PaaS)": 2,
    "Data Warehouse": 10,
    "Bucket Storage": 100,
    "Non-OS Disk": 3,
    "Deployed Agent": 2,
    "Developer": 1,
}

# Figures no cloud can report. Supplied by the customer, and never defaulted -- see
# resolve_customer_supplied(). Keyed by argparse dest; the text is the prompt.
CUSTOMER_QUESTIONS = {
    "deployed_agents": "Deployed Agents in the cloud (hosts/containers running the FortiCNAPP "
                       "agent; leave out on-prem hosts, asked for next)",
    "developers": "Developers (people using the Code Security product)",
    "on_prem_hosts": "On-prem hosts running the agent (VMs, bare metal and Kubernetes nodes)",
    "on_prem_images": "Images in on-prem container registries (proxy scanner)",
}

# Categories that are wholly customer-supplied, and the answer each one takes.
CUSTOMER_SUPPLIED = {"Deployed Agent": "deployed_agents", "Developer": "developers"}

# On-prem answers and the categories they add to. A host is seen only through its agent, so it
# counts on both lines, the same as an agented cloud VM (ruled 2026-10-20, footnote 13).
ON_PREM = {
    "VM": "on_prem_hosts",
    "Deployed Agent": "on_prem_hosts",
    "Container Image": "on_prem_images",
}

# Delimiters written by the counting scripts' --encoded-output block.
_ENCODED_BEGIN = re.compile(r"^-+\s*BEGIN FORTICNAPP RESOURCE COUNTS.*$")
_ENCODED_END = re.compile(r"^-+\s*END FORTICNAPP RESOURCE COUNTS.*$")


def decode_transcript(text):
    """Pull the gzip+base64 payload out of a captured terminal transcript.

    Returns None when the text holds no encoded block, which is how load_input() decides the
    file was plain JSON instead. A block that is present but will not decode raises, because a
    truncated paste is exactly what gzip's checksum exists to catch -- failing here is the
    whole reason the counting scripts compress before encoding.
    """
    lines, collecting, blob = text.splitlines(), False, []
    for line in lines:
        if _ENCODED_BEGIN.match(line.strip()):
            collecting, blob = True, []
            continue
        if _ENCODED_END.match(line.strip()):
            collecting = False
            break
        if collecting:
            blob.append(line.strip())
    if not blob:
        return None
    raw = gzip.decompress(base64.b64decode("".join(blob)))
    return json.loads(raw)


def load_input(path):
    """Read one input, whether it is a JSON file or a transcript containing an encoded block."""
    text = sys.stdin.read() if path == "-" else Path(path).read_text()
    label = "stdin" if path == "-" else path

    stripped = text.lstrip()
    if stripped.startswith("{"):
        try:
            return json.loads(text), label
        except ValueError as e:
            raise SystemExit(f"FATAL: {label} starts like JSON but will not parse: {e}")

    try:
        decoded = decode_transcript(text)
    except Exception as e:
        raise SystemExit(
            f"FATAL: {label} contains an encoded block that will not decode ({e}).\n"
            "       A truncated or reflowed paste fails here by design rather than decoding to "
            "a plausible partial answer. Ask for the block again, unwrapped."
        )
    if decoded is None:
        raise SystemExit(
            f"FATAL: {label} is neither a counts JSON file nor a transcript containing an "
            "encoded block.\n"
            "       Transcripts only carry one if the counting script was run with "
            "--encoded-output."
        )
    return decoded, label


def _account_size(account):
    return sum(row.get("count") or 0 for row in account.get("categories") or [])


def dedupe_accounts(documents):
    """Collapse runs of the same cloud to one copy of each account, so overlaps count once.

    The field re-runs a cloud when a run missed something -- a forgotten subscription, a
    login that could not see everything -- and the two runs share accounts. Summing their
    totals counts every shared account twice. Every counting script lists its accounts, so a
    cloud with several inputs is rebuilt from its accounts instead, each account taken once.

    When an account appears twice the larger copy wins, by total count. Two copies differ
    because one run could see less of it or ran on another day; the larger is the one less
    likely to be missing something, and the warning names both so a person can check.

    Rebuilding from accounts is used only where a cloud has several inputs. Per-account counts
    are rounded one account at a time, so their sum can differ from the run's own total by a
    unit or two on smoothed categories, and a single input keeps the total the run printed.

    Returns, per document, the totals rows to use, plus a list of the overlaps found.
    """
    by_csp = {}
    for i, (doc, _label) in enumerate(documents):
        by_csp.setdefault(doc.get("csp", "unknown"), []).append(i)

    rows = [doc.get("totals") or [] for doc, _label in documents]
    overlaps = []
    for csp, indexes in by_csp.items():
        if len(indexes) < 2:
            continue
        if any(not documents[i][0].get("accounts") for i in indexes):
            continue  # cannot rebuild from accounts; the caller warns that totals are summed
        kept = {}
        for i in indexes:
            for account in documents[i][0]["accounts"]:
                aid = account.get("account_id")
                if aid in kept:
                    prev_i, prev = kept[aid]
                    winner = (i, account) if _account_size(account) > _account_size(prev) else (prev_i, prev)
                    overlaps.append({
                        "csp": csp, "account_id": aid,
                        "sizes": [(documents[prev_i][1], _account_size(prev)),
                                  (documents[i][1], _account_size(account))],
                        "kept": documents[winner[0]][1],
                    })
                    kept[aid] = winner
                else:
                    kept[aid] = (i, account)
        for i in indexes:
            rows[i] = [row for idx, account in kept.values() if idx == i
                       for row in account.get("categories") or []]
    return rows, overlaps


def merge(documents):
    """Sum per-category counts across clouds, keeping provenance and versions for reporting."""
    counts, provenance, sources, versions = {}, {}, [], set()
    totals_rows, overlaps = dedupe_accounts(documents)
    for (doc, label), doc_rows in zip(documents, totals_rows):
        csp = doc.get("csp", "unknown")
        versions.add(doc.get("script_version", "unknown"))
        diagnostics = doc.get("diagnostics") or {}
        failed = [c["name"] for c in diagnostics.get("canaries") or []
                  if c.get("status") == "fail"]
        sources.append({
            "csp": csp,
            "source": label,
            "script_version": doc.get("script_version"),
            "generated_at": doc.get("generated_at"),
            "scanned": len(doc.get("scope", {}).get("scanned") or []) or None,
            "canaries_failed": failed,
        })
        for row in doc_rows:
            category = row.get("category")
            if category is None:
                continue
            counts[category] = counts.get(category, 0) + (row.get("count") or 0)
            provenance.setdefault(category, set()).add(
                (csp, row.get("time_range"), row.get("method"))
            )
    return counts, provenance, sources, versions, overlaps


def resolve_customer_supplied(args):
    """Get the customer-supplied figures, and never invent them.

    Zero is a perfectly valid answer for any of them, but it has to be an answer. Defaulting an
    unasked question to zero is how a whole billable category silently disappears from a quote,
    so a value is either supplied on the command line or typed at a prompt -- and when neither
    is possible the run stops rather than guessing. The on-prem questions follow the same rule:
    most customers answer 0, but a customer with an on-prem estate who is never asked would be
    quoted for their cloud alone.
    """
    supplied = {dest: getattr(args, dest) for dest in CUSTOMER_QUESTIONS}
    for dest, value in supplied.items():
        if value is not None and value < 0:
            raise SystemExit(f"FATAL: --{dest.replace('_', '-')} cannot be negative")
    missing = [dest for dest, value in supplied.items() if value is None]
    if not missing:
        return supplied

    if not sys.stdin.isatty():
        flags = " ".join(f"--{dest.replace('_', '-')} N" for dest in missing)
        raise SystemExit(
            "FATAL: no value given for " + ", ".join(missing) + ", and stdin is not a "
            "terminal so there is nobody to ask.\n"
            f"       These come from the customer, not from their cloud. Pass {flags}.\n"
            "       Zero is a valid answer for any of them, but it has to be stated."
        )

    print("\nThese figures cannot be read from any cloud and come from the customer.")
    print("Zero is a valid answer for any of them -- but it has to be an answer, not a blank.\n")
    for name in missing:
        prompt = CUSTOMER_QUESTIONS[name]
        while True:
            reply = input(f"  {prompt}: ").strip()
            if reply.isdigit():
                supplied[name] = int(reply)
                break
            print("    Enter a whole number, 0 or greater.", file=sys.stderr)
    return supplied


def billable(count, ratio, rounding):
    """Convert a category count into billable resources.

    Rounding is a pricing decision rather than a measurement one, so it is a visible switch
    rather than a buried assumption. The default was confirmed as policy on 2026-09-21; the
    switch stays because the decision is reversible, not because it is still open:

      total (default, ruled) -- keep the exact fraction per category and round only the final
      sum. A customer with one of everything pays for roughly one billable resource, not one
      per category.

      category -- round each category up on its own. 1 container image then bills the same as
      300 of them, which is defensible under a "N resources = 1 billable" reading but charges
      a trivial estate nine times over.

    The two differ most on small estates, which is where sizing conversations usually start.
    """
    if count == 0:
        return 0.0
    if rounding == "category":
        return float(math.ceil(count / ratio))
    return count / ratio


def render(counts, provenance, supplied, rounding):
    """Build the per-category rows plus the total."""
    rows = []
    for category, ratio in RATIOS.items():
        on_prem = supplied[ON_PREM[category]] if category in ON_PREM else 0
        if category in CUSTOMER_SUPPLIED:
            count, source = supplied[CUSTOMER_SUPPLIED[category]], "customer"
            methods = ["customer-supplied"]
        else:
            if category not in counts and not on_prem:
                continue
            count = counts.get(category, 0)
            # The window is what a reader needs to interpret the number; the instrument behind
            # it is detail that belongs in the JSON. On a multi-cloud sizing every category
            # legitimately has several methods, so printing "mixed" on every row says nothing,
            # whereas "now + 30d" says the total blends a snapshot with an average.
            windows = sorted({p[1] for p in provenance.get(category, set()) if p[1]},
                             key=lambda w: (w == "now", w))
            methods = sorted({p[2] for p in provenance.get(category, set()) if p[2]})
            if on_prem:
                windows.append("customer")
                methods.append("customer-supplied")
            source = " + ".join(windows) if windows else "unknown"
        count += on_prem
        rows.append({
            "category": category,
            "count": count,
            "on_prem": on_prem,
            "ratio": ratio,
            "billable": billable(count, ratio, rounding),
            "window": source,
            "methods": methods,
        })
    exact = sum(r["billable"] for r in rows)
    return rows, exact


TOTAL_LABEL = "TOTAL BILLABLE"


def print_table(rows, exact, rounding):
    c1 = max([len(r["category"]) for r in rows] + [len("Category"), len(TOTAL_LABEL)])
    c5 = max([len(r["window"]) for r in rows] + [len("Window")])
    rule = f"{'-' * c1}-|{'-' * 9}|{'-' * 9}|{'-' * 11}|{'-' * (c5 + 1)}"
    print(f"\n{'Category':<{c1}} | {'Count':>7} | {'Ratio':>7} | {'Billable':>9} | {'Window':<{c5}}")
    print(rule)
    for r in rows:
        print(f"{r['category']:<{c1}} | {r['count']:>7} | {str(r['ratio']) + ':1':>7} | "
              f"{r['billable']:>9.2f} | {r['window']:<{c5}}".rstrip())
    print(rule)
    total = math.ceil(exact) if rounding == "total" else int(exact)
    print(f"{TOTAL_LABEL:<{c1}} | {'':>7} | {'':>7} | {total:>9} |")
    if rounding == "total" and abs(total - exact) > 1e-9:
        print(f"\n{exact:.2f} before rounding. Policy is to round once on the total; "
              f"--rounding category rounds each category up instead.")


def main():
    parser = argparse.ArgumentParser(
        description="Convert FortiCNAPP resource counts into billable resources.",
        epilog="Inputs may be *_resource_counts.json files, terminal transcripts containing an "
               "--encoded-output block, or '-' for stdin. Pass one per cloud, or none for an "
               "on-prem-only sizing; several runs of one cloud are combined by account.",
    )
    parser.add_argument("inputs", nargs="*", help="JSON file(s), transcript(s), or '-'")
    parser.add_argument("--deployed-agents", type=int, metavar="N",
                        help="Deployed Agent count in the cloud from the customer, on-prem hosts "
                             "excluded (0 is valid)")
    parser.add_argument("--developers", type=int, metavar="N",
                        help="Developer count from the customer (0 is valid)")
    parser.add_argument("--on-prem-hosts", type=int, metavar="N",
                        help="On-prem hosts and Kubernetes nodes running the agent; each adds 1 "
                             "to VM and 1 to Deployed Agent (0 is valid)")
    parser.add_argument("--on-prem-images", type=int, metavar="N",
                        help="Images in on-prem container registries; added to Container Image "
                             "(0 is valid)")
    parser.add_argument("--rounding", choices=("total", "category"), default="total",
                        help="Round once on the total (default policy) or up within each category")
    parser.add_argument("--output-json", metavar="PATH",
                        help="Also write the result as JSON")
    args = parser.parse_args()

    sys.stdout.reconfigure(line_buffering=True)
    print(f"FortiCNAPP billable resources - script version {script_version()}")

    documents = [load_input(path) for path in args.inputs]
    counts, provenance, sources, versions, overlaps = merge(documents)
    if documents and not counts:
        raise SystemExit("FATAL: no category totals found in any input")

    clouds = [s["csp"] for s in sources]
    duplicates = {c for c in clouds if clouds.count(c) > 1}
    unmergeable = {c for c in duplicates
                   if any(not doc.get("accounts") for doc, _l in documents if doc.get("csp") == c)}
    if unmergeable:
        print(f"WARNING: more than one input for {', '.join(sorted(unmergeable))}, and at least "
              "one lists no accounts. Counts are summed, so an account in two runs counts "
              "twice.", file=sys.stderr)
    if duplicates - unmergeable:
        print(f"NOTE: more than one input for {', '.join(sorted(duplicates - unmergeable))}. "
              "Combined by account, so an account in several runs counts once.", file=sys.stderr)
    for o in overlaps:
        sizes = ", ".join(f"{n} in {src}" for src, n in o["sizes"])
        print(f"WARNING: {o['csp']} account {o['account_id']} is in more than one input "
              f"({sizes}). Kept the copy from {o['kept']}.", file=sys.stderr)
    if len(versions) > 1:
        print(f"WARNING: inputs came from different script versions ({', '.join(sorted(versions))}). "
              "Counting rules change between versions, so these totals may not be comparable.",
              file=sys.stderr)

    suspect = [(s["csp"], s["canaries_failed"]) for s in sources if s.get("canaries_failed")]
    if suspect:
        print("", file=sys.stderr)
        for csp, names in suspect:
            print(f"WARNING: the {csp} run reported failing canaries ({', '.join(names)}). "
                  "Those counts are suspect, so any total derived from them is too.",
                  file=sys.stderr)

    supplied = resolve_customer_supplied(args)
    rows, exact = render(counts, provenance, supplied, args.rounding)

    listed = ", ".join("{} ({})".format(s["csp"], s["source"]) for s in sources)
    print(f"\nSources: {listed or 'none (on-prem only)'}")
    print_table(rows, exact, args.rounding)

    if args.output_json:
        payload = {
            "script_version": script_version(),
            "rounding": args.rounding,
            "sources": sources,
            "account_overlaps": overlaps,
            "customer_supplied": supplied,
            "categories": rows,
            "total_billable_exact": round(exact, 4),
            "total_billable": math.ceil(exact) if args.rounding == "total" else int(exact),
        }
        Path(args.output_json).write_text(json.dumps(payload, indent=2))
        print(f"\nJSON written to {args.output_json}")


if __name__ == "__main__":
    main()
