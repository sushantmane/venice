#!/usr/bin/env python3
"""
Flaky Test Report Generator for Venice CI.

Analyzes test results across CI runs on main to detect flaky tests, newly broken
tests, and consistently failing tests. Attributes failures to likely causing
commits and PR authors.

Data flow:
  1. Fetch recent completed E2ETests runs on main (gh CLI)
  2. For each run, query job-level pass/fail via API (no artifact download needed)
  3. Only download artifacts for FAILED jobs to get test-method-level detail
  4. Update rolling 90-day history on the ci-data branch
  5. Classify tests: flaky, newly broken, consistently failing, resolved
  6. Attribute recent failures to commits/PRs via git log
  7. Create/update a single GitHub issue with the report

Usage:
  # Full run (in CI cron job)
  python scripts/ci/flaky_test_report.py --repo linkedin/venice

  # Dry run (no issue creation, no history push)
  python scripts/ci/flaky_test_report.py --repo linkedin/venice --dry-run

  # Analyze more history
  python scripts/ci/flaky_test_report.py --repo linkedin/venice --runs 50
"""

import argparse
import base64
import json
import os
import shutil
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime, timedelta, timezone

HISTORY_FILE = "flaky-history.json"
HISTORY_BRANCH = "ci-data"
WINDOW_DAYS = 90
FLAKY_THRESHOLD = 0.05  # 5% failure rate across the window
ISSUE_LABEL = "flaky-test"
RECENT_WINDOW = 10  # last N runs for "recent" classification


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def run_cmd(cmd, capture=True, cwd=None):
    """Run a command and return stdout, or None on failure."""
    result = subprocess.run(cmd, capture_output=capture, text=True, cwd=cwd)
    if result.returncode != 0:
        if capture and result.stderr:
            print(f"  cmd failed: {' '.join(cmd[:6])}...: {result.stderr.strip()[:200]}",
                  file=sys.stderr)
        return None
    return result.stdout.strip() if capture else ""


def gh(*args):
    """Run a gh CLI command."""
    return run_cmd(["gh"] + list(args))


def git(*args, cwd=None):
    """Run a git command."""
    return run_cmd(["git"] + list(args), cwd=cwd)


# ---------------------------------------------------------------------------
# History persistence (ci-data branch)
# ---------------------------------------------------------------------------

def load_history(repo):
    """Load flaky test history from the ci-data branch via GitHub API."""
    content = gh(
        "api", f"repos/{repo}/contents/{HISTORY_FILE}",
        "--jq", ".content",
        "-H", "Accept: application/vnd.github.v3+json",
        "-f", f"ref={HISTORY_BRANCH}",
    )
    if content:
        try:
            decoded = base64.b64decode(content).decode("utf-8")
            return json.loads(decoded)
        except Exception as e:
            print(f"  Warning: failed to decode history: {e}", file=sys.stderr)

    return {"_version": 1, "_window_days": WINDOW_DAYS, "runs": []}


def save_history(history, repo, work_dir):
    """Push updated history JSON to the ci-data branch."""
    history["_updated_at"] = datetime.now(timezone.utc).isoformat()
    history_json = json.dumps(history, indent=2)

    clone_dir = os.path.join(work_dir, "ci-data-repo")

    token = os.environ.get("GH_TOKEN", os.environ.get("GITHUB_TOKEN", ""))
    if token:
        repo_url = f"https://x-access-token:{token}@github.com/{repo}.git"
    else:
        repo_url = f"https://github.com/{repo}.git"

    cloned = run_cmd(
        ["git", "clone", "--depth=1", "--branch", HISTORY_BRANCH, repo_url, clone_dir]
    )
    if cloned is None:
        run_cmd(["git", "clone", "--depth=1", repo_url, clone_dir])
        git("checkout", "--orphan", HISTORY_BRANCH, cwd=clone_dir)
        git("rm", "-rf", ".", cwd=clone_dir)

    dest = os.path.join(clone_dir, HISTORY_FILE)
    with open(dest, "w") as f:
        f.write(history_json)

    git("add", HISTORY_FILE, cwd=clone_dir)

    status = git("status", "--porcelain", cwd=clone_dir)
    if not status:
        print("  History unchanged, nothing to push.")
        return

    git("commit", "-m",
        f"Update flaky test history ({datetime.now(timezone.utc).strftime('%Y-%m-%d')})",
        cwd=clone_dir)
    git("push", "origin", HISTORY_BRANCH, cwd=clone_dir)
    print("  History pushed to ci-data branch.")


def squash_history_branch(repo, work_dir):
    """Force-push a squashed single-commit ci-data branch (monthly cleanup)."""
    token = os.environ.get("GH_TOKEN", os.environ.get("GITHUB_TOKEN", ""))
    if token:
        repo_url = f"https://x-access-token:{token}@github.com/{repo}.git"
    else:
        repo_url = f"https://github.com/{repo}.git"

    clone_dir = os.path.join(work_dir, "ci-data-squash")
    cloned = run_cmd(
        ["git", "clone", "--depth=1", "--branch", HISTORY_BRANCH, repo_url, clone_dir]
    )
    if cloned is None:
        print("  ci-data branch does not exist, nothing to squash.")
        return

    git("checkout", "--orphan", "ci-data-squashed", cwd=clone_dir)
    git("add", "-A", cwd=clone_dir)
    git("commit", "-m",
        f"Squash flaky test history ({datetime.now(timezone.utc).strftime('%Y-%m-%d')})",
        cwd=clone_dir)
    git("branch", "-D", HISTORY_BRANCH, cwd=clone_dir)
    git("branch", "-m", HISTORY_BRANCH, cwd=clone_dir)
    git("push", "origin", HISTORY_BRANCH, "--force", cwd=clone_dir)
    print("  ci-data branch squashed to single commit.")


# ---------------------------------------------------------------------------
# CI run data
# ---------------------------------------------------------------------------

def get_recent_runs(repo, workflow, branch="main", limit=10):
    """Fetch recent completed CI runs from GitHub."""
    output = gh(
        "run", "list",
        "--repo", repo,
        "--workflow", workflow,
        "--branch", branch,
        "--status", "completed",
        "-L", str(limit),
        "--json", "databaseId,headSha,createdAt,conclusion",
    )
    if not output:
        return []
    return json.loads(output)


def get_failed_jobs(repo, run_id):
    """Get list of failed integration test job names and IDs for a run."""
    output = gh(
        "api", f"repos/{repo}/actions/runs/{run_id}/jobs",
        "--paginate",
        "--jq",
        '.jobs[] | select(.conclusion == "failure") '
        '| select(.name | test("^[Ii]ntegrationTests_")) '
        '| {name, id}',
    )
    if not output:
        return []

    jobs = []
    for line in output.strip().split("\n"):
        if line.strip():
            try:
                jobs.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return jobs


def download_job_artifact(repo, run_id, job_name, work_dir):
    """Download and extract artifact for a single failed job."""
    job_dir = os.path.join(work_dir, f"run-{run_id}", job_name)
    os.makedirs(job_dir, exist_ok=True)

    gh("run", "download", str(run_id),
       "--repo", repo,
       "--name", job_name,
       "--dir", job_dir)

    # Extract tar.gz files
    for root, _, files in os.walk(job_dir):
        for f in files:
            if f.endswith(".tar.gz"):
                tar_path = os.path.join(root, f)
                run_cmd(["tar", "xzf", tar_path, "-C", root])

    return job_dir


def parse_test_results(artifact_dir):
    """Parse TEST-*.xml files and return {test_fqn: status} map."""
    results = {}

    for root, _, files in os.walk(artifact_dir):
        for f in files:
            if not (f.startswith("TEST-") and f.endswith(".xml")):
                continue
            xml_path = os.path.join(root, f)
            try:
                tree = ET.parse(xml_path)
                for tc in tree.getroot().iter("testcase"):
                    classname = tc.get("classname", "")
                    name = tc.get("name", "")
                    if not classname or not name:
                        continue

                    fqn = f"{classname}#{name}"

                    if tc.find("failure") is not None:
                        results[fqn] = "fail"
                    elif tc.find("error") is not None:
                        results[fqn] = "error"
                    elif tc.find("skipped") is not None:
                        results[fqn] = "skip"
                    else:
                        results[fqn] = "pass"
            except ET.ParseError:
                print(f"  Warning: failed to parse {xml_path}", file=sys.stderr)

    return results


def process_run(repo, run, work_dir):
    """
    Process a single CI run.

    For successful runs: record zero failures (no artifact download needed).
    For failed/cancelled runs: download only failed job artifacts to get details.

    Returns a dict suitable for appending to history["runs"], or None to skip.
    """
    run_id = run["databaseId"]
    conclusion = run.get("conclusion", "")
    sha = run["headSha"][:8]
    timestamp = run["createdAt"]

    if conclusion == "cancelled":
        print(f"    Skipping cancelled run.")
        return None

    if conclusion == "success":
        # All tests passed — no need to download anything
        print(f"    All jobs passed.")
        return {
            "run_id": run_id,
            "sha": sha,
            "timestamp": timestamp,
            "conclusion": conclusion,
            "failures": [],
        }

    # Run had failures — find which integration test jobs failed
    failed_jobs = get_failed_jobs(repo, run_id)
    if not failed_jobs:
        # Failure was in a non-test job (e.g., build failure, CompletionAlert)
        print(f"    No integration test jobs failed (build-level failure?).")
        return {
            "run_id": run_id,
            "sha": sha,
            "timestamp": timestamp,
            "conclusion": conclusion,
            "failures": [],
        }

    print(f"    {len(failed_jobs)} failed job(s): {', '.join(j['name'] for j in failed_jobs)}")

    all_failures = []
    for job in failed_jobs:
        job_name = job["name"]
        print(f"    Downloading artifact for {job_name}...")
        job_dir = download_job_artifact(repo, run_id, job_name, work_dir)
        results = parse_test_results(job_dir)

        job_failures = [t for t, s in results.items() if s in ("fail", "error")]
        if job_failures:
            print(f"      {len(job_failures)} test failures found.")
            all_failures.extend(job_failures)
        elif results:
            print(f"      {len(results)} tests found, all passing (timeout/infra failure?).")
        else:
            print(f"      No test XML results found (build/compile failure?).")

        # Clean up to save disk
        shutil.rmtree(job_dir, ignore_errors=True)

    return {
        "run_id": run_id,
        "sha": sha,
        "timestamp": timestamp,
        "conclusion": conclusion,
        "failures": all_failures,
    }


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

def trim_history(history, window_days):
    """Remove runs older than window_days."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=window_days)).isoformat()
    history["runs"] = [r for r in history["runs"] if r["timestamp"] >= cutoff]


def classify_tests(history):
    """
    Classify every test that has ever failed within the history window.

    Categories:
      flaky              - sometimes passes, sometimes fails
      newly_broken       - was passing, now failing consistently in recent runs
      consistently_failing - failing in nearly all runs
      resolved           - was failing, now passing in recent runs
    """
    empty = {"flaky": {}, "newly_broken": {}, "consistently_failing": {}, "resolved": {}}
    if not history["runs"]:
        return empty

    runs = sorted(history["runs"], key=lambda r: r["timestamp"])
    total_runs = len(runs)
    recent = runs[-min(RECENT_WINDOW, total_runs):]

    # Collect per-test failure counts
    test_stats = defaultdict(lambda: {
        "fail_count": 0,
        "recent_fails": 0,
        "first_failure": None,
        "last_failure": None,
    })

    recent_ids = {r["run_id"] for r in recent}

    for run in runs:
        for test in run.get("failures", []):
            s = test_stats[test]
            s["fail_count"] += 1
            if s["first_failure"] is None:
                s["first_failure"] = run["timestamp"]
            s["last_failure"] = run["timestamp"]
            if run["run_id"] in recent_ids:
                s["recent_fails"] += 1

    recent_count = len(recent)

    flaky = {}
    newly_broken = {}
    consistently_failing = {}
    resolved = {}

    for test, s in test_stats.items():
        fail_rate = s["fail_count"] / total_runs
        recent_fail_rate = s["recent_fails"] / recent_count

        if recent_fail_rate == 0:
            resolved[test] = {
                "total_fail_rate": round(fail_rate, 3),
                "last_failure": s["last_failure"],
            }
        elif recent_fail_rate >= 0.9 and fail_rate < 0.7:
            newly_broken[test] = {
                "fail_rate": round(fail_rate, 3),
                "recent_fail_rate": round(recent_fail_rate, 3),
                "first_failure": s["first_failure"],
                "last_failure": s["last_failure"],
                "fail_count": s["fail_count"],
                "total_runs": total_runs,
            }
        elif recent_fail_rate >= 0.9:
            consistently_failing[test] = {
                "fail_rate": round(fail_rate, 3),
                "first_failure": s["first_failure"],
                "fail_count": s["fail_count"],
                "total_runs": total_runs,
            }
        elif fail_rate >= FLAKY_THRESHOLD:
            flaky[test] = {
                "fail_rate": round(fail_rate, 3),
                "recent_fail_rate": round(recent_fail_rate, 3),
                "fail_count": s["fail_count"],
                "total_runs": total_runs,
                "last_failure": s["last_failure"],
            }

    return {
        "flaky": flaky,
        "newly_broken": newly_broken,
        "consistently_failing": consistently_failing,
        "resolved": resolved,
    }


# ---------------------------------------------------------------------------
# Attribution
# ---------------------------------------------------------------------------

def attribute_failure(repo, test_fqn, since_date):
    """
    Find commits/PRs that likely caused a test failure.

    Looks at git log for the test's source file and related production files.
    Returns a list of {sha, author, subject, pr} dicts.
    """
    class_part = test_fqn.split("#")[0]
    simple_name = class_part.split(".")[-1]

    find_result = run_cmd(
        ["find", ".", "-name", f"{simple_name}.java", "-path", "*/src/*"],
    )
    if not find_result:
        return []

    files = find_result.strip().split("\n")
    if not files:
        return []

    commits = []
    seen_shas = set()

    for fpath in files[:3]:
        log_output = git(
            "log", f"--since={since_date}", "--format=%H|%ae|%s", "--", fpath,
        )
        if not log_output:
            continue
        for line in log_output.split("\n"):
            parts = line.split("|", 2)
            if len(parts) != 3:
                continue
            sha, author, subject = parts
            if sha in seen_shas:
                continue
            seen_shas.add(sha)

            pr_num = gh(
                "api", f"repos/{repo}/commits/{sha}/pulls",
                "--jq", ".[0].number",
            )
            commits.append({
                "sha": sha[:8],
                "author": author.split("@")[0],
                "subject": subject[:80],
                "pr": pr_num if pr_num and pr_num != "null" else None,
            })

    return commits[:5]


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def generate_report(classification, repo, history):
    """Generate the full markdown report."""
    lines = []
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    total_runs = len(history["runs"])

    lines.append(f"# Flaky Test Report \u2014 {today}\n")
    lines.append(f"Analyzed **{total_runs}** E2ETest runs on `main` "
                 f"(last {WINDOW_DAYS} days).\n")

    n_flaky = len(classification["flaky"])
    n_broken = len(classification["newly_broken"])
    n_consistent = len(classification["consistently_failing"])
    n_resolved = len(classification["resolved"])

    lines.append("## Summary\n")
    lines.append("| Category | Count |")
    lines.append("|----------|-------|")
    lines.append(f"| Newly broken | {n_broken} |")
    lines.append(f"| Flaky | {n_flaky} |")
    lines.append(f"| Consistently failing | {n_consistent} |")
    lines.append(f"| Recently resolved | {n_resolved} |")
    lines.append("")

    if classification["newly_broken"]:
        lines.append("## Newly Broken Tests\n")
        lines.append("These tests were passing but have started failing consistently.\n")
        lines.append("| Test | Fail Rate | Since | Likely Cause |")
        lines.append("|------|-----------|-------|--------------|")
        for test in sorted(classification["newly_broken"]):
            s = classification["newly_broken"][test]
            pct = f"{s['fail_rate']*100:.0f}% ({s['fail_count']}/{s['total_runs']})"
            since = s["first_failure"][:10]
            commits = attribute_failure(repo, test, since)
            if commits:
                c = commits[0]
                cause = f"#{c['pr']} by {c['author']}" if c["pr"] else f"`{c['sha']}` by {c['author']}"
            else:
                cause = "\u2014"
            lines.append(f"| `{test}` | {pct} | {since} | {cause} |")
        lines.append("")

    if classification["flaky"]:
        lines.append("## Flaky Tests\n")
        lines.append("These tests pass sometimes and fail sometimes.\n")
        lines.append("| Test | Flake Rate | Recent | Last Failure |")
        lines.append("|------|-----------|--------|--------------|")
        for test, s in sorted(classification["flaky"].items(),
                              key=lambda x: x[1]["fail_rate"], reverse=True):
            pct = f"{s['fail_rate']*100:.0f}% ({s['fail_count']}/{s['total_runs']})"
            recent = f"{s['recent_fail_rate']*100:.0f}%"
            last = s["last_failure"][:10] if s["last_failure"] else "\u2014"
            lines.append(f"| `{test}` | {pct} | {recent} | {last} |")
        lines.append("")

    if classification["consistently_failing"]:
        lines.append("## Consistently Failing Tests\n")
        lines.append("These tests have been failing in nearly all runs.\n")
        lines.append("| Test | Failing Since | Runs Failed |")
        lines.append("|------|---------------|-------------|")
        for test in sorted(classification["consistently_failing"]):
            s = classification["consistently_failing"][test]
            since = s["first_failure"][:10]
            count = f"{s['fail_count']}/{s['total_runs']}"
            lines.append(f"| `{test}` | {since} | {count} |")
        lines.append("")

    if classification["resolved"]:
        lines.append("## Recently Resolved\n")
        lines.append("These tests were flaky or broken but are now passing.\n")
        lines.append("| Test | Last Failure |")
        lines.append("|------|-------------|")
        for test in sorted(classification["resolved"]):
            s = classification["resolved"][test]
            last = s["last_failure"][:10] if s["last_failure"] else "\u2014"
            lines.append(f"| `{test}` | {last} |")
        lines.append("")

    if not any(classification[k] for k in ["flaky", "newly_broken", "consistently_failing"]):
        lines.append("## All Clear\n")
        lines.append("No flaky or broken tests detected in the analysis window.\n")

    return "\n".join(lines)


def create_or_update_issue(repo, report, dry_run=False):
    """Create or update the flaky test tracking GitHub issue."""
    existing = gh(
        "issue", "list",
        "--repo", repo,
        "--label", ISSUE_LABEL,
        "--state", "open",
        "--json", "number",
        "--jq", ".[0].number",
    )

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    title = f"Flaky Test Report \u2014 {today}"

    if dry_run:
        action = "update" if existing and existing != "null" else "create"
        print(f"\n[DRY RUN] Would {action} issue with title: {title}")
        print(report)
        return

    if existing and existing != "null":
        gh("issue", "edit", existing,
           "--repo", repo,
           "--title", title,
           "--body", report)
        print(f"  Updated issue #{existing}")
    else:
        result = gh("issue", "create",
                     "--repo", repo,
                     "--title", title,
                     "--body", report,
                     "--label", ISSUE_LABEL)
        print(f"  Created issue: {result}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Venice Flaky Test Report Generator")
    parser.add_argument("--repo", default="linkedin/venice",
                        help="GitHub repository (owner/name)")
    parser.add_argument("--workflow", default="E2ETests",
                        help="Workflow name to analyze")
    parser.add_argument("--branch", default="main",
                        help="Branch to analyze")
    parser.add_argument("--runs", type=int, default=20,
                        help="Number of recent runs to fetch")
    parser.add_argument("--window-days", type=int, default=WINDOW_DAYS,
                        help="Rolling history window in days")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print report without creating issues or updating history")
    parser.add_argument("--squash", action="store_true",
                        help="Squash the ci-data branch to a single commit and exit")
    parser.add_argument("--output", help="Write report to this file")

    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as work_dir:
        # --- Squash mode ---
        if args.squash:
            print("Squashing ci-data branch...")
            squash_history_branch(args.repo, work_dir)
            return

        # --- Fetch runs ---
        print(f"Fetching recent {args.workflow} runs on {args.branch}...")
        runs = get_recent_runs(args.repo, args.workflow, args.branch, args.runs)
        if not runs:
            print("No completed runs found.")
            return
        print(f"  Found {len(runs)} completed runs.")

        # --- Load history ---
        print("Loading history from ci-data branch...")
        history = load_history(args.repo)
        history["_window_days"] = args.window_days
        existing_ids = {r["run_id"] for r in history["runs"]}

        new_runs = [r for r in runs if r["databaseId"] not in existing_ids]
        print(f"  {len(new_runs)} new runs to process "
              f"({len(runs) - len(new_runs)} already in history).")

        # --- Process new runs ---
        for run in new_runs:
            run_id = run["databaseId"]
            conclusion = run.get("conclusion", "")
            print(f"\n  Run {run_id} ({run['createdAt'][:10]}, {conclusion})...")

            entry = process_run(args.repo, run, work_dir)
            if entry:
                history["runs"].append(entry)

        # --- Trim old entries ---
        trim_history(history, args.window_days)

        # --- Classify ---
        print("\nClassifying tests...")
        classification = classify_tests(history)

        n_total = sum(len(v) for v in classification.values())
        print(f"  {n_total} tests with failures in history.")

        # --- Report ---
        report = generate_report(classification, args.repo, history)

        if args.output:
            with open(args.output, "w") as f:
                f.write(report)
            print(f"Report written to {args.output}")

        # Write to $GITHUB_STEP_SUMMARY if in CI
        summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
        if summary_path:
            with open(summary_path, "a") as f:
                f.write(report)

        # --- Issue ---
        create_or_update_issue(args.repo, report, dry_run=args.dry_run)

        # --- Save history ---
        if not args.dry_run:
            print("\nSaving history...")
            save_history(history, args.repo, work_dir)

        print("\nDone.")


if __name__ == "__main__":
    main()
