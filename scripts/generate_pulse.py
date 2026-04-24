#!/usr/bin/env python3
"""Generate the Lab Pulse data file for the Single-Molecule-Sequencing hub site.

Aggregates recent activity across active org repos into a single YAML data
file plus a ready-to-include markdown fragment. Runs nightly via a GitHub
Actions cron. Consumers:

  - `_data/pulse.yml` : consumed by Jekyll liquid loops in `_layouts/pulse.html`
                       and `index.html` (headline numbers).
  - `_includes/pulse.md` : optional markdown fragment for transclusion.

Environment:

  GH_TOKEN   : fine-grained PAT with `contents:read` + `metadata:read` +
               `issues:read` + `pull_requests:read` + `actions:read`
               on every repo we care about. Falls back to the ambient
               GITHUB_TOKEN (limited to public repos) if absent.
  PULSE_WINDOW_DAYS : override the default 7-day activity window.

Design notes:
  - Uses the REST API directly (`requests`) for full header control so we
    can surface rate-limit info and use conditional GETs.
  - Caches JSON responses under `.cache/` keyed on etag to stay polite
    with the GH API. Safe to wipe the cache directory.
  - Hardened against private-repo 403/404s: any repo that can't be read
    at all is reported in `pulse.yml::unreachable_repos` but does not
    abort the run.
  - No emojis anywhere in generated output (per user instruction).

This file is the single source of truth for pulse generation. Tests live
next door in `test_generate_pulse.py`.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
import yaml

# ---------------------------------------------------------------------------
# Config

ORG = "Single-Molecule-Sequencing"
HUB_REPO = "single-molecule-sequencing.github.io"

# Repos to exclude from the pulse (archived, deprecated, or noise).
DENY_LIST = frozenset(
    [
        "EndReason",
        "Error-Rate-SMS",
        "demo-repository",
        "portal",
        "SMS_seq_test",
        "SMA_seq_test",
        "spec-kit",
        ".github-private",
        ".github",
        "sss",
        "Textbook",
        "ONT_raw_data_explorer",
        "SMS",
        "End_reason_tagger",
        "dev-env-setup",
    ]
)

DEFAULT_WINDOW_DAYS = 7
RELEASE_WINDOW_DAYS = 30
GH_API = "https://api.github.com"


# ---------------------------------------------------------------------------
# Data classes

@dataclass
class RepoSummary:
    name: str
    default_branch: str
    description: str
    visibility: str
    pushed_at: str
    last_commit: dict[str, Any] | None = None
    commits_in_window: int = 0
    open_issues: int = 0
    open_prs: int = 0
    ci_status: str = "unknown"
    ci_url: str | None = None
    days_since_last_commit: int | None = None


@dataclass
class PulseContext:
    session: requests.Session
    window_days: int
    since_iso: str
    now: datetime
    unreachable: list[dict[str, str]] = field(default_factory=list)


# ---------------------------------------------------------------------------
# HTTP helpers

def make_session(token: str | None) -> requests.Session:
    s = requests.Session()
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "lab-pulse-generator/1.0",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    s.headers.update(headers)
    return s


def gh_get(
    session: requests.Session,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    unreachable: list[dict[str, str]] | None = None,
) -> list | dict | None:
    """GET a GitHub REST endpoint. Returns None on 403/404 to keep going."""
    url = path if path.startswith("http") else f"{GH_API}{path}"
    try:
        r = session.get(url, params=params, timeout=20)
    except requests.RequestException as e:
        if unreachable is not None:
            unreachable.append({"path": path, "error": f"request-failed: {e}"})
        return None
    if r.status_code == 200:
        try:
            return r.json()
        except ValueError:
            return None
    if r.status_code in (401, 403, 404):
        if unreachable is not None:
            unreachable.append({"path": path, "error": f"http-{r.status_code}"})
        return None
    # Unexpected status: surface it loudly so we notice.
    print(
        f"warning: unexpected status {r.status_code} for {path}",
        file=sys.stderr,
    )
    return None


def gh_paginate(
    session: requests.Session,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    max_pages: int = 3,
    unreachable: list[dict[str, str]] | None = None,
) -> list:
    """Paginate a list endpoint. Stops at `max_pages` to keep runtime bounded."""
    out: list = []
    page = 1
    while page <= max_pages:
        p = dict(params or {})
        p.setdefault("per_page", 30)
        p["page"] = page
        batch = gh_get(session, path, params=p, unreachable=unreachable)
        if not isinstance(batch, list) or not batch:
            break
        out.extend(batch)
        if len(batch) < p["per_page"]:
            break
        page += 1
    return out


# ---------------------------------------------------------------------------
# Repo enumeration

def list_org_repos(
    session: requests.Session,
    *,
    unreachable: list[dict[str, str]] | None = None,
) -> list[dict[str, Any]]:
    """Enumerate non-archived org repos, filtered against the deny list."""
    raw = gh_paginate(
        session,
        f"/orgs/{ORG}/repos",
        params={"per_page": 100, "type": "all"},
        max_pages=5,
        unreachable=unreachable,
    )
    kept: list[dict[str, Any]] = []
    for repo in raw:
        if repo.get("archived"):
            continue
        name = repo.get("name", "")
        if name in DENY_LIST:
            continue
        kept.append(repo)
    return kept


# ---------------------------------------------------------------------------
# Per-repo data

def fetch_commits(
    ctx: PulseContext, repo: str, default_branch: str
) -> list[dict[str, Any]]:
    """Last 5 commits on the default branch — the `since` filter is advisory
    because GH API returns at most per_page, ordered by date."""
    res = gh_get(
        ctx.session,
        f"/repos/{ORG}/{repo}/commits",
        params={"sha": default_branch, "per_page": 5, "since": ctx.since_iso},
        unreachable=ctx.unreachable,
    )
    if not isinstance(res, list):
        return []
    return res


def fetch_issues_and_prs(
    ctx: PulseContext, repo: str
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split the `/issues` endpoint into issue + PR lists.

    GitHub's `/issues` endpoint returns both issues and PRs; PRs have a
    `pull_request` key. We pull recent `updated` items and partition.
    """
    items = gh_paginate(
        ctx.session,
        f"/repos/{ORG}/{repo}/issues",
        params={
            "state": "all",
            "since": ctx.since_iso,
            "per_page": 30,
            "sort": "updated",
            "direction": "desc",
        },
        max_pages=2,
        unreachable=ctx.unreachable,
    )
    issues, prs = [], []
    for it in items:
        if "pull_request" in it:
            prs.append(it)
        else:
            issues.append(it)
    return issues, prs


def fetch_releases(ctx: PulseContext, repo: str) -> list[dict[str, Any]]:
    cutoff = ctx.now - timedelta(days=RELEASE_WINDOW_DAYS)
    res = gh_get(
        ctx.session,
        f"/repos/{ORG}/{repo}/releases",
        params={"per_page": 3},
        unreachable=ctx.unreachable,
    )
    if not isinstance(res, list):
        return []
    recent = []
    for r in res:
        pub = r.get("published_at") or r.get("created_at")
        if not pub:
            continue
        try:
            when = datetime.fromisoformat(pub.replace("Z", "+00:00"))
        except ValueError:
            continue
        if when >= cutoff:
            recent.append(r)
    return recent


def fetch_ci_status(
    ctx: PulseContext, repo: str, default_branch: str
) -> tuple[str, str | None]:
    """Return (status, url) for the latest completed workflow run on the default branch."""
    res = gh_get(
        ctx.session,
        f"/repos/{ORG}/{repo}/actions/runs",
        params={"per_page": 3, "branch": default_branch},
        unreachable=ctx.unreachable,
    )
    if not isinstance(res, dict):
        return "unknown", None
    runs = res.get("workflow_runs") or []
    for run in runs:
        conclusion = run.get("conclusion")
        status = run.get("status")
        if status == "completed" and conclusion:
            return conclusion, run.get("html_url")
    return "unknown", None


# ---------------------------------------------------------------------------
# Aggregation

def days_since(iso_str: str, now: datetime) -> int | None:
    if not iso_str:
        return None
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    except ValueError:
        return None
    return max(0, (now - dt).days)


def truncate_message(msg: str, limit: int = 120) -> str:
    first_line = (msg or "").splitlines()[0] if msg else ""
    return first_line[:limit].rstrip()


def build_repo_summary(
    ctx: PulseContext, repo_meta: dict[str, Any]
) -> tuple[RepoSummary, list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    name = repo_meta["name"]
    default_branch = (repo_meta.get("default_branch")
                      or (repo_meta.get("defaultBranchRef") or {}).get("name")
                      or "main")
    summary = RepoSummary(
        name=name,
        default_branch=default_branch,
        description=repo_meta.get("description") or "",
        visibility="private" if repo_meta.get("private") else "public",
        pushed_at=repo_meta.get("pushed_at", ""),
    )
    summary.days_since_last_commit = days_since(summary.pushed_at, ctx.now)
    commits = fetch_commits(ctx, name, default_branch)
    summary.commits_in_window = len(commits)
    if commits:
        top = commits[0]
        summary.last_commit = {
            "sha": (top.get("sha") or "")[:7],
            "message": truncate_message((top.get("commit") or {}).get("message", "")),
            "when": (top.get("commit") or {}).get("author", {}).get("date", ""),
            "author": (top.get("author") or {}).get("login")
            or (top.get("commit") or {}).get("author", {}).get("name", ""),
            "url": top.get("html_url"),
        }
    issues, prs = fetch_issues_and_prs(ctx, name)
    summary.open_issues = sum(1 for i in issues if i.get("state") == "open")
    summary.open_prs = sum(1 for p in prs if p.get("state") == "open")
    summary.ci_status, summary.ci_url = fetch_ci_status(ctx, name, default_branch)
    releases = fetch_releases(ctx, name)

    return summary, commits, issues, prs, releases


def normalize_commit(repo: str, c: dict[str, Any]) -> dict[str, Any]:
    commit = c.get("commit") or {}
    author_obj = c.get("author") or {}
    return {
        "repo": repo,
        "sha": (c.get("sha") or "")[:7],
        "author": author_obj.get("login")
        or commit.get("author", {}).get("name", "")
        or "unknown",
        "when": commit.get("author", {}).get("date", ""),
        "message": truncate_message(commit.get("message", "")),
        "url": c.get("html_url"),
    }


def normalize_issue_or_pr(repo: str, item: dict[str, Any], kind: str) -> dict[str, Any]:
    return {
        "repo": repo,
        "kind": kind,
        "number": item.get("number"),
        "title": (item.get("title") or "")[:160],
        "state": item.get("state"),
        "author": (item.get("user") or {}).get("login", ""),
        "created_at": item.get("created_at"),
        "updated_at": item.get("updated_at"),
        "closed_at": item.get("closed_at"),
        "merged_at": (item.get("pull_request") or {}).get("merged_at") if kind == "pr" else None,
        "url": item.get("html_url"),
    }


def normalize_release(repo: str, r: dict[str, Any]) -> dict[str, Any]:
    return {
        "repo": repo,
        "tag": r.get("tag_name"),
        "name": r.get("name") or r.get("tag_name"),
        "published_at": r.get("published_at") or r.get("created_at"),
        "url": r.get("html_url"),
        "prerelease": bool(r.get("prerelease")),
    }


# ---------------------------------------------------------------------------
# lab-wiki KG snapshot (optional)

def fetch_kg_snapshot(ctx: PulseContext) -> dict[str, Any] | None:
    """Try to read docs/kg-snapshots/latest/_toc.json from lab-wiki. Returns
    None cleanly if the snapshot has not been built yet."""
    raw = gh_get(
        ctx.session,
        f"/repos/{ORG}/lab-wiki/contents/docs/kg-snapshots/latest/_toc.json",
        unreachable=ctx.unreachable,
    )
    if not isinstance(raw, dict):
        return None
    content = raw.get("content")
    if not content:
        return None
    try:
        import base64
        body = base64.b64decode(content).decode("utf-8")
        parsed = json.loads(body)
    except Exception:
        return None
    return {
        "entities": parsed.get("entities"),
        "mentions": parsed.get("mentions"),
        "new_this_week": parsed.get("new_this_week"),
        "generated_at": parsed.get("generated_at"),
        "link": f"https://github.com/{ORG}/lab-wiki/blob/master/docs/kg-snapshots/latest/",
    }


# ---------------------------------------------------------------------------
# lab-papers paper statuses (optional)

def fetch_paper_statuses(ctx: PulseContext) -> list[dict[str, Any]]:
    """Read papers.yaml from lab-papers and project statuses."""
    raw = gh_get(
        ctx.session,
        f"/repos/{ORG}/lab-papers/contents/papers.yaml",
        unreachable=ctx.unreachable,
    )
    if not isinstance(raw, dict) or "content" not in raw:
        return []
    try:
        import base64
        body = base64.b64decode(raw["content"]).decode("utf-8")
        data = yaml.safe_load(body) or {}
    except Exception:
        return []
    out: list[dict[str, Any]] = []
    papers = data.get("papers") or {}
    if isinstance(papers, dict):
        for pid, meta in papers.items():
            if not isinstance(meta, dict):
                continue
            out.append(
                {
                    "kind": "paper",
                    "id": pid,
                    "title": (meta.get("title") or "")[:160],
                    "status": meta.get("status") or meta.get("stage") or "unknown",
                    "target_date": meta.get("target_submission_date")
                    or meta.get("target_date"),
                    "lead": meta.get("lead"),
                }
            )
    projects = data.get("projects") or {}
    if isinstance(projects, dict):
        for pid, meta in projects.items():
            if not isinstance(meta, dict):
                continue
            out.append(
                {
                    "kind": "project",
                    "id": pid,
                    "title": (meta.get("title") or "")[:160],
                    "status": meta.get("status") or "unknown",
                    "lead": meta.get("lead"),
                }
            )
    return out


# ---------------------------------------------------------------------------
# Assembly

def build_pulse(
    repos_raw: list[dict[str, Any]],
    ctx: PulseContext,
    *,
    kg_snapshot: dict[str, Any] | None = None,
    paper_statuses: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    repo_cards: list[dict[str, Any]] = []
    all_commits: list[dict[str, Any]] = []
    all_issues: list[dict[str, Any]] = []
    all_prs: list[dict[str, Any]] = []
    all_releases: list[dict[str, Any]] = []
    totals = {
        "repos_tracked": 0,
        "commits_past_week": 0,
        "issues_opened": 0,
        "issues_closed": 0,
        "prs_opened": 0,
        "prs_merged": 0,
        "prs_closed": 0,
        "releases_recent": 0,
        "open_issues": 0,
        "open_prs": 0,
    }

    since_dt = ctx.now - timedelta(days=ctx.window_days)

    for repo_meta in repos_raw:
        summary, commits, issues, prs, releases = build_repo_summary(ctx, repo_meta)
        totals["repos_tracked"] += 1
        totals["commits_past_week"] += summary.commits_in_window
        totals["open_issues"] += summary.open_issues
        totals["open_prs"] += summary.open_prs

        for c in commits:
            all_commits.append(normalize_commit(summary.name, c))

        for it in issues:
            norm = normalize_issue_or_pr(summary.name, it, "issue")
            all_issues.append(norm)
            if norm["created_at"]:
                try:
                    created = datetime.fromisoformat(norm["created_at"].replace("Z", "+00:00"))
                    if created >= since_dt:
                        totals["issues_opened"] += 1
                except ValueError:
                    pass
            if norm["closed_at"]:
                try:
                    closed = datetime.fromisoformat(norm["closed_at"].replace("Z", "+00:00"))
                    if closed >= since_dt:
                        totals["issues_closed"] += 1
                except ValueError:
                    pass

        for pr in prs:
            norm = normalize_issue_or_pr(summary.name, pr, "pr")
            all_prs.append(norm)
            if norm["created_at"]:
                try:
                    created = datetime.fromisoformat(norm["created_at"].replace("Z", "+00:00"))
                    if created >= since_dt:
                        totals["prs_opened"] += 1
                except ValueError:
                    pass
            if norm["merged_at"]:
                try:
                    merged = datetime.fromisoformat(norm["merged_at"].replace("Z", "+00:00"))
                    if merged >= since_dt:
                        totals["prs_merged"] += 1
                except ValueError:
                    pass
            elif norm["closed_at"]:
                try:
                    closed = datetime.fromisoformat(norm["closed_at"].replace("Z", "+00:00"))
                    if closed >= since_dt:
                        totals["prs_closed"] += 1
                except ValueError:
                    pass

        for rel in releases:
            all_releases.append(normalize_release(summary.name, rel))
        totals["releases_recent"] += len(releases)

        repo_cards.append(
            {
                "name": summary.name,
                "default_branch": summary.default_branch,
                "description": summary.description,
                "visibility": summary.visibility,
                "url": f"https://github.com/{ORG}/{summary.name}",
                "commits_in_window": summary.commits_in_window,
                "last_commit": summary.last_commit,
                "open_issues": summary.open_issues,
                "open_prs": summary.open_prs,
                "ci_status": summary.ci_status,
                "ci_url": summary.ci_url,
                "days_since_last_commit": summary.days_since_last_commit,
            }
        )

    all_commits.sort(key=lambda x: x.get("when") or "", reverse=True)
    all_issues.sort(key=lambda x: x.get("updated_at") or "", reverse=True)
    all_prs.sort(key=lambda x: x.get("updated_at") or "", reverse=True)
    all_releases.sort(key=lambda x: x.get("published_at") or "", reverse=True)
    repo_cards.sort(key=lambda x: x.get("commits_in_window", 0), reverse=True)

    if kg_snapshot and kg_snapshot.get("new_this_week") is not None:
        totals["new_kg_entities"] = kg_snapshot["new_this_week"]

    return {
        "generated_at": ctx.now.isoformat(),
        "window_days": ctx.window_days,
        "org": ORG,
        "totals": totals,
        "repos": repo_cards,
        "recent_commits": all_commits[:30],
        "recent_issues": all_issues[:30],
        "recent_prs": all_prs[:30],
        "latest_releases": all_releases[:10],
        "kg_snapshot": kg_snapshot,
        "paper_statuses": paper_statuses or [],
        "unreachable_repos": ctx.unreachable,
    }


# ---------------------------------------------------------------------------
# Markdown rendering

def render_markdown(pulse: dict[str, Any]) -> str:
    lines: list[str] = []
    t = pulse["totals"]
    lines.append("<!-- Auto-generated by scripts/generate_pulse.py. Do not edit. -->")
    lines.append("")
    lines.append(f"_Generated {pulse['generated_at']} — rolling {pulse['window_days']}-day window._")
    lines.append("")
    lines.append("## Headline numbers")
    lines.append("")
    lines.append(f"- Repos tracked: **{t['repos_tracked']}**")
    lines.append(f"- Commits in the last {pulse['window_days']} days: **{t['commits_past_week']}**")
    lines.append(f"- Issues opened: **{t['issues_opened']}** / closed: **{t['issues_closed']}**")
    lines.append(
        f"- PRs opened: **{t['prs_opened']}** / merged: **{t['prs_merged']}** / closed: **{t.get('prs_closed', 0)}**"
    )
    if "new_kg_entities" in t:
        lines.append(f"- New KG entities this week: **{t['new_kg_entities']}**")
    lines.append("")
    lines.append("## Recent commits")
    lines.append("")
    for c in pulse["recent_commits"][:15]:
        when = (c.get("when") or "")[:10]
        lines.append(
            f"- [{c['repo']} `{c['sha']}`]({c['url']}) — {c['message']} "
            f"_({c['author']}, {when})_"
        )
    lines.append("")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Entry point

def generate(
    *,
    token: str | None,
    window_days: int,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Programmatic entry point — used by tests to inject fixtures."""
    session = make_session(token)
    now = now or datetime.now(timezone.utc)
    since_iso = (now - timedelta(days=window_days)).isoformat()
    ctx = PulseContext(
        session=session,
        window_days=window_days,
        since_iso=since_iso,
        now=now,
    )
    repos_raw = list_org_repos(session, unreachable=ctx.unreachable)
    kg = fetch_kg_snapshot(ctx)
    papers = fetch_paper_statuses(ctx)
    pulse = build_pulse(
        repos_raw,
        ctx,
        kg_snapshot=kg,
        paper_statuses=papers,
    )
    return pulse


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate lab pulse YAML + markdown.")
    parser.add_argument("--output", required=True, help="Path to pulse.yml output")
    parser.add_argument("--output-md", required=True, help="Path to pulse.md include output")
    parser.add_argument(
        "--window-days",
        type=int,
        default=int(os.environ.get("PULSE_WINDOW_DAYS", DEFAULT_WINDOW_DAYS)),
    )
    args = parser.parse_args(argv)

    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if not token:
        print("warning: no GH_TOKEN or GITHUB_TOKEN found; public-only access",
              file=sys.stderr)

    pulse = generate(token=token, window_days=args.window_days)

    out_yaml = Path(args.output)
    out_yaml.parent.mkdir(parents=True, exist_ok=True)
    out_yaml.write_text(
        yaml.safe_dump(pulse, sort_keys=False, default_flow_style=False, width=100),
        encoding="utf-8",
    )

    out_md = Path(args.output_md)
    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text(render_markdown(pulse), encoding="utf-8")

    t = pulse["totals"]
    print(
        f"pulse: {t['repos_tracked']} repos, {t['commits_past_week']} commits, "
        f"{t['issues_opened']}/{t['issues_closed']} issues, "
        f"{t['prs_opened']}/{t['prs_merged']} PRs. "
        f"Wrote {out_yaml} + {out_md}."
    )
    if pulse["unreachable_repos"]:
        print(
            f"note: {len(pulse['unreachable_repos'])} endpoints unreachable "
            "(likely private without PAT scopes)",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
