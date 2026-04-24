"""Tests for generate_pulse.py. Run with: pytest scripts/test_generate_pulse.py -q"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import yaml

import generate_pulse as gp


# ---------------------------------------------------------------------------
# Fixtures

FROZEN_NOW = datetime(2026, 4, 24, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def ctx():
    return gp.PulseContext(
        session=gp.make_session(token="dummy"),
        window_days=7,
        since_iso=(FROZEN_NOW - timedelta(days=7)).isoformat(),
        now=FROZEN_NOW,
    )


@pytest.fixture
def sample_commit():
    return {
        "sha": "abc123def4567890",
        "html_url": "https://github.com/foo/bar/commit/abc123def",
        "author": {"login": "gregfar"},
        "commit": {
            "author": {"name": "Greg", "date": "2026-04-23T08:12:00Z"},
            "message": "add pulse generator\n\nDetails here.",
        },
    }


@pytest.fixture
def sample_issue():
    return {
        "number": 42,
        "title": "Feature: lab pulse dashboard",
        "state": "open",
        "user": {"login": "gregfar"},
        "created_at": "2026-04-22T10:00:00Z",
        "updated_at": "2026-04-23T10:00:00Z",
        "closed_at": None,
        "html_url": "https://github.com/foo/bar/issues/42",
    }


@pytest.fixture
def sample_pr_merged():
    return {
        "number": 99,
        "title": "Merge pulse",
        "state": "closed",
        "user": {"login": "gregfar"},
        "created_at": "2026-04-20T10:00:00Z",
        "updated_at": "2026-04-23T10:00:00Z",
        "closed_at": "2026-04-23T10:00:00Z",
        "html_url": "https://github.com/foo/bar/pull/99",
        "pull_request": {"merged_at": "2026-04-23T10:00:00Z"},
    }


# ---------------------------------------------------------------------------
# 1. Utility functions

def test_days_since_returns_integer():
    past = (FROZEN_NOW - timedelta(days=3)).isoformat()
    assert gp.days_since(past, FROZEN_NOW) == 3


def test_days_since_handles_empty_string():
    assert gp.days_since("", FROZEN_NOW) is None


def test_truncate_message_first_line_only():
    assert gp.truncate_message("line1\nline2\nline3") == "line1"


def test_truncate_message_respects_limit():
    long = "x" * 500
    assert len(gp.truncate_message(long, limit=50)) == 50


# ---------------------------------------------------------------------------
# 2. Normalizers

def test_normalize_commit_shape(sample_commit):
    out = gp.normalize_commit("ont-ecosystem", sample_commit)
    assert out["repo"] == "ont-ecosystem"
    assert out["sha"] == "abc123d"
    assert out["author"] == "gregfar"
    assert out["message"] == "add pulse generator"
    assert out["url"] == "https://github.com/foo/bar/commit/abc123def"
    assert out["when"] == "2026-04-23T08:12:00Z"


def test_normalize_issue_vs_pr_kind(sample_issue, sample_pr_merged):
    i = gp.normalize_issue_or_pr("r", sample_issue, "issue")
    p = gp.normalize_issue_or_pr("r", sample_pr_merged, "pr")
    assert i["kind"] == "issue"
    assert i["merged_at"] is None
    assert p["kind"] == "pr"
    assert p["merged_at"] == "2026-04-23T10:00:00Z"


# ---------------------------------------------------------------------------
# 3. Deny list

def test_deny_list_contains_known_stale_repos():
    assert ".github" in gp.DENY_LIST
    assert "demo-repository" in gp.DENY_LIST
    assert "EndReason" in gp.DENY_LIST


def test_deny_list_does_not_contain_active_repos():
    for active in ("ont-ecosystem", "lab-wiki", "fragment-viewer",
                   "SMAseq_paper", "cas9-targeted-sequencing"):
        assert active not in gp.DENY_LIST


# ---------------------------------------------------------------------------
# 4. End-to-end aggregation against fake data

def test_build_pulse_end_to_end(monkeypatch, ctx, sample_commit,
                                sample_issue, sample_pr_merged):
    """Golden-file-ish test: stub network layer and assert the assembled shape."""

    repos_raw = [
        {
            "name": "ont-ecosystem",
            "default_branch": "main",
            "description": "framework",
            "private": True,
            "archived": False,
            "pushed_at": "2026-04-23T09:00:00Z",
        },
        {
            "name": "fragment-viewer",
            "default_branch": "main",
            "description": "viewer",
            "private": False,
            "archived": False,
            "pushed_at": "2026-04-22T09:00:00Z",
        },
    ]

    def fake_fetch_commits(c, repo, branch):
        return [sample_commit] if repo == "ont-ecosystem" else []

    def fake_fetch_issues_and_prs(c, repo):
        if repo == "ont-ecosystem":
            return [sample_issue], [sample_pr_merged]
        return [], []

    def fake_fetch_releases(c, repo):
        return []

    def fake_fetch_ci(c, repo, branch):
        return "success", "https://example/ci"

    monkeypatch.setattr(gp, "fetch_commits", fake_fetch_commits)
    monkeypatch.setattr(gp, "fetch_issues_and_prs", fake_fetch_issues_and_prs)
    monkeypatch.setattr(gp, "fetch_releases", fake_fetch_releases)
    monkeypatch.setattr(gp, "fetch_ci_status", fake_fetch_ci)

    pulse = gp.build_pulse(repos_raw, ctx)

    assert pulse["window_days"] == 7
    assert pulse["org"] == gp.ORG
    assert pulse["totals"]["repos_tracked"] == 2
    assert pulse["totals"]["commits_past_week"] == 1
    assert pulse["totals"]["issues_opened"] == 1
    assert pulse["totals"]["prs_merged"] == 1
    assert len(pulse["repos"]) == 2
    # repos sorted by commits in window (ont-ecosystem first)
    assert pulse["repos"][0]["name"] == "ont-ecosystem"
    assert pulse["repos"][0]["ci_status"] == "success"
    assert pulse["recent_commits"][0]["repo"] == "ont-ecosystem"


# ---------------------------------------------------------------------------
# 5. Schema / top-level keys

def test_pulse_schema_has_all_required_keys(monkeypatch, ctx):
    monkeypatch.setattr(gp, "fetch_commits", lambda *a, **k: [])
    monkeypatch.setattr(gp, "fetch_issues_and_prs", lambda *a, **k: ([], []))
    monkeypatch.setattr(gp, "fetch_releases", lambda *a, **k: [])
    monkeypatch.setattr(gp, "fetch_ci_status", lambda *a, **k: ("unknown", None))

    pulse = gp.build_pulse(
        [{"name": "x", "default_branch": "main", "pushed_at": "2026-04-23T00:00:00Z"}],
        ctx,
        kg_snapshot={"entities": 100, "mentions": 200, "new_this_week": 5,
                     "generated_at": "2026-04-24", "link": "..."},
        paper_statuses=[{"kind": "paper", "id": "smaseq-ng", "title": "...",
                         "status": "drafting", "lead": "diya"}],
    )

    required = {"generated_at", "window_days", "org", "totals", "repos",
                "recent_commits", "recent_issues", "recent_prs",
                "latest_releases", "kg_snapshot", "paper_statuses",
                "unreachable_repos"}
    assert required.issubset(pulse.keys())
    assert pulse["totals"]["new_kg_entities"] == 5
    assert pulse["paper_statuses"][0]["id"] == "smaseq-ng"


# ---------------------------------------------------------------------------
# 6. YAML round-trip

def test_yaml_round_trip_matches(tmp_path: Path, monkeypatch, ctx):
    monkeypatch.setattr(gp, "fetch_commits", lambda *a, **k: [])
    monkeypatch.setattr(gp, "fetch_issues_and_prs", lambda *a, **k: ([], []))
    monkeypatch.setattr(gp, "fetch_releases", lambda *a, **k: [])
    monkeypatch.setattr(gp, "fetch_ci_status", lambda *a, **k: ("unknown", None))
    pulse = gp.build_pulse(
        [{"name": "x", "default_branch": "main", "pushed_at": "2026-04-23T00:00:00Z"}],
        ctx,
    )
    p = tmp_path / "pulse.yml"
    p.write_text(yaml.safe_dump(pulse, sort_keys=False))
    parsed = yaml.safe_load(p.read_text())
    assert parsed["totals"]["repos_tracked"] == 1


# ---------------------------------------------------------------------------
# 7. Markdown rendering

def test_render_markdown_has_headlines_and_no_emoji(monkeypatch, ctx,
                                                    sample_commit):
    monkeypatch.setattr(gp, "fetch_commits", lambda *a, **k: [sample_commit])
    monkeypatch.setattr(gp, "fetch_issues_and_prs", lambda *a, **k: ([], []))
    monkeypatch.setattr(gp, "fetch_releases", lambda *a, **k: [])
    monkeypatch.setattr(gp, "fetch_ci_status", lambda *a, **k: ("unknown", None))

    pulse = gp.build_pulse(
        [{"name": "ont-ecosystem", "default_branch": "main",
          "pushed_at": "2026-04-23T09:00:00Z"}],
        ctx,
    )
    md = gp.render_markdown(pulse)
    assert "Headline numbers" in md
    assert "Recent commits" in md
    assert "ont-ecosystem" in md
    # Basic emoji-free assertion: check that no character has codepoint > 0x2700
    # (common emoji range start). Allow em-dashes / pipes / standard punctuation.
    for ch in md:
        assert ord(ch) < 0x2600, f"emoji-like codepoint found: {hex(ord(ch))} ({ch!r})"


# ---------------------------------------------------------------------------
# 8. gh_get returns None on 404 without raising

class _FakeResp:
    def __init__(self, status, body=None):
        self.status_code = status
        self._body = body or {}

    def json(self):
        return self._body


def test_gh_get_returns_none_on_404(monkeypatch):
    session = gp.make_session(None)
    calls = {}

    def fake_get(url, params=None, timeout=None):
        calls["url"] = url
        return _FakeResp(404)

    monkeypatch.setattr(session, "get", fake_get)
    unreachable: list = []
    res = gp.gh_get(session, "/repos/foo/bar", unreachable=unreachable)
    assert res is None
    assert unreachable and unreachable[0]["error"] == "http-404"


# ---------------------------------------------------------------------------
# 9. Session auth header shape

def test_make_session_with_token_sets_bearer():
    s = gp.make_session("xyz")
    assert s.headers["Authorization"] == "Bearer xyz"
    assert "application/vnd.github+json" in s.headers["Accept"]


def test_make_session_without_token_has_no_auth():
    s = gp.make_session(None)
    assert "Authorization" not in s.headers


# ---------------------------------------------------------------------------
# 10. PR merged vs closed distinction

def test_merged_pr_counts_under_prs_merged_not_prs_closed(monkeypatch, ctx,
                                                           sample_pr_merged):
    monkeypatch.setattr(gp, "fetch_commits", lambda *a, **k: [])
    monkeypatch.setattr(gp, "fetch_issues_and_prs",
                        lambda c, r: ([], [sample_pr_merged]))
    monkeypatch.setattr(gp, "fetch_releases", lambda *a, **k: [])
    monkeypatch.setattr(gp, "fetch_ci_status", lambda *a, **k: ("unknown", None))

    pulse = gp.build_pulse(
        [{"name": "r", "default_branch": "main",
          "pushed_at": "2026-04-23T00:00:00Z"}],
        ctx,
    )
    assert pulse["totals"]["prs_merged"] == 1
    assert pulse["totals"]["prs_closed"] == 0


# ---------------------------------------------------------------------------
# 11. Deny-list filtering through list_org_repos

def test_list_org_repos_filters_archived_and_deny(monkeypatch):
    session = gp.make_session(None)
    raw_payload = [
        {"name": "ont-ecosystem", "archived": False},
        {"name": "demo-repository", "archived": False},  # denied
        {"name": "archived-thing", "archived": True},    # filtered
        {"name": "fragment-viewer", "archived": False},
    ]

    def fake_paginate(sess, path, *, params=None, max_pages=3,
                      unreachable=None):
        return raw_payload

    monkeypatch.setattr(gp, "gh_paginate", fake_paginate)
    kept = gp.list_org_repos(session)
    names = [r["name"] for r in kept]
    assert "ont-ecosystem" in names
    assert "fragment-viewer" in names
    assert "demo-repository" not in names
    assert "archived-thing" not in names
