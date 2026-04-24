# Lab Pulse scripts

`generate_pulse.py` builds the nightly `_data/pulse.yml` data file + a
`_includes/pulse.md` markdown include. Runs from the repo root on every
nightly cron in `.github/workflows/lab-pulse.yml`.

## Local run

```bash
export GH_TOKEN=$(gh auth token)
python3 scripts/generate_pulse.py --output _data/pulse.yml --output-md _includes/pulse.md
```

## Tests

```bash
cd scripts
pytest test_generate_pulse.py -q
```

## Authentication

The workflow prefers `secrets.LAB_REFRESH_PAT` (fine-grained PAT with
`contents:read`, `metadata:read`, `issues:read`, `pull_requests:read`,
and `actions:read` scopes across every org repo). Falls back to the
default `GITHUB_TOKEN`, which can only read public repos.

To set up the PAT:

1. Create a fine-grained PAT at https://github.com/settings/tokens?type=beta
2. Resource owner: `Single-Molecule-Sequencing`.
3. Repository access: "All repositories".
4. Permissions (Repository): Contents: **Read**, Metadata: **Read**,
   Issues: **Read**, Pull requests: **Read**, Actions: **Read**.
5. Copy the PAT.
6. `gh secret set LAB_REFRESH_PAT -R Single-Molecule-Sequencing/single-molecule-sequencing.github.io`

Without the PAT, the generator still runs but only sees public repos
(`fragment-viewer`, `ONT-SMA-seq`, `CypScope`, `barbell`, `dorado-run`,
`dorado-bench`, `PGx-prep`, `CypScope-prep`, `End_Reason_nf`).

## Output

`_data/pulse.yml` carries:

- `totals` (repos_tracked, commits_past_week, issues_opened / closed, prs_opened / merged / closed, releases_recent, new_kg_entities)
- `repos[]` — per-repo card (commits_in_window, last_commit, open_issues, open_prs, ci_status, days_since_last_commit)
- `recent_commits[]` — top 30 org-wide, reverse-chronological
- `recent_issues[]` / `recent_prs[]` — top 30 of each
- `latest_releases[]` — last 10 releases (30-day window)
- `kg_snapshot` — pulled from `lab-wiki/docs/kg-snapshots/latest/_toc.json` when available
- `paper_statuses[]` — from `lab-papers/papers.yaml`
- `unreachable_repos[]` — endpoints that 403/404'd during the run

Consumers:

- `index.html` (headline strip) reads `site.data.pulse.totals`
- `_layouts/pulse.html` renders the full dashboard
