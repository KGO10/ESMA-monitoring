# ESMA ELTIF Monitor

Automates the ESMA ELTIF register check, stores snapshot history, compares changes, and creates Jira tickets for:
- new funds
- new ISINs (grouped per fund)

## Project files
- [esma_monitor.py](esma_monitor.py): main runnable script
- [.github/workflows/esma_monitor.yml](.github/workflows/esma_monitor.yml): GitHub Actions workflow
- [requirements.txt](requirements.txt): Python dependencies
- [history/](history/): daily snapshots + diff reports

## Local run
1. Create and activate a virtual environment.
2. Install dependencies:
   - `pip install -r requirements.txt`
3. Create/update [.env](.env) with Jira settings.
4. Run:
   - `python esma_monitor.py`

## Required environment variables
Set in local [.env](.env) and as GitHub Secrets:

- `JIRA_BASE_URL` (example: `https://msjira.morningstar.com`)
- `JIRA_PROJECT_KEY` (example: `DTD`)
- `JIRA_ISSUE_TYPE` (example: `Task`)
- Auth: either
  - `JIRA_BEARER_TOKEN`
  - or both `JIRA_USER` and `JIRA_API_TOKEN`
- `JIRA_FIELD_KEY_MAP` (JSON object)
- `JIRA_OPTION_ID_MAP` (JSON object)
- `JIRA_AUTO_COMMENT` (optional; default is used if missing)

## GitHub Actions schedule
Workflow uses this cron:

- `18 3 * * *`

GitHub schedules are UTC. So this is **03:18 UTC daily**.

Examples:
- CET (UTC+1): 04:18
- CEST (UTC+2): 05:18

## GitHub repo setup checklist
1. Push project to GitHub.
2. Add all required Secrets in repository settings.
3. Confirm workflow exists at [.github/workflows/esma_monitor.yml](.github/workflows/esma_monitor.yml).
4. Run once manually using **Run workflow**.
5. Confirm:
   - snapshot files are updated in [history/](history/)
   - Jira tickets are created when new records are found

## Notes
- IE and LU domiciles are excluded from comparison logic.
- ISIN Codes-only funds default to country Italy.
- Workflow commits updated snapshot files back to the repository.
