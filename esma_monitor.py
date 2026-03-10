import json
import os
import shutil
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

URL = "https://www.esma.europa.eu/sites/default/files/library/esma34-46-101_esma_register_eltif_art33.xlsx"
HISTORY_DIR = Path("history")
LATEST_FILE = Path("esma_register_eltif_art33_ELTIFRG_latest.xlsx")
ISIN_CODES_DEFAULT_COUNTRY = "Italy"

COUNTRY_MAP = {
    "AT": "Austria", "BE": "Belgium", "BG": "Bulgaria", "HR": "Croatia", "CY": "Cyprus",
    "CZ": "Czech Republic", "DK": "Denmark", "EE": "Estonia", "FI": "Finland", "FR": "France",
    "DE": "Germany", "GR": "Greece", "HU": "Hungary", "IS": "Iceland", "IE": "Ireland",
    "IT": "Italy", "LV": "Latvia", "LI": "Liechtenstein", "LT": "Lithuania", "LU": "Luxembourg",
    "MT": "Malta", "NL": "Netherlands", "NO": "Norway", "PL": "Poland", "PT": "Portugal",
    "RO": "Romania", "SK": "Slovakia", "SI": "Slovenia", "ES": "Spain", "SE": "Sweden"
}

NON_SETTABLE_FIELDS = set()


def normalize_col_name(c: str) -> str:
    return " ".join(str(c).split()).strip().lower()


def resolve_col_name(df_in: pd.DataFrame, target_name: str) -> str:
    target_norm = normalize_col_name(target_name)
    matches = [c for c in df_in.columns if normalize_col_name(c) == target_norm]
    if not matches:
        raise KeyError(f"Column not found: {target_name}")
    return matches[0]


def resolve_col_name_candidates(df: pd.DataFrame, candidates: list[str]) -> str:
    norm_to_actual = {normalize_col_name(c): c for c in df.columns}

    for candidate in candidates:
        key = normalize_col_name(candidate)
        if key in norm_to_actual:
            return norm_to_actual[key]

    actual_norms = list(norm_to_actual.keys())
    for candidate in candidates:
        key = normalize_col_name(candidate)
        for actual in actual_norms:
            if key in actual or actual in key:
                return norm_to_actual[actual]

    raise KeyError(f"None of candidate columns found: {candidates}. Available: {list(df.columns)}")


def canonicalize(df_in: pd.DataFrame) -> pd.DataFrame:
    return (
        df_in
        .fillna("")
        .astype(str)
        .apply(lambda col: col.str.strip())
    )


def parse_isins(value) -> set:
    if pd.isna(value):
        return set()
    text = str(value).strip()
    if text == "" or text.lower() in {"nan", "none"}:
        return set()
    text = text.replace("\n", ";").replace(",", ";").replace("|", ";")
    parts = [p.strip().upper() for p in text.split(";")]
    return {p for p in parts if p}


def parse_semicolon_values(value) -> list[str]:
    if value is None:
        return []
    text = str(value).strip()
    if not text:
        return []
    parts = [p.strip().upper() for p in text.replace("\n", ";").split(";")]
    return [p for p in parts if p]


def ensure_country_column(df_in: pd.DataFrame, home_col: str) -> str:
    country_col_target = "Home Member State Country"
    existing = [c for c in df_in.columns if normalize_col_name(c) == normalize_col_name(country_col_target)]
    if existing:
        return existing[0]

    home_codes = df_in[home_col].fillna("").astype(str).str.strip().str.upper()
    df_in[country_col_target] = home_codes.map(COUNTRY_MAP).fillna("Unknown")
    return country_col_target


def load_isin_codes_sheet(file_path: Path) -> dict:
    try:
        df_isin = pd.read_excel(file_path, sheet_name="ISIN Codes")
    except Exception:
        return {}

    try:
        fund_col = resolve_col_name_candidates(df_isin, [
            "Name of the ELTIF", "ELTIF Name", "Fund Name", "Name"
        ])
        isin_col = resolve_col_name_candidates(df_isin, [
            "ISIN code of the ELTIF (where available)",
            "ISIN code", "ISIN codes", "ISIN", "ISIN Code(s)"
        ])
    except Exception:
        return {}

    out = {}
    temp = canonicalize(df_isin)
    for _, r in temp.iterrows():
        fund = str(r.get(fund_col, "")).strip()
        if not fund:
            continue
        key = fund.upper()
        isins = parse_isins(r.get(isin_col))
        out.setdefault(key, {"fund_name": fund, "isins": set()})
        out[key]["isins"].update(isins)

    return out


def build_daily_snapshot(today: str) -> Path:
    HISTORY_DIR.mkdir(exist_ok=True)
    daily_file = HISTORY_DIR / f"esma_register_eltif_art33_ELTIFRG_{today}.xlsx"

    xls = pd.ExcelFile(URL)
    if "ELTIFRG" not in xls.sheet_names:
        raise KeyError("Sheet 'ELTIFRG' not found in source workbook.")

    df_eltifrg = pd.read_excel(xls, sheet_name="ELTIFRG")
    df_isin_codes = pd.read_excel(xls, sheet_name="ISIN Codes") if "ISIN Codes" in xls.sheet_names else pd.DataFrame()

    home_state_col = resolve_col_name(df_eltifrg, "Home Member State")
    ensure_country_column(df_eltifrg, home_state_col)

    with pd.ExcelWriter(daily_file, engine="openpyxl") as writer:
        df_eltifrg.to_excel(writer, sheet_name="ELTIFRG", index=False)
        if not df_isin_codes.empty:
            df_isin_codes.to_excel(writer, sheet_name="ISIN Codes", index=False)

    return daily_file


def compare_snapshots(previous_file: Path, current_file: Path) -> tuple[pd.DataFrame, pd.DataFrame, Path]:
    prev_df_raw = pd.read_excel(previous_file)
    curr_df_raw = pd.read_excel(current_file)

    key_col_target = "Name of the ELTIF"
    isin_col_target = "ISIN codes of the ELTIF (each separate unit or share class), where available"
    home_state_col_target = "Home Member State"

    key_col_prev = resolve_col_name(prev_df_raw, key_col_target)
    key_col_curr = resolve_col_name(curr_df_raw, key_col_target)
    isin_col_prev = resolve_col_name(prev_df_raw, isin_col_target)
    isin_col_curr = resolve_col_name(curr_df_raw, isin_col_target)
    home_col_prev = resolve_col_name(prev_df_raw, home_state_col_target)
    home_col_curr = resolve_col_name(curr_df_raw, home_state_col_target)

    country_col_prev = ensure_country_column(prev_df_raw, home_col_prev)
    country_col_curr = ensure_country_column(curr_df_raw, home_col_curr)

    exclude_codes = {"IE", "LU"}
    prev_codes = prev_df_raw[home_col_prev].fillna("").astype(str).str.strip().str.upper()
    curr_codes = curr_df_raw[home_col_curr].fillna("").astype(str).str.strip().str.upper()
    prev_df = prev_df_raw.loc[~prev_codes.isin(exclude_codes)].copy()
    curr_df = curr_df_raw.loc[~curr_codes.isin(exclude_codes)].copy()

    prev_norm = canonicalize(prev_df)
    curr_norm = canonicalize(curr_df)

    prev_norm["_key"] = prev_norm[key_col_prev].str.upper()
    curr_norm["_key"] = curr_norm[key_col_curr].str.upper()

    prev_dupes = prev_df.loc[prev_norm.duplicated("_key", keep=False)].copy()
    curr_dupes = curr_df.loc[curr_norm.duplicated("_key", keep=False)].copy()

    prev_cmp = prev_norm.drop_duplicates("_key", keep="first").set_index("_key")
    curr_cmp = curr_norm.drop_duplicates("_key", keep="first").set_index("_key")

    added_keys = curr_cmp.index.difference(prev_cmp.index)
    new_funds = (
        curr_cmp.loc[added_keys, [key_col_curr, country_col_curr]].copy()
        if len(added_keys) > 0
        else pd.DataFrame(columns=[key_col_curr, country_col_curr])
    )
    new_funds = new_funds.rename(columns={key_col_curr: "fund_name", country_col_curr: "country"}).reset_index(drop=True)

    new_isins_by_key = {}

    for k in curr_cmp.index:
        prev_isins = parse_isins(prev_cmp.at[k, isin_col_prev]) if k in prev_cmp.index else set()
        curr_isins = parse_isins(curr_cmp.at[k, isin_col_curr])
        added = curr_isins - prev_isins
        if added:
            rec = new_isins_by_key.setdefault(k, {"isins": set(), "sources": set()})
            rec["isins"].update(added)
            rec["sources"].add("ELTIFRG")

    prev_isin_sheet = load_isin_codes_sheet(previous_file)
    curr_isin_sheet = load_isin_codes_sheet(current_file)

    for k, rec_curr in curr_isin_sheet.items():
        curr_set = rec_curr.get("isins", set())
        prev_set = prev_isin_sheet.get(k, {}).get("isins", set())
        added = curr_set - prev_set
        if added:
            rec = new_isins_by_key.setdefault(k, {"isins": set(), "sources": set()})
            rec["isins"].update(added)
            rec["sources"].add("ISIN Codes")

    new_isin_records = []
    for k, rec in sorted(new_isins_by_key.items()):
        if k in curr_cmp.index:
            fund_name = str(curr_cmp.at[k, key_col_curr]).strip()
            country_name = str(curr_cmp.at[k, country_col_curr]).strip() if country_col_curr in curr_cmp.columns else "Unknown"
        else:
            fund_name = curr_isin_sheet.get(k, {}).get("fund_name", k)
            country_name = ISIN_CODES_DEFAULT_COUNTRY

        isin_list = sorted(rec["isins"])
        new_isin_records.append(
            {
                "fund_name": fund_name,
                "country": country_name,
                "new_isin_codes": "; ".join(isin_list),
                "new_isin_count": len(isin_list),
                "source_tabs": ", ".join(sorted(rec["sources"])),
            }
        )

    new_isins = pd.DataFrame(new_isin_records)

    current_tag = current_file.stem.replace("esma_register_eltif_art33_ELTIFRG_", "")
    previous_tag = previous_file.stem.replace("esma_register_eltif_art33_ELTIFRG_", "")
    diff_file = HISTORY_DIR / f"diff_{current_tag}_vs_{previous_tag}.xlsx"

    with pd.ExcelWriter(diff_file, engine="openpyxl") as writer:
        new_funds.to_excel(writer, sheet_name="new_funds", index=False)
        new_isins.to_excel(writer, sheet_name="new_isins", index=False)
        prev_dupes.to_excel(writer, sheet_name="dupes_previous", index=False)
        curr_dupes.to_excel(writer, sheet_name="dupes_current", index=False)
        pd.DataFrame(
            {
                "metric": [
                    "current_rows_raw", "previous_rows_raw", "current_rows_processed", "previous_rows_processed",
                    "new_funds", "new_isin_funds", "new_isin_codes_total", "duplicate_names_previous", "duplicate_names_current"
                ],
                "value": [
                    len(curr_df_raw), len(prev_df_raw), len(curr_df), len(prev_df), len(new_funds), len(new_isins),
                    int(new_isins["new_isin_count"].sum()) if len(new_isins) > 0 else 0,
                    len(prev_dupes), len(curr_dupes),
                ],
            }
        ).to_excel(writer, sheet_name="summary", index=False)

    return new_funds, new_isins, diff_file


def _to_bool(v):
    if isinstance(v, bool):
        return v
    text = str(v).strip().lower()
    if text in {"true", "yes", "y", "1"}:
        return True
    if text in {"false", "no", "n", "0"}:
        return False
    return v


def _safe_json(resp):
    try:
        return resp.json()
    except Exception:
        return {}


def _load_json_env(var_name: str) -> dict:
    raw = os.getenv(var_name, "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        print(f"Invalid {var_name} JSON. Ignoring it.")
        return {}


def _merge_option_maps(primary: dict, secondary: dict) -> dict:
    merged = {**primary}
    for fk, mp in (secondary or {}).items():
        if fk not in merged or not isinstance(merged[fk], dict):
            merged[fk] = {}
        if isinstance(mp, dict):
            merged[fk].update({str(k): str(v) for k, v in mp.items()})
    return merged


def _get_field_catalog(jira_base_url: str, headers: dict, auth) -> dict:
    last_exc = None
    for endpoint in ["/rest/api/2/field", "/rest/api/3/field", "/rest/api/latest/field"]:
        try:
            r = requests.get(f"{jira_base_url}{endpoint}", headers=headers, auth=auth, timeout=30)
            if r.status_code == 404:
                continue
            r.raise_for_status()
            items = r.json()
            if isinstance(items, list):
                return {f.get("id"): f for f in items if f.get("id")}
        except Exception as e:
            last_exc = e
    if last_exc:
        raise last_exc
    raise ValueError("Could not load Jira field catalog.")


def _discover_option_ids_from_issues(project_key: str, target_field_keys: list[str], jira_base_url: str, headers: dict, auth) -> dict:
    if not target_field_keys:
        return {}

    field_list = ",".join(["key"] + target_field_keys)
    params = {
        "jql": f"project = {project_key} ORDER BY created DESC",
        "fields": field_list,
        "maxResults": 100,
    }

    r = requests.get(f"{jira_base_url}/rest/api/2/search", params=params, headers=headers, auth=auth, timeout=30)
    if not r.ok:
        print(f"Could not discover option IDs from search endpoint (HTTP {r.status_code}).")
        return {}

    data = r.json()
    issues = data.get("issues", []) if isinstance(data, dict) else []
    discovered = {}

    for issue in issues:
        fields = issue.get("fields", {}) if isinstance(issue, dict) else {}
        for fk in target_field_keys:
            val = fields.get(fk)
            if not val:
                continue

            discovered.setdefault(fk, {})

            if isinstance(val, dict):
                option_id = val.get("id")
                option_value = val.get("value") or val.get("name")
                if option_id and option_value:
                    discovered[fk][str(option_value)] = str(option_id)
            elif isinstance(val, list):
                for item in val:
                    if isinstance(item, dict):
                        option_id = item.get("id")
                        option_value = item.get("value") or item.get("name")
                        if option_id and option_value:
                            discovered[fk][str(option_value)] = str(option_id)

    return discovered


def _mark_non_settable_fields(error_dict: dict) -> bool:
    changed = False
    for fk, msg in (error_dict or {}).items():
        m = str(msg).lower()
        if "cannot be set" in m or "unknown" in m or "not on the appropriate screen" in m:
            if fk not in NON_SETTABLE_FIELDS:
                NON_SETTABLE_FIELDS.add(fk)
                changed = True
    return changed


def build_extra_fields(country_value: str, field_catalog: dict, option_id_map: dict, field_key_map: dict) -> dict:
    extras_by_name = {
        "Country": country_value,
        "DTD - Universe": "Open-End",
        "DTD - Data Type": "Add Investment - Complete Activation (Add New Fund)",
        "DTD - Identifier": "ao",
        "External Dependency": "Internal",
        "DTD - Root Cause Category": "No Issue: New Business Requirements",
        "VIP": "No",
    }

    name_to_key = {}
    for field_key, field_def in field_catalog.items():
        nm = str(field_def.get("name", "")).strip().lower()
        if nm:
            name_to_key[nm] = field_key

    result = {}
    for display_name, raw_value in extras_by_name.items():
        field_key = field_key_map.get(display_name) or name_to_key.get(display_name.lower())
        if not field_key or field_key in NON_SETTABLE_FIELDS:
            continue

        field_def = field_catalog.get(field_key, {})
        schema = field_def.get("schema", {})
        schema_type = schema.get("type")

        if schema_type == "boolean":
            result[field_key] = _to_bool(raw_value)

        elif schema_type == "array":
            items_type = schema.get("items")
            vals = raw_value if isinstance(raw_value, list) else [raw_value]
            if items_type == "option":
                id_lookup = option_id_map.get(field_key, {}) if isinstance(option_id_map, dict) else {}
                option_items = []
                for v in vals:
                    option_id = id_lookup.get(str(v)) if isinstance(id_lookup, dict) else None
                    option_items.append({"id": str(option_id)} if option_id else {"value": str(v)})
                result[field_key] = option_items
            else:
                result[field_key] = vals

        elif schema_type == "option":
            id_lookup = option_id_map.get(field_key, {}) if isinstance(option_id_map, dict) else {}
            option_id = id_lookup.get(str(raw_value)) if isinstance(id_lookup, dict) else None
            result[field_key] = {"id": str(option_id)} if option_id else {"value": str(raw_value)}

        else:
            result[field_key] = raw_value

    return result


def add_jira_comment(issue_key: str, comment_text: str, jira_base_url: str, headers: dict, auth):
    if not comment_text:
        return
    comment_url = f"{jira_base_url}/rest/api/2/issue/{issue_key}/comment"
    payload = {"body": comment_text}
    r = requests.post(comment_url, json=payload, headers=headers, auth=auth, timeout=30)
    if r.status_code in (200, 201):
        print(f"Added comment to {issue_key}")
    else:
        print(f"Failed to add comment to {issue_key}. HTTP {r.status_code}")
        print(r.text)


def create_jira_issue(summary: str, description: str, country_value: str, *, jira_base_url: str, project_key: str, issue_type: str, auto_comment: str, headers: dict, auth, field_catalog: dict, option_id_map: dict, field_key_map: dict) -> str:
    def _build_payload():
        p = {
            "fields": {
                "project": {"key": project_key},
                "summary": summary,
                "description": description,
                "issuetype": {"name": issue_type},
            }
        }
        p["fields"].update(build_extra_fields(country_value, field_catalog, option_id_map, field_key_map))
        return p

    create_url = f"{jira_base_url}/rest/api/2/issue"
    payload = _build_payload()
    response = requests.post(create_url, json=payload, headers=headers, auth=auth, timeout=30)

    if response.status_code == 201:
        issue_key = response.json().get("key")
        print(f"Created Jira ticket: {issue_key} | {summary}")
        add_jira_comment(issue_key, auto_comment, jira_base_url, headers, auth)
        return issue_key

    if response.status_code == 400:
        body = _safe_json(response)
        if _mark_non_settable_fields(body.get("errors", {})):
            print(f"Retrying without non-settable fields: {sorted(NON_SETTABLE_FIELDS)}")
            payload = _build_payload()
            response = requests.post(create_url, json=payload, headers=headers, auth=auth, timeout=30)
            if response.status_code == 201:
                issue_key = response.json().get("key")
                print(f"Created Jira ticket: {issue_key} | {summary}")
                add_jira_comment(issue_key, auto_comment, jira_base_url, headers, auth)
                return issue_key

    print(f"Failed to create issue for summary: {summary}")
    print(f"HTTP {response.status_code}")
    print(response.text)
    response.raise_for_status()


def create_jira_tickets(new_funds: pd.DataFrame, new_isins: pd.DataFrame):
    load_dotenv()

    jira_base_url = os.getenv("JIRA_BASE_URL", "https://msjira.morningstar.com").rstrip("/")
    project_key = os.getenv("JIRA_PROJECT_KEY", "DTD")
    issue_type = os.getenv("JIRA_ISSUE_TYPE", "Task")
    auto_comment = os.getenv("JIRA_AUTO_COMMENT", "Fund(s) identified automatically from the ESMA list of authorised ELTIFs.")

    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    auth = None

    bearer_token = os.getenv("JIRA_BEARER_TOKEN")
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"
    else:
        jira_user = os.getenv("JIRA_USER")
        jira_api_token = os.getenv("JIRA_API_TOKEN")
        if jira_user and jira_api_token:
            auth = HTTPBasicAuth(jira_user, jira_api_token)
        else:
            print("Jira credentials not set. Skipping ticket creation.")
            return

    field_catalog = _get_field_catalog(jira_base_url, headers, auth)
    field_key_map = _load_json_env("JIRA_FIELD_KEY_MAP")
    option_id_map_env = _load_json_env("JIRA_OPTION_ID_MAP")

    target_option_fields = list((field_key_map or {}).values())
    option_id_map_auto = _discover_option_ids_from_issues(project_key, target_option_fields, jira_base_url, headers, auth)
    option_id_map = _merge_option_maps(option_id_map_auto, option_id_map_env)

    # print("Auto-discovered option IDs:")
    # print(json.dumps(option_id_map_auto, indent=2))

    created_issue_keys = []
    failed_creations = []

    if isinstance(new_funds, pd.DataFrame) and not new_funds.empty:
        for _, row in new_funds.iterrows():
            fund_name = str(row.get("fund_name", "Unknown Fund")).strip()
            country = str(row.get("country", "Unknown")).strip()
            summary = f"New ELTIF found in {country}"
            description = f"Potential new ELTIF fund: {fund_name}\nPlease investigate and activate the fund accordingly."

            try:
                created_issue_keys.append(
                    create_jira_issue(
                        summary,
                        description,
                        country,
                        jira_base_url=jira_base_url,
                        project_key=project_key,
                        issue_type=issue_type,
                        auto_comment=auto_comment,
                        headers=headers,
                        auth=auth,
                        field_catalog=field_catalog,
                        option_id_map=option_id_map,
                        field_key_map=field_key_map,
                    )
                )
            except Exception as e:
                failed_creations.append({"summary": summary, "error": str(e)})

    if isinstance(new_isins, pd.DataFrame) and not new_isins.empty:
        for _, row in new_isins.iterrows():
            fund_name = str(row.get("fund_name", "Unknown Fund")).strip()
            country = str(row.get("country", "Unknown")).strip()
            source_tabs = str(row.get("source_tabs", "")).strip()

            isin_values = parse_semicolon_values(row.get("new_isin_codes", ""))
            if not isin_values and "new_isin_code" in row:
                single = str(row.get("new_isin_code", "")).strip().upper()
                isin_values = [single] if single else []

            isin_count = len(isin_values)
            isin_lines = "\n".join([f"- {x}" for x in isin_values]) if isin_values else "- n/a"

            summary = f"New ELTIF found in {country}"
            description = (
                f"Potential new ISIN(s) for ELTIF fund: {fund_name}\n"
                f"Count of new ISINs: {isin_count}\n"
                f"Source tab(s): {source_tabs if source_tabs else 'n/a'}\n"
                f"ISIN list:\n{isin_lines}\n\n"
                "Please investigate and activate the fund accordingly."
            )

            try:
                created_issue_keys.append(
                    create_jira_issue(
                        summary,
                        description,
                        country,
                        jira_base_url=jira_base_url,
                        project_key=project_key,
                        issue_type=issue_type,
                        auto_comment=auto_comment,
                        headers=headers,
                        auth=auth,
                        field_catalog=field_catalog,
                        option_id_map=option_id_map,
                        field_key_map=field_key_map,
                    )
                )
            except Exception as e:
                failed_creations.append({"summary": summary, "error": str(e)})

    print(f"Total Jira tickets created: {len(created_issue_keys)}")
    if created_issue_keys:
        print("Browse links:")
        for k in created_issue_keys:
            print(f"- {jira_base_url}/browse/{k}")

    if failed_creations:
        print(f"Failed ticket creations: {len(failed_creations)}")
        for item in failed_creations:
            print(f"- {item['summary']} | {item['error']}")


def keep_latest_snapshots(max_snapshots: int = 10):
    snapshot_files = sorted(HISTORY_DIR.glob("esma_register_eltif_art33_ELTIFRG_*.xlsx"))
    files_to_delete = snapshot_files[:-max_snapshots] if len(snapshot_files) > max_snapshots else []
    for old_file in files_to_delete:
        old_file.unlink(missing_ok=True)


def main():
    today = pd.Timestamp.today().date().isoformat()
    print(f"Run date: {today}")

    daily_file = build_daily_snapshot(today)
    print(f"Saved daily snapshot: {daily_file.resolve()}")

    if not LATEST_FILE.exists():
        shutil.copy2(daily_file, LATEST_FILE)
        print("No previous latest snapshot found. Initialized latest file; skipping compare and Jira.")
        return

    previous_file = LATEST_FILE
    new_funds, new_isins, diff_file = compare_snapshots(previous_file, daily_file)

    print(f"New funds: {len(new_funds)}")
    print(f"Funds with new ISIN codes: {len(new_isins)}")
    print(f"Diff report: {diff_file.resolve()}")

    shutil.copy2(daily_file, LATEST_FILE)
    keep_latest_snapshots(max_snapshots=10)

    if (isinstance(new_funds, pd.DataFrame) and not new_funds.empty) or (isinstance(new_isins, pd.DataFrame) and not new_isins.empty):
        create_jira_tickets(new_funds, new_isins)
    else:
        print("No new funds or ISIN changes. No Jira tickets created.")


if __name__ == "__main__":
    main()
