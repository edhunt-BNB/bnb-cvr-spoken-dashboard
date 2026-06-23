#!/usr/bin/env python3
"""
update_data.py  v2.2

Reads telesales lead data from Airtable and produces data.json
for a GitHub Pages dashboard.

Changes from v1:
- UTC-safe date parsing and Monday calculation
- Aggregation by last_call_date in addition to import_date
- List ID aggregation (list_summary and list_weekly)
- Reduced logging (every 10th page + summary)
- Extended output structure
"""

import json
import os
import sys
import time
import datetime
import urllib.request
import urllib.parse
from collections import defaultdict


AIRTABLE_BASE_ID = "appc3AWUlFaHlmdWk"
AIRTABLE_TABLE_NAME = "BNB Report"
AIRTABLE_TABLE_ID = "tblkhYU7kDPoA6V23"

SPOKEN_OUTCOMES = {"Sale", "Bad Data", "Convertible"}

CONVERTIBLE_CODES = ["PAYISSUE", "FREEZSPACE", "HEALTH", "CALLBACK", "MEDIFOOD", "TOOEXP", "FUSSY", "NI"]

FILTER_FORMULA = 'OR({Result Outcome}="Sale",{Result Outcome}="Bad Data",{Result Outcome}="Convertible")'

FIELDS = [
    "Agent First Name",
    "Agent Last Name",
    "Result Outcome",
    "Result Code",
    "import_date",
    "Is Final",
    "last_call_date",
    "Original List ID",
]


def log(msg):
    """Log a message to stderr."""
    print(msg, file=sys.stderr)


def parse_date_utc(date_str):
    """Parse a date string as UTC, returning a datetime.date or None."""
    if not date_str:
        return None
    parts = date_str[:10].split('-')
    if len(parts) != 3:
        return None
    try:
        return datetime.date(int(parts[0]), int(parts[1]), int(parts[2]))
    except (ValueError, TypeError):
        return None


def get_monday_of_week(d):
    """Return the Monday of the ISO week containing date d, UTC-safe."""
    iso_cal = d.isocalendar()
    jan4 = datetime.date(iso_cal[0], 1, 4)
    week1_monday = jan4 - datetime.timedelta(days=jan4.weekday())
    return week1_monday + datetime.timedelta(weeks=iso_cal[1] - 1)


def fetch_all_records(pat):
    """Fetch all spoken-to records from Airtable using pagination."""
    records = []
    offset = None
    page_num = 0

    encoded_formula = urllib.parse.quote(FILTER_FORMULA)
    fields_param = "&".join("fields[]=" + urllib.parse.quote(f) for f in FIELDS)
    base_url = (
        "https://api.airtable.com/v0/"
        + AIRTABLE_BASE_ID
        + "/"
        + urllib.parse.quote(AIRTABLE_TABLE_NAME)
        + "?pageSize=100&filterByFormula="
        + encoded_formula
        + "&"
        + fields_param
    )

    headers = {
        "Authorization": "Bearer " + pat,
        "Content-Type": "application/json",
    }

    while True:
        url = base_url
        if offset:
            url = url + "&offset=" + urllib.parse.quote(str(offset))

        page_num += 1

        # Only log every 10th page to keep output manageable
        if page_num % 10 == 1:
            log("Fetching page " + str(page_num) + " (records so far: " + str(len(records)) + ")")

        req = urllib.request.Request(url, headers=headers)

        try:
            with urllib.request.urlopen(req) as response:
                data = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            error_body = e.read().decode("utf-8")
            log("HTTP error " + str(e.code) + ": " + error_body)
            raise
        except urllib.error.URLError as e:
            log("URL error: " + str(e.reason))
            raise

        page_records = data.get("records", [])
        records.extend(page_records)

        offset = data.get("offset")
        if not offset:
            log("Fetch complete. Pages: " + str(page_num) + ", total records: " + str(len(records)))
            break

        # Rate limit: 5 requests/sec max, sleep 0.25s between calls
        time.sleep(0.25)

    return records


def parse_record(record):
    """
    Parse a single Airtable record into a structured dict.
    Returns None if the record should be skipped.
    """
    fields = record.get("fields", {})

    # Agent name
    first_name = (fields.get("Agent First Name") or "").strip()
    last_name = (fields.get("Agent Last Name") or "").strip()
    if not first_name and not last_name:
        return None
    agent_name = (first_name + " " + last_name).strip()

    # Result outcome
    outcome = (fields.get("Result Outcome") or "").strip()
    if outcome not in SPOKEN_OUTCOMES:
        return None

    # import_date (UTC-safe)
    import_date = parse_date_utc(fields.get("import_date", ""))
    if import_date is None:
        return None

    import_week_key = get_monday_of_week(import_date).isoformat()
    import_month_key = datetime.date(import_date.year, import_date.month, 1).isoformat()

    # last_call_date (UTC-safe) — may be absent
    last_call_date = parse_date_utc(fields.get("last_call_date", ""))
    if last_call_date is not None:
        last_call_week_key = get_monday_of_week(last_call_date).isoformat()
        last_call_month_key = datetime.date(last_call_date.year, last_call_date.month, 1).isoformat()
    else:
        last_call_week_key = None
        last_call_month_key = None

    # Original List ID (numeric field, may be absent or None)
    list_id_raw = fields.get("Original List ID")
    try:
        list_id = int(list_id_raw) if list_id_raw is not None else None
    except (ValueError, TypeError):
        list_id = None

    # Result code (for Convertible sub-codes)
    result_code = (fields.get("Result Code") or "").strip().upper()

    return {
        "agent_name": agent_name,
        "import_week_key": import_week_key,
        "import_month_key": import_month_key,
        "last_call_week_key": last_call_week_key,
        "last_call_month_key": last_call_month_key,
        "list_id": list_id,
        "outcome": outcome,
        "result_code": result_code,
    }


def make_empty_agent_bucket():
    """Create an empty per-agent aggregation bucket."""
    bucket = {
        "total_spoken": 0,
        "sales": 0,
        "bad_data": 0,
        "convertible_total": 0,
    }
    for code in CONVERTIBLE_CODES:
        bucket[code.lower()] = 0
    return bucket


def make_empty_list_bucket():
    """Create an empty per-list aggregation bucket."""
    return {
        "total_spoken": 0,
        "sales": 0,
        "bad_data": 0,
        "convertible_total": 0,
    }


def tally_outcome(bucket, outcome, result_code):
    """Increment the appropriate counters in a bucket for one record."""
    bucket["total_spoken"] += 1
    if outcome == "Sale":
        bucket["sales"] += 1
    elif outcome == "Bad Data":
        bucket["bad_data"] += 1
    elif outcome == "Convertible":
        bucket["convertible_total"] += 1
        if result_code in [c.upper() for c in CONVERTIBLE_CODES]:
            if result_code.lower() in bucket:
                bucket[result_code.lower()] += 1


def aggregate_records(parsed_records, cutoff_weeks, cutoff_months):
    """
    Aggregate parsed records into all output buckets.

    Returns a tuple of six dicts:
      weekly_data, monthly_data,
      weekly_lc_data, monthly_lc_data,
      list_summary_data, list_weekly_data
    """
    # (agent_name, period_key) -> agent bucket
    weekly_data = defaultdict(make_empty_agent_bucket)
    monthly_data = defaultdict(make_empty_agent_bucket)
    weekly_lc_data = defaultdict(make_empty_agent_bucket)
    monthly_lc_data = defaultdict(make_empty_agent_bucket)

    # list_id -> list bucket (no date filter)
    list_summary_data = defaultdict(make_empty_list_bucket)
    # (list_id, week_key) -> list bucket (last_call_date as time axis)
    list_weekly_data = defaultdict(make_empty_list_bucket)

    convertible_upper = [c.upper() for c in CONVERTIBLE_CODES]

    for r in parsed_records:
        agent = r["agent_name"]
        outcome = r["outcome"]
        result_code = r["result_code"]
        list_id = r["list_id"]

        # --- import_date weekly ---
        wk = r["import_week_key"]
        if wk in cutoff_weeks:
            tally_outcome(weekly_data[(agent, wk)], outcome, result_code)

        # --- import_date monthly ---
        mk = r["import_month_key"]
        if mk in cutoff_months:
            tally_outcome(monthly_data[(agent, mk)], outcome, result_code)

        # --- last_call_date weekly ---
        lc_wk = r["last_call_week_key"]
        if lc_wk and lc_wk in cutoff_weeks:
            tally_outcome(weekly_lc_data[(agent, lc_wk)], outcome, result_code)

        # --- last_call_date monthly ---
        lc_mk = r["last_call_month_key"]
        if lc_mk and lc_mk in cutoff_months:
            tally_outcome(monthly_lc_data[(agent, lc_mk)], outcome, result_code)

        # --- list_summary (all records, no date filter) ---
        if list_id is not None:
            lb = list_summary_data[list_id]
            lb["total_spoken"] += 1
            if outcome == "Sale":
                lb["sales"] += 1
            elif outcome == "Bad Data":
                lb["bad_data"] += 1
            elif outcome == "Convertible":
                lb["convertible_total"] += 1

        # --- list_weekly (by last_call_date, within cutoff) ---
        if list_id is not None and lc_wk and lc_wk in cutoff_weeks:
            lwb = list_weekly_data[(list_id, lc_wk)]
            lwb["total_spoken"] += 1
            if outcome == "Sale":
                lwb["sales"] += 1
            elif outcome == "Bad Data":
                lwb["bad_data"] += 1
            elif outcome == "Convertible":
                lwb["convertible_total"] += 1

    return (
        weekly_data,
        monthly_data,
        weekly_lc_data,
        monthly_lc_data,
        list_summary_data,
        list_weekly_data,
    )


def build_agent_rows(data_dict):
    """Convert an (agent_name, period_key) -> bucket dict into a sorted list of row dicts."""
    rows = []
    for (agent_name, period_key), bucket in sorted(
        data_dict.items(), key=lambda x: (x[0][1], x[0][0])
    ):
        row = {
            "agent_name": agent_name,
            "period_start": period_key,
            "total_spoken": bucket["total_spoken"],
            "sales": bucket["sales"],
            "bad_data": bucket["bad_data"],
            "convertible_total": bucket["convertible_total"],
        }
        for code in CONVERTIBLE_CODES:
            row[code.lower()] = bucket[code.lower()]
        rows.append(row)
    return rows


def build_output(
    weekly_data, monthly_data,
    weekly_lc_data, monthly_lc_data,
    list_summary_data, list_weekly_data,
    today,
):
    """Build the final output dict."""
    now_utc = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    period_end = today.isoformat()
    period_start_13w = (today - datetime.timedelta(weeks=13)).isoformat()

    # list_summary rows
    list_summary_rows = []
    for list_id, bucket in sorted(list_summary_data.items()):
        list_summary_rows.append({
            "list_id": list_id,
            "total_spoken": bucket["total_spoken"],
            "sales": bucket["sales"],
            "bad_data": bucket["bad_data"],
            "convertible_total": bucket["convertible_total"],
        })

    # list_weekly rows
    list_weekly_rows = []
    for (list_id, period_key), bucket in sorted(
        list_weekly_data.items(), key=lambda x: (x[0][1], x[0][0])
    ):
        list_weekly_rows.append({
            "list_id": list_id,
            "period_start": period_key,
            "total_spoken": bucket["total_spoken"],
            "sales": bucket["sales"],
            "bad_data": bucket["bad_data"],
            "convertible_total": bucket["convertible_total"],
        })

    return {
        "last_updated": now_utc,
        "period_start": period_start_13w,
        "period_end": period_end,
        "weekly": build_agent_rows(weekly_data),
        "monthly": build_agent_rows(monthly_data),
        "weekly_last_call": build_agent_rows(weekly_lc_data),
        "monthly_last_call": build_agent_rows(monthly_lc_data),
        "list_summary": list_summary_rows,
        "list_weekly": list_weekly_rows,
    }


def compute_cutoff_periods(today, num_weeks=13, num_months=3):
    """
    Compute the set of week and month period_start strings to include.
    Uses UTC-safe Monday calculation.
    """
    current_week_monday = get_monday_of_week(today)

    cutoff_weeks = set()
    for i in range(num_weeks):
        week_start = current_week_monday - datetime.timedelta(weeks=i)
        cutoff_weeks.add(week_start.isoformat())

    cutoff_months = set()
    year = today.year
    month = today.month
    for i in range(num_months):
        cutoff_months.add(datetime.date(year, month, 1).isoformat())
        month -= 1
        if month == 0:
            month = 12
            year -= 1

    return cutoff_weeks, cutoff_months


def main():
    pat = os.environ.get("AIRTABLE_PAT")
    if not pat:
        log("ERROR: AIRTABLE_PAT environment variable is not set.")
        sys.exit(1)

    if len(sys.argv) > 1:
        output_path = sys.argv[1]
    else:
        output_path = "./data.json"

    today = datetime.date.today()
    log("Starting data fetch (v2.2). Today: " + today.isoformat())

    cutoff_weeks, cutoff_months = compute_cutoff_periods(today, num_weeks=13, num_months=3)
    log(
        "Cutoff periods: "
        + str(len(cutoff_weeks)) + " weeks, "
        + str(len(cutoff_months)) + " months."
    )

    log("Fetching records from Airtable...")
    raw_records = fetch_all_records(pat)

    log("Parsing " + str(len(raw_records)) + " records...")
    parsed_records = []
    skipped = 0
    for record in raw_records:
        parsed = parse_record(record)
        if parsed is None:
            skipped += 1
        else:
            parsed_records.append(parsed)
    log("Parsed: " + str(len(parsed_records)) + ", skipped: " + str(skipped))

    log("Aggregating...")
    (
        weekly_data, monthly_data,
        weekly_lc_data, monthly_lc_data,
        list_summary_data, list_weekly_data,
    ) = aggregate_records(parsed_records, cutoff_weeks, cutoff_months)

    output = build_output(
        weekly_data, monthly_data,
        weekly_lc_data, monthly_lc_data,
        list_summary_data, list_weekly_data,
        today,
    )

    output_json = json.dumps(output, indent=2)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(output_json)

    log(
        "Done. Written to: " + output_path + " | "
        + "weekly=" + str(len(output["weekly"]))
        + " monthly=" + str(len(output["monthly"]))
        + " weekly_last_call=" + str(len(output["weekly_last_call"]))
        + " monthly_last_call=" + str(len(output["monthly_last_call"]))
        + " list_summary=" + str(len(output["list_summary"]))
        + " list_weekly=" + str(len(output["list_weekly"]))
    )


if __name__ == "__main__":
    main()
