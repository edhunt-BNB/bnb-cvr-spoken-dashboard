#!/usr/bin/env python3
"""
update_data.py  v2.6

Reads telesales lead data from Airtable and produces data.json
for a GitHub Pages dashboard.

Changes from v2.5:
- New weekly Sale-outcome talk time aggregation for the Weekly Sale Trend sub-tab:
    talk_time_weekly_sale_by_agent (agent, week_start, count, avg_talk in seconds)
- New daily talk time for the last fully completed Monday-Sunday week:
    talk_time_last_week_daily (agent, day_of_week_iso 1-7, count, avg_talk in seconds)
- talk_time_meta now carries last_completed_week_start and last_completed_week_end
  so the front-end can label Chart 2 with the exact date range it is showing.
- Business week numbering (BNB internal: Week 1 starts Monday 29 Dec 2025) is
  computed client-side from the ISO Monday date; not stored in data.json.

Changes from v2.4:
- New talk-time bins for the Outcome Analysis histogram:
    0-1m, 1-3m, 3-5m, 5-10m, 10m+ (was 0-30s / 30-60s / 1-2m / 2-5m / 5m+)
- Attempts analysis extended: bins now 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 10+
- New AGAM > 60s rule added to Suspicious Calls (same shape as HANGUP > 60s)
- New conversion_by_talk_length section: per (agent, minute_bucket) for buckets
  5..14 and 15+, tracks total calls and Sale outcomes. Feeds a new chart
  showing conversion rate by call duration.
- Suspicious call drilldowns now include a MASKED phone snippet
  (first 6 digits, remainder masked with *) plus the call date. Full phone
  still requires the Airtable record click-through. Compromise between PII
  safety and Ed's need to cross-reference calls in the dialer log.
- Suspicious agent tiles: aggregate flag counts per (agent, rule) for the UI.

Changes from v2.3:
- Added MIN_CALLS_PER_AGENT floor (default 20) applied across every talk-time
  diagnostic output.

Changes from v2.2:
- Extended penetration pass to also carry Talk Time, Result Code,
  Agent name, Lead Total Attempts, last_call_date so talk time
  diagnostics can be computed from the same fetch (no extra API pass).
- New aggregate_talk_time() emits diagnostics for the Talk Time QA tab.
"""

import json
import os
import sys
import time
import datetime
import urllib.request
import urllib.parse
from collections import defaultdict
from statistics import median


AIRTABLE_BASE_ID = "appc3AWUlFaHlmdWk"
AIRTABLE_TABLE_NAME = "BNB Report"
AIRTABLE_TABLE_ID = "tblkhYU7kDPoA6V23"

SPOKEN_OUTCOMES = {"Sale", "Bad Data", "Convertible"}

CONVERTIBLE_CODES = ["PAYISSUE", "FREEZSPACE", "HEALTH", "CALLBACK", "MEDIFOOD", "TOOEXP", "FUSSY", "NI"]

FILTER_FORMULA = 'OR({Result Outcome}="Sale",{Result Outcome}="Bad Data",{Result Outcome}="Convertible")'

# Widened penetration-pass fields: adds fields needed for talk-time diagnostics
# Phone Num is fetched but only a masked snippet (first 6 digits) is written to
# data.json - the full number never leaves the pipeline environment.
PENETRATION_FIELDS = [
    "Original List ID",
    "Result Outcome",
    "import_date",
    "Talk Time",
    "Agent First Name",
    "Agent Last Name",
    "Result Code",
    "Lead Total Attempts",
    "last_call_date",
    "Phone Num",
]

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

# Talk-time diagnostics config
HANGUP_TALK_TIME_THRESHOLD = 60   # seconds
AGAM_TALK_TIME_THRESHOLD = 60     # seconds; AGAM calls with talk > this flag as suspicious
SKEW_THRESHOLD_PCT = 25            # % deviation vs team avg
SKEW_MIN_CALLS = 5                 # minimum calls per (agent, code) to be eligible for skew flag
SKEW_MIN_TEAM_CALLS = 20           # minimum team calls for that code to compute a team baseline
SKEW_MIN_TEAM_AVG = 5              # seconds; ignore team baselines below this to avoid tiny-number noise
SUSPICIOUS_SAMPLE_CAP = 50         # cap sample records per flagged combo
MIN_CALLS_PER_AGENT = 20           # agent-level floor: agents below this are excluded from all talk-time diagnostics
CONVERSION_MIN_MINUTES = 5         # conversion-by-length starts at 5-minute calls
CONVERSION_MAX_MINUTES = 15        # 15+ becomes the final bucket


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

        time.sleep(0.25)

    return records


def parse_record(record):
    """
    Parse a single Airtable record into a structured dict.
    Returns None if the record should be skipped.
    """
    fields = record.get("fields", {})

    first_name = (fields.get("Agent First Name") or "").strip()
    last_name = (fields.get("Agent Last Name") or "").strip()
    if not first_name and not last_name:
        return None
    agent_name = (first_name + " " + last_name).strip()

    outcome = (fields.get("Result Outcome") or "").strip()
    if outcome not in SPOKEN_OUTCOMES:
        return None

    import_date = parse_date_utc(fields.get("import_date", ""))
    if import_date is None:
        return None

    import_week_key = get_monday_of_week(import_date).isoformat()
    import_month_key = datetime.date(import_date.year, import_date.month, 1).isoformat()

    last_call_date = parse_date_utc(fields.get("last_call_date", ""))
    if last_call_date is not None:
        last_call_week_key = get_monday_of_week(last_call_date).isoformat()
        last_call_month_key = datetime.date(last_call_date.year, last_call_date.month, 1).isoformat()
    else:
        last_call_week_key = None
        last_call_month_key = None

    list_id_raw = fields.get("Original List ID")
    try:
        list_id = int(list_id_raw) if list_id_raw is not None else None
    except (ValueError, TypeError):
        list_id = None

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
    return {
        "total_spoken": 0,
        "sales": 0,
        "bad_data": 0,
        "convertible_total": 0,
    }


def tally_outcome(bucket, outcome, result_code):
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
    weekly_data = defaultdict(make_empty_agent_bucket)
    monthly_data = defaultdict(make_empty_agent_bucket)
    weekly_lc_data = defaultdict(make_empty_agent_bucket)
    monthly_lc_data = defaultdict(make_empty_agent_bucket)
    list_summary_data = defaultdict(make_empty_list_bucket)
    list_weekly_data = defaultdict(make_empty_list_bucket)

    for r in parsed_records:
        agent = r["agent_name"]
        outcome = r["outcome"]
        result_code = r["result_code"]
        list_id = r["list_id"]

        wk = r["import_week_key"]
        if wk in cutoff_weeks:
            tally_outcome(weekly_data[(agent, wk)], outcome, result_code)

        mk = r["import_month_key"]
        if mk in cutoff_months:
            tally_outcome(monthly_data[(agent, mk)], outcome, result_code)

        lc_wk = r["last_call_week_key"]
        if lc_wk and lc_wk in cutoff_weeks:
            tally_outcome(weekly_lc_data[(agent, lc_wk)], outcome, result_code)

        lc_mk = r["last_call_month_key"]
        if lc_mk and lc_mk in cutoff_months:
            tally_outcome(monthly_lc_data[(agent, lc_mk)], outcome, result_code)

        if list_id is not None:
            lb = list_summary_data[list_id]
            lb["total_spoken"] += 1
            if outcome == "Sale":
                lb["sales"] += 1
            elif outcome == "Bad Data":
                lb["bad_data"] += 1
            elif outcome == "Convertible":
                lb["convertible_total"] += 1

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
    now_utc = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    period_end = today.isoformat()
    period_start_13w = (today - datetime.timedelta(weeks=13)).isoformat()

    list_summary_rows = []
    for list_id, bucket in sorted(list_summary_data.items()):
        list_summary_rows.append({
            "list_id": list_id,
            "total_spoken": bucket["total_spoken"],
            "sales": bucket["sales"],
            "bad_data": bucket["bad_data"],
            "convertible_total": bucket["convertible_total"],
        })

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


def fetch_penetration_records(pat):
    """Fetch ALL records (no outcome filter) - now carries talk-time fields too."""
    records = []
    offset = None
    page_num = 0

    fields_param = "&".join("fields[]=" + urllib.parse.quote(f) for f in PENETRATION_FIELDS)
    base_url = (
        "https://api.airtable.com/v0/"
        + AIRTABLE_BASE_ID
        + "/"
        + urllib.parse.quote(AIRTABLE_TABLE_NAME)
        + "?pageSize=100&"
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
        if page_num % 20 == 1:
            log("[Penetration] Fetching page " + str(page_num) + " (records so far: " + str(len(records)) + ")")

        req = urllib.request.Request(url, headers=headers)

        try:
            with urllib.request.urlopen(req) as response:
                data = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            error_body = e.read().decode("utf-8")
            log("[Penetration] HTTP error " + str(e.code) + ": " + error_body)
            raise

        page_records = data.get("records", [])
        records.extend(page_records)

        offset = data.get("offset")
        if not offset:
            log("[Penetration] Fetch complete. Pages: " + str(page_num) + ", total records: " + str(len(records)))
            break

        time.sleep(0.25)

    return records


def aggregate_penetration(raw_records, cutoff_weeks, cutoff_months):
    """Aggregate penetration data: total leads per list, with weekly/monthly breakdown."""
    list_totals = defaultdict(lambda: {"total_leads": 0, "spoken_to": 0, "not_spoken": 0, "na": 0})
    list_weekly = defaultdict(lambda: {"total_leads": 0, "spoken_to": 0, "not_spoken": 0, "na": 0})
    list_monthly = defaultdict(lambda: {"total_leads": 0, "spoken_to": 0, "not_spoken": 0, "na": 0})

    for record in raw_records:
        fields = record.get("fields", {})
        list_id = fields.get("Original List ID")
        list_id_str = str(list_id) if list_id else "Unknown"
        outcome = (fields.get("Result Outcome") or "").strip()

        is_spoken = outcome in SPOKEN_OUTCOMES
        is_not_spoken = outcome == "Not Spoken"
        is_na = outcome == "N/A"

        lt = list_totals[list_id_str]
        lt["total_leads"] += 1
        if is_spoken:
            lt["spoken_to"] += 1
        elif is_not_spoken:
            lt["not_spoken"] += 1
        elif is_na:
            lt["na"] += 1

        import_date_str = (fields.get("import_date") or "").strip()
        if not import_date_str:
            continue
        d = parse_date_utc(import_date_str)
        if d is None:
            continue

        week_key = get_monday_of_week(d).isoformat()
        month_key = datetime.date(d.year, d.month, 1).isoformat()

        if week_key in cutoff_weeks:
            wk = list_weekly[(list_id_str, week_key)]
            wk["total_leads"] += 1
            if is_spoken:
                wk["spoken_to"] += 1
            elif is_not_spoken:
                wk["not_spoken"] += 1
            elif is_na:
                wk["na"] += 1

        if month_key in cutoff_months:
            mk = list_monthly[(list_id_str, month_key)]
            mk["total_leads"] += 1
            if is_spoken:
                mk["spoken_to"] += 1
            elif is_not_spoken:
                mk["not_spoken"] += 1
            elif is_na:
                mk["na"] += 1

    pen_totals = []
    for lid, counts in sorted(list_totals.items(), key=lambda x: -x[1]["total_leads"]):
        pen_totals.append({"list_id": lid, **counts})

    pen_weekly = []
    for (lid, wk), counts in sorted(list_weekly.items(), key=lambda x: (x[0][1], x[0][0])):
        pen_weekly.append({"list_id": lid, "period_start": wk, **counts})

    pen_monthly = []
    for (lid, mk), counts in sorted(list_monthly.items(), key=lambda x: (x[0][1], x[0][0])):
        pen_monthly.append({"list_id": lid, "period_start": mk, **counts})

    return pen_totals, pen_weekly, pen_monthly


# --------------------------------------------------------------------
# TALK TIME DIAGNOSTICS (v2.3)
# --------------------------------------------------------------------

def _attempts_bin(attempts):
    """Bucket Lead Total Attempts into display bins.
    v2.5 extends the range: 1..10 explicit, then 10+ for the long tail.
    """
    if attempts is None:
        return None
    if attempts <= 1:
        return "1"
    if attempts >= 11:
        return "10+"
    return str(attempts)


def _talk_bin(t):
    """v2.5 bins: 0-1m / 1-3m / 3-5m / 5-10m / 10m+ (in seconds)."""
    if t < 60:
        return "0-1m"
    if t < 180:
        return "1-3m"
    if t < 300:
        return "3-5m"
    if t < 600:
        return "5-10m"
    return "10m+"


def _conversion_bucket(talk_seconds):
    """Bucket a talk time (seconds) into a minute-integer bucket for the
    conversion-by-length chart. Returns None if the call is below the
    minimum (5 minutes). Buckets 5..14 map to their integer minute value;
    anything at or above 15 minutes maps to '15+'.
    """
    if talk_seconds is None or talk_seconds < CONVERSION_MIN_MINUTES * 60:
        return None
    minutes = int(talk_seconds // 60)
    if minutes >= CONVERSION_MAX_MINUTES:
        return "15+"
    return str(minutes)


def _mask_phone(phone_str):
    """Return the first 6 numeric digits of a phone number followed by asterisks
    for the remainder. Anything non-numeric is stripped before masking.
    Never emits fewer than 6 digits.
    """
    if not phone_str:
        return ""
    digits = "".join(c for c in str(phone_str) if c.isdigit())
    if not digits:
        return ""
    if len(digits) <= 6:
        return digits
    return digits[:6] + "*" * (len(digits) - 6)


def _round(x, n=1):
    if x is None:
        return None
    try:
        return round(float(x), n)
    except (ValueError, TypeError):
        return None


def aggregate_talk_time(raw_records, cutoff_weeks):
    """
    Compute talk-time diagnostics from the wide penetration-pass records.

    Emits (as a dict) the following keys, all designed for direct consumption
    by the Talk Time QA tab in index.html:
      talk_time_by_agent_code
      talk_time_by_agent_outcome
      talk_time_by_attempts_outcome
      outcome_by_attempt
      talk_time_weekly_by_code
      hangup_over_60s
      agam_over_60s
      skewed_combos
      conversion_by_talk_length
      suspicious_tile_counts
      talk_time_meta
    """
    # (agent, code) -> list of records: {t, date, rid}
    combo_records = defaultdict(list)
    # (agent, outcome) -> {"talks": [t...], "bins": {bin: count}}
    outcome_agg = defaultdict(lambda: {"talks": [], "bins": defaultdict(int)})
    # (agent, attempts_bin, outcome) -> [t]
    attempts_agg = defaultdict(list)
    # (attempts_bin, outcome) -> count (team-level companion)
    attempts_outcome_count = defaultdict(int)
    # (agent, week_key, code) -> [t]
    weekly_code_agg = defaultdict(list)
    # code -> [t] (team baseline)
    team_code_talks = defaultdict(list)
    # (agent, minute_bucket) -> {"total": N, "sales": M} — for conversion_by_talk_length
    conv_by_length = defaultdict(lambda: {"total": 0, "sales": 0})
    # (minute_bucket) -> {"total": N, "sales": M} — team total for the same
    conv_by_length_team = defaultdict(lambda: {"total": 0, "sales": 0})
    # (agent, week_start_iso) -> [t] — Sale-outcome talks only, for Weekly Sale Trend chart 1
    sale_weekly_agg = defaultdict(list)
    # (agent, day_of_week_iso 1..7) -> [t] — all-outcome talks in last completed Mon-Sun week
    daily_last_week_agg = defaultdict(list)

    # Compute the last fully completed Mon-Sun week (relative to the pipeline run's date)
    today = datetime.date.today()
    days_to_current_monday = today.weekday()  # Mon=0, Sun=6
    current_monday = today - datetime.timedelta(days=days_to_current_monday)
    last_week_monday = current_monday - datetime.timedelta(days=7)
    last_week_sunday = last_week_monday + datetime.timedelta(days=6)

    hangup_over_60s = []
    agam_over_60s = []
    parsed_count = 0

    # ---------------------------------------------------------------
    # PASS 1: count records per agent to determine the qualifying set.
    # Non-qualifying agents (below MIN_CALLS_PER_AGENT) are excluded
    # from every talk-time output so dialer attribution noise (single
    # calls attributed to non-agents / ex-agents / test users) does
    # not clutter the analysis.
    # ---------------------------------------------------------------
    _pre_agent_count = defaultdict(int)
    for record in raw_records:
        fields = record.get("fields", {})
        first_name = (fields.get("Agent First Name") or "").strip()
        last_name = (fields.get("Agent Last Name") or "").strip()
        if not first_name and not last_name:
            continue
        talk_raw = fields.get("Talk Time")
        try:
            talk = float(talk_raw) if talk_raw is not None else None
        except (ValueError, TypeError):
            talk = None
        if talk is None or talk <= 0:
            continue
        _pre_agent_count[(first_name + " " + last_name).strip()] += 1

    qualifying_agents = {a for a, n in _pre_agent_count.items() if n >= MIN_CALLS_PER_AGENT}
    excluded_agent_names = sorted(a for a, n in _pre_agent_count.items() if n < MIN_CALLS_PER_AGENT)

    for record in raw_records:
        record_id = record.get("id") or ""
        fields = record.get("fields", {})

        first_name = (fields.get("Agent First Name") or "").strip()
        last_name = (fields.get("Agent Last Name") or "").strip()
        if not first_name and not last_name:
            continue
        agent = (first_name + " " + last_name).strip()
        if agent not in qualifying_agents:
            continue

        # Talk Time is a duration in seconds (Airtable Duration field).
        # Skip records with no talk time so we don't skew averages toward 0.
        talk_raw = fields.get("Talk Time")
        try:
            talk = float(talk_raw) if talk_raw is not None else None
        except (ValueError, TypeError):
            talk = None
        if talk is None or talk <= 0:
            continue

        code = (fields.get("Result Code") or "").strip().upper()
        outcome = (fields.get("Result Outcome") or "").strip()
        if not code or not outcome:
            continue

        attempts_raw = fields.get("Lead Total Attempts")
        try:
            attempts = int(attempts_raw) if attempts_raw is not None else None
        except (ValueError, TypeError):
            attempts = None
        att_bin = _attempts_bin(attempts)

        last_call_str = (fields.get("last_call_date") or "").strip()[:10]
        last_call_date = parse_date_utc(last_call_str) if last_call_str else None
        last_call_week_key = get_monday_of_week(last_call_date).isoformat() if last_call_date else None
        phone_masked = _mask_phone(fields.get("Phone Num", ""))

        parsed_count += 1

        # --- combo (agent, code) ---
        combo_records[(agent, code)].append({
            "t": talk,
            "date": last_call_str,
            "rid": record_id,
            "phone": phone_masked,
        })
        team_code_talks[code].append(talk)

        # --- (agent, outcome) ---
        oa = outcome_agg[(agent, outcome)]
        oa["talks"].append(talk)
        oa["bins"][_talk_bin(talk)] += 1

        # --- (agent, attempts_bin, outcome) ---
        if att_bin is not None:
            attempts_agg[(agent, att_bin, outcome)].append(talk)
            attempts_outcome_count[(att_bin, outcome)] += 1

        # --- weekly by code ---
        if last_call_week_key and last_call_week_key in cutoff_weeks:
            weekly_code_agg[(agent, last_call_week_key, code)].append(talk)

        # --- weekly Sale-outcome only (feeds Chart 1 of Weekly Sale Trend) ---
        if outcome == "Sale" and last_call_week_key and last_call_week_key in cutoff_weeks:
            sale_weekly_agg[(agent, last_call_week_key)].append(talk)

        # --- daily last completed week (feeds Chart 2 of Weekly Sale Trend) ---
        if last_call_date is not None and last_week_monday <= last_call_date <= last_week_sunday:
            # weekday() returns 0-6 (Mon=0). Convert to ISO 1-7 (Mon=1..Sun=7)
            day_iso = last_call_date.weekday() + 1
            daily_last_week_agg[(agent, day_iso)].append(talk)

        # --- conversion by talk length (min bucket) ---
        conv_bucket = _conversion_bucket(talk)
        if conv_bucket is not None:
            conv_by_length[(agent, conv_bucket)]["total"] += 1
            conv_by_length_team[conv_bucket]["total"] += 1
            if outcome == "Sale":
                conv_by_length[(agent, conv_bucket)]["sales"] += 1
                conv_by_length_team[conv_bucket]["sales"] += 1

        # --- HANGUP > 60s flag ---
        if code == "HANGUP" and talk > HANGUP_TALK_TIME_THRESHOLD:
            hangup_over_60s.append({
                "date": last_call_str,
                "agent": agent,
                "code": code,
                "outcome": outcome,
                "talk": int(talk),
                "record_id": record_id,
                "phone_masked": phone_masked,
            })

        # --- AGAM > 60s flag ---
        if code == "AGAM" and talk > AGAM_TALK_TIME_THRESHOLD:
            agam_over_60s.append({
                "date": last_call_str,
                "agent": agent,
                "code": code,
                "outcome": outcome,
                "talk": int(talk),
                "record_id": record_id,
                "phone_masked": phone_masked,
            })

    # --- Build talk_time_by_agent_code rows ---
    by_agent_code_rows = []
    for (agent, code), recs in sorted(combo_records.items(), key=lambda x: (x[0][0], x[0][1])):
        talks = [r["t"] for r in recs]
        by_agent_code_rows.append({
            "agent": agent,
            "code": code,
            "count": len(talks),
            "avg_talk": _round(sum(talks) / len(talks), 1),
            "median_talk": _round(median(talks), 1),
        })

    # --- Team baseline per code ---
    team_code_baseline = {}
    for code, talks in team_code_talks.items():
        if len(talks) >= SKEW_MIN_TEAM_CALLS:
            team_code_baseline[code] = {
                "avg": _round(sum(talks) / len(talks), 1),
                "median": _round(median(talks), 1),
                "count": len(talks),
            }
        else:
            team_code_baseline[code] = {
                "avg": _round(sum(talks) / len(talks), 1) if talks else None,
                "median": _round(median(talks), 1) if talks else None,
                "count": len(talks),
            }

    # --- Skewed combos ---
    # A combo is flagged if the agent's avg deviates from the team avg for that
    # code by more than SKEW_THRESHOLD_PCT AND the agent has SKEW_MIN_CALLS+.
    skewed_combos = []
    for (agent, code), recs in combo_records.items():
        n = len(recs)
        if n < SKEW_MIN_CALLS:
            continue
        team = team_code_baseline.get(code)
        if not team or team["count"] < SKEW_MIN_TEAM_CALLS:
            continue
        team_avg = team["avg"]
        if team_avg is None or team_avg < SKEW_MIN_TEAM_AVG:
            continue
        agent_avg = sum(r["t"] for r in recs) / n
        dev_pct = ((agent_avg - team_avg) / team_avg) * 100.0
        if abs(dev_pct) < SKEW_THRESHOLD_PCT:
            continue
        # Sample records (up to cap), sorted by date desc then talk desc
        sorted_recs = sorted(recs, key=lambda r: (r["date"] or "", r["t"]), reverse=True)
        sample = [{
            "date": r["date"],
            "talk": int(r["t"]),
            "record_id": r["rid"],
            "phone_masked": r.get("phone", ""),
        } for r in sorted_recs[:SUSPICIOUS_SAMPLE_CAP]]
        skewed_combos.append({
            "agent": agent,
            "code": code,
            "count": n,
            "agent_avg": _round(agent_avg, 1),
            "team_avg": team_avg,
            "deviation_pct": _round(dev_pct, 1),
            "direction": "high" if dev_pct > 0 else "low",
            "sample_records": sample,
            "sample_capped": n > SUSPICIOUS_SAMPLE_CAP,
        })
    # Sort by absolute deviation desc
    skewed_combos.sort(key=lambda x: -abs(x["deviation_pct"]))

    # --- talk_time_by_agent_outcome ---
    by_agent_outcome_rows = []
    for (agent, outcome), agg in sorted(outcome_agg.items(), key=lambda x: (x[0][0], x[0][1])):
        talks = agg["talks"]
        bins = agg["bins"]
        by_agent_outcome_rows.append({
            "agent": agent,
            "outcome": outcome,
            "count": len(talks),
            "avg_talk": _round(sum(talks) / len(talks), 1),
            "median_talk": _round(median(talks), 1),
            "bin_0_1m": bins.get("0-1m", 0),
            "bin_1_3m": bins.get("1-3m", 0),
            "bin_3_5m": bins.get("3-5m", 0),
            "bin_5_10m": bins.get("5-10m", 0),
            "bin_10m_plus": bins.get("10m+", 0),
        })

    # --- talk_time_by_attempts_outcome (per agent) ---
    by_attempts_outcome_rows = []
    for (agent, att_bin, outcome), talks in sorted(attempts_agg.items()):
        by_attempts_outcome_rows.append({
            "agent": agent,
            "attempts_bin": att_bin,
            "outcome": outcome,
            "count": len(talks),
            "avg_talk": _round(sum(talks) / len(talks), 1),
        })

    # --- outcome_by_attempt (team level) ---
    outcome_by_attempt_rows = []
    for (att_bin, outcome), n in sorted(attempts_outcome_count.items()):
        outcome_by_attempt_rows.append({
            "attempts_bin": att_bin,
            "outcome": outcome,
            "count": n,
        })

    # --- talk_time_weekly_by_code ---
    weekly_code_rows = []
    for (agent, wk, code), talks in sorted(weekly_code_agg.items()):
        weekly_code_rows.append({
            "agent": agent,
            "week": wk,
            "code": code,
            "count": len(talks),
            "avg_talk": _round(sum(talks) / len(talks), 1),
        })

    # --- talk_time_weekly_sale_by_agent (Chart 1: Sale-outcome talk time by week) ---
    sale_weekly_rows = []
    for (agent, wk), talks in sorted(sale_weekly_agg.items()):
        sale_weekly_rows.append({
            "agent": agent,
            "week": wk,
            "count": len(talks),
            "avg_talk": _round(sum(talks) / len(talks), 1),
        })

    # --- talk_time_last_week_daily (Chart 2: all-outcome talk by day for last completed week) ---
    daily_last_week_rows = []
    for (agent, day_iso), talks in sorted(daily_last_week_agg.items()):
        daily_last_week_rows.append({
            "agent": agent,
            "day_iso": day_iso,
            "count": len(talks),
            "avg_talk": _round(sum(talks) / len(talks), 1),
        })

    # --- HANGUP >60s and AGAM >60s: sort by date desc for default view ---
    hangup_over_60s.sort(key=lambda x: (x["date"] or "", x["agent"]), reverse=True)
    agam_over_60s.sort(key=lambda x: (x["date"] or "", x["agent"]), reverse=True)

    # --- Suspicious tile counts: per (agent, rule) counts for header tiles ---
    tile_counts = defaultdict(lambda: {"hangup": 0, "agam": 0, "skew": 0})
    for r in hangup_over_60s:
        tile_counts[r["agent"]]["hangup"] += 1
    for r in agam_over_60s:
        tile_counts[r["agent"]]["agam"] += 1
    for c in skewed_combos:
        tile_counts[c["agent"]]["skew"] += 1
    tile_rows = []
    for agent, cnts in tile_counts.items():
        tile_rows.append({
            "agent": agent,
            "hangup": cnts["hangup"],
            "agam": cnts["agam"],
            "skew": cnts["skew"],
            "total": cnts["hangup"] + cnts["agam"] + cnts["skew"],
        })
    # Sort by total desc, then agent name
    tile_rows.sort(key=lambda x: (-x["total"], x["agent"]))

    # --- Conversion by talk length ---
    conv_rows = []
    for (agent, bucket), c in sorted(conv_by_length.items()):
        total = c["total"]
        sales = c["sales"]
        conv_rows.append({
            "agent": agent,
            "minute": bucket,
            "total": total,
            "sales": sales,
            "sale_pct": _round(100.0 * sales / total, 1) if total else None,
        })
    conv_team_rows = []
    for bucket, c in sorted(conv_by_length_team.items(), key=lambda x: (0 if x[0] != "15+" else 1, x[0])):
        total = c["total"]
        sales = c["sales"]
        conv_team_rows.append({
            "minute": bucket,
            "total": total,
            "sales": sales,
            "sale_pct": _round(100.0 * sales / total, 1) if total else None,
        })

    # --- Team baselines (for client-side deviation shading) ---
    team_baseline_rows = []
    for code, stats in sorted(team_code_baseline.items()):
        team_baseline_rows.append({
            "code": code,
            "count": stats["count"],
            "avg_talk": stats["avg"],
            "median_talk": stats["median"],
        })

    return {
        "talk_time_by_agent_code": by_agent_code_rows,
        "talk_time_team_code_baseline": team_baseline_rows,
        "talk_time_by_agent_outcome": by_agent_outcome_rows,
        "talk_time_by_attempts_outcome": by_attempts_outcome_rows,
        "outcome_by_attempt": outcome_by_attempt_rows,
        "talk_time_weekly_by_code": weekly_code_rows,
        "hangup_over_60s": hangup_over_60s,
        "agam_over_60s": agam_over_60s,
        "skewed_combos": skewed_combos,
        "talk_time_weekly_sale_by_agent": sale_weekly_rows,
        "talk_time_last_week_daily": daily_last_week_rows,
        "suspicious_tile_counts": tile_rows,
        "conversion_by_talk_length": conv_rows,
        "conversion_by_talk_length_team": conv_team_rows,
        "talk_time_meta": {
            "records_with_talk_time": parsed_count,
            "hangup_threshold_seconds": HANGUP_TALK_TIME_THRESHOLD,
            "agam_threshold_seconds": AGAM_TALK_TIME_THRESHOLD,
            "skew_threshold_pct": SKEW_THRESHOLD_PCT,
            "skew_min_calls": SKEW_MIN_CALLS,
            "skew_min_team_calls": SKEW_MIN_TEAM_CALLS,
            "sample_cap": SUSPICIOUS_SAMPLE_CAP,
            "min_calls_per_agent": MIN_CALLS_PER_AGENT,
            "conversion_min_minutes": CONVERSION_MIN_MINUTES,
            "conversion_max_minutes": CONVERSION_MAX_MINUTES,
            "qualifying_agents": len(qualifying_agents),
            "excluded_agent_names": excluded_agent_names,
            "airtable_base_id": AIRTABLE_BASE_ID,
            "airtable_table_id": AIRTABLE_TABLE_ID,
            "last_completed_week_start": last_week_monday.isoformat(),
            "last_completed_week_end": last_week_sunday.isoformat(),
        },
    }


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
    log("Starting data fetch (v2.3). Today: " + today.isoformat())

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

    log("Aggregating spoken-to data...")
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

    log("Fetching ALL records for penetration + talk-time data...")
    pen_records = fetch_penetration_records(pat)
    log("Aggregating penetration data...")
    pen_totals, pen_weekly, pen_monthly = aggregate_penetration(pen_records, cutoff_weeks, cutoff_months)

    output["list_penetration"] = pen_totals
    output["penetration_weekly"] = pen_weekly
    output["penetration_monthly"] = pen_monthly

    log("Aggregating talk-time diagnostics...")
    talk_time_output = aggregate_talk_time(pen_records, cutoff_weeks)
    output.update(talk_time_output)

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
        + " penetration=" + str(len(pen_totals))
        + " pen_weekly=" + str(len(pen_weekly))
        + " pen_monthly=" + str(len(pen_monthly))
        + " by_agent_code=" + str(len(output["talk_time_by_agent_code"]))
        + " by_agent_outcome=" + str(len(output["talk_time_by_agent_outcome"]))
        + " by_attempts_outcome=" + str(len(output["talk_time_by_attempts_outcome"]))
        + " weekly_by_code=" + str(len(output["talk_time_weekly_by_code"]))
        + " hangup60=" + str(len(output["hangup_over_60s"]))
        + " agam60=" + str(len(output["agam_over_60s"]))
        + " skewed_combos=" + str(len(output["skewed_combos"]))
        + " tiles=" + str(len(output["suspicious_tile_counts"]))
        + " conv_len_rows=" + str(len(output["conversion_by_talk_length"]))
    )


if __name__ == "__main__":
    main()
