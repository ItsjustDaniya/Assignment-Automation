#!/usr/bin/env python3
"""
Assignment Automation Pipeline
Metabase -> pandas -> Google Sheets

FIXES IN THIS VERSION
  1. ConnectionError / ECONNRESET is now retried (was fatal on first hit — this is
     what killed card 9913).
  2. requests.Session with a urllib3 Retry adapter, POST explicitly allowed.
  3. Timeout is a (connect, read) tuple set BELOW the typical proxy read ceiling,
     so we get a clean client timeout instead of a server-side reset.
  4. Auth uses a Metabase API key (x-api-key header) — no session token to
     refresh, so a 401 just fails fast with a clear error instead of retrying
     a login that can't fix an invalid/revoked key.
  5. Every request is timed, so slow cards are visible in the log.
  6. Automatic CSV fallback for heavy cards — starts streaming sooner than JSON.
  7. validate_response now RAISES on empty results and on Metabase's 202 error
     payloads (raise_for_status does not catch those).
  8. AI Mock's two sheet writes are isolated — one failure no longer skips the other.
  9. Freshness assertion warns when the newest row looks stale (cached result).
 10. sys.exit(1) on any task failure, so CI actually goes red.
 11. NEW — dedupe_columns(): fetch_card_df() now normalizes duplicate/blank
     column names the SAME way regardless of whether a card came back via the
     CSV or JSON endpoint. This is what was causing the "redundant columns"
     symptom on card 9913 in the script but not in the notebook: 9913 is a
     HEAVY_CARDS card, so the script tries CSV first (pandas auto-suffixes
     duplicate headers as `date`, `date.1`, `date.2`, ...) while the notebook
     always hit the JSON endpoint directly (json.loads silently collapses
     duplicate keys, so it "looked fine" — it just quietly dropped a column).
     Both paths now go through the same explicit dedupe step, and we log it
     loudly instead of failing silently.
"""

import io
import os
import sys
import json
import time
import traceback
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
import pandas as pd
import gspread
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials

# -------------------- START TIMER --------------------
start_time = time.time()

# -------------------- ENV & AUTH --------------------
METABASE_API_KEY = os.getenv("METABASE_API_KEY")
service_account_json = os.getenv("SERVICE_ACCOUNT_JSON")

missing = [n for n, v in [
    ("METABASE_API_KEY", METABASE_API_KEY),
    ("SERVICE_ACCOUNT_JSON", service_account_json),
] if not v]
if missing:
    raise ValueError(f"❌ Missing environment variables: {', '.join(missing)}")

# -------------------- GOOGLE AUTH --------------------
service_info = json.loads(service_account_json)
creds = Credentials.from_service_account_info(
    service_info,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ],
)
gc = gspread.authorize(creds)

# -------------------- METABASE CONFIG --------------------
METABASE_BASE = "https://metabase-lierhfgoeiwhr.newtonschool.co"

METABASE_HEADERS = {
    "Content-Type": "application/json",
    "x-api-key": METABASE_API_KEY,
}

# (connect_timeout, read_timeout).
# Keep read_timeout UNDER the reverse-proxy ceiling in front of Metabase
# (commonly 300s for nginx/Cloudflare). A client timeout is retryable and
# diagnosable; a proxy-side reset is neither.
MB_TIMEOUT = (15, 280)
MB_MAX_RETRIES = 3          # attempts after the first, per card
MB_RETRY_BASE_WAIT = 20     # seconds; grows linearly per attempt

# Cards known to be heavy — try the lighter CSV endpoint first.
# NOTE: 9913 removed. Its Metabase question returns duplicate column names
# (e.g. 'email' appears 3x, 'user_id' 2x, 'au_batch_name' 2x — confirmed via
# CSV export). pd.read_csv exposes these as 'email.1', 'email.2', etc., which
# is what showed up as "duplicate columns" in the sheet. The JSON endpoint
# doesn't hit this problem: json.loads collapses duplicate keys automatically,
# which is exactly the behavior already confirmed working in the notebook.
# So for 9913 specifically, JSON is not just a fallback, it's the correct path.
HEAVY_CARDS = {7939}

# -------------------- HTTP SESSION WITH RETRIES --------------------
SESSION = requests.Session()
_adapter = HTTPAdapter(
    max_retries=Retry(
        total=3,
        connect=3,
        read=0,                                   # mb_post owns read retries
        backoff_factor=5,                         # 5s, 10s, 20s
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["POST", "GET"]),  # POST is NOT retried by default
        raise_on_status=False,
        respect_retry_after_header=True,
    ),
    pool_connections=10,
    pool_maxsize=10,
)
SESSION.mount("https://", _adapter)
SESSION.mount("http://", _adapter)

# -------------------- ENVIRONMENT DIAGNOSTICS --------------------
try:
    print("🔎 ENV CHECK")
    print(f"   MB API key present: {bool(METABASE_API_KEY)}")
    print(f"   MB base           : {METABASE_BASE}")
    print(f"   SA client_email   : {service_info.get('client_email')}")
    print(f"   Read timeout      : {MB_TIMEOUT[1]}s, retries: {MB_MAX_RETRIES}")
except Exception as e:
    print(f"⚠️ Env check failed: {e}")

# -------------------- SHEET KEYS --------------------
SHEET_PROJECTS_MAIN    = "1e9BxI2N6ms1hh_OdfHgktntxi41_2Y2Jv5Oo-WnQDAo"
SHEET_AI_MOCK_RAW      = "1-nHZ8plywsj2qCjjW5cTVbYhZW81gN5HVfERk4YmvCM"
SHEET_AI               = "1sUCcOrOgvZJc66Oyqjqo_ranftT1QApXoexKGdx3NG8"
SHEET_LECTURE_QUALITY  = "1D-nPNTKoma5mS6B5_gAH5vRWysoGbE6inPghK969n8A"
SHEET_LECTURE_FEEDBACK = "1QKoJUTCM_rz7y0DVh_xEwqV1aGGd9dPHcZaLxBUtpiU"


# -------------------- METABASE FETCH --------------------
def card_url(card_id, fmt="json"):
    """Build the export URL for a Metabase card. fmt is 'json' or 'csv'."""
    return f"{METABASE_BASE}/api/card/{card_id}/query/{fmt}"


RETRYABLE = (
    requests.exceptions.Timeout,
    requests.exceptions.ConnectionError,
    requests.exceptions.ChunkedEncodingError,
)


def mb_post(url, attempt=0, stream=False):
    """
    POST to Metabase with:
      - retries on timeout AND connection reset
      - elapsed-time logging so slow cards are visible

    Auth is a static API key (x-api-key), so unlike session-token auth there
    is nothing to refresh: a 401 means the key itself is missing/invalid/
    revoked, and retrying with the same key would just fail the same way —
    so we raise immediately with a clear message instead of looping.
    """
    label = url.split("/api/card/")[-1] if "/api/card/" in url else url
    t0 = time.time()
    try:
        r = SESSION.post(url, headers=METABASE_HEADERS,
                         timeout=MB_TIMEOUT, stream=stream)

        if r.status_code == 401:
            raise RuntimeError(
                f"Card {label}: Metabase rejected the API key (401). "
                f"Check that METABASE_API_KEY is set and still valid."
            )

        r.raise_for_status()
        print(f"   ⏱️ card {label} responded in {time.time() - t0:.0f}s")
        return r

    except RETRYABLE as e:
        elapsed = time.time() - t0
        if attempt < MB_MAX_RETRIES:
            wait = MB_RETRY_BASE_WAIT * (attempt + 1)
            print(f"   🔁 {type(e).__name__} on card {label} after {elapsed:.0f}s "
                  f"— retry {attempt + 1}/{MB_MAX_RETRIES} in {wait}s")
            time.sleep(wait)
            return mb_post(url, attempt=attempt + 1, stream=stream)

        print(f"   ❌ Card {label} gave up after {attempt + 1} attempts "
              f"({elapsed:.0f}s on the last one): {e}")
        raise

    except requests.exceptions.RequestException as e:
        body = ""
        try:
            body = e.response.text[:300] if e.response is not None else ""
        except Exception:
            pass
        print(f"   ❌ Card {label} failed after {time.time() - t0:.0f}s: {e} {body}")
        raise


def validate_response(response, card_id):
    """
    Parse and validate a Metabase JSON response.
    RAISES on empty results or error payloads — an empty result must never be
    mistaken for 'nothing to do', because that silently leaves a sheet stale.
    """
    try:
        data = response.json()
    except ValueError:
        raise RuntimeError(
            f"Card {card_id}: invalid JSON (HTTP {response.status_code}, "
            f"body starts: {response.text[:200]!r})"
        )

    if isinstance(data, dict):
        err = data.get("error") or data.get("error_type") or data.get("message")
        raise RuntimeError(f"Card {card_id} query error: {err or data}")

    if not data:
        raise RuntimeError(
            f"Card {card_id} returned 0 rows — refusing to overwrite the sheet "
            f"with an empty result."
        )
    return data


def dedupe_columns(df, card_id):
    """
    Drop duplicate columns so both the CSV and JSON fetch paths produce the
    same result for a given card.

    Two cases, both handled here:
      1. Exact duplicate names (e.g. two columns both literally called
         'date') — caught by df.columns.duplicated().
      2. pandas-style auto-suffixed duplicates (e.g. 'email', 'email.1',
         'email.2', 'user_id', 'user_id.1') — this is what pd.read_csv does
         when the source data already had repeated headers. These have
         DIFFERENT names so .duplicated() alone misses them; we strip the
         trailing '.N' suffix to recover the base name and dedupe on that.

    In both cases we keep the FIRST occurrence and drop the rest.
    """
    import re

    def base_name(col):
        m = re.match(r"^(.*)\.\d+$", str(col))
        return m.group(1) if m else str(col)

    before = df.shape[1]
    base_names = [base_name(c) for c in df.columns]
    seen = set()
    keep_cols = []       # original column labels to keep
    keep_base = []       # their base (de-suffixed) names, in order
    dropped_cols = []
    for col, base in zip(df.columns, base_names):
        if base in seen:
            dropped_cols.append(col)
        else:
            seen.add(base)
            keep_cols.append(col)
            keep_base.append(base)

    df = df[keep_cols]
    df.columns = keep_base  # normalize e.g. keep 'email' rather than leaving suffixes

    dropped = before - df.shape[1]
    if dropped:
        print(f"   ⚠️ Card {card_id}: dropped {dropped} duplicate column(s) "
              f"(incl. pandas-suffixed like 'x.1', 'x.2'): {dropped_cols}")
    return df


def fetch_card_df(card_id):
    """
    Fetch a card as a DataFrame.
    Heavy cards try CSV first (streams sooner, far lighter than JSON for wide
    result sets), then fall back to JSON.

    Both the CSV and JSON paths now run through dedupe_columns() before
    returning, so callers see consistent column names no matter which
    endpoint actually served the data.
    """
    if card_id in HEAVY_CARDS:
        try:
            print(f"   📥 Card {card_id}: trying CSV endpoint (heavy card)")
            r = mb_post(card_url(card_id, "csv"), stream=True)
            df = pd.read_csv(io.StringIO(r.text))
            if df.empty:
                raise RuntimeError(f"Card {card_id} CSV returned 0 rows")
            df = dedupe_columns(df, card_id)
            print(f"   ✅ Card {card_id}: {len(df)} rows via CSV")
            return df
        except Exception as e:
            print(f"   ⚠️ CSV path failed for card {card_id} ({e}); trying JSON")

    r = mb_post(card_url(card_id, "json"))
    data = validate_response(r, card_id)
    df = pd.DataFrame(data)
    df = dedupe_columns(df, card_id)
    print(f"   ✅ Card {card_id}: {len(df)} rows via JSON")
    return df


def check_freshness(df, card_id, date_cols=("date", "lecture_date", "release_date"),
                    max_age_days=2):
    """Warn if the newest row looks stale — the signature of a cached result."""
    col = next((c for c in date_cols if c in df.columns), None)
    if not col:
        return
    latest = pd.to_datetime(df[col], errors="coerce").max()
    if pd.isna(latest):
        return
    age = (pd.Timestamp.now() - latest.tz_localize(None)
           if latest.tzinfo else pd.Timestamp.now() - latest).days
    print(f"   🕒 Card {card_id}: newest '{col}' = {latest} ({age}d old)")
    if age > max_age_days:
        print(f"   🚨 WARNING: card {card_id} data is {age} days old — "
              f"possible cached Metabase result or an upstream data gap.")


# -------------------- GOOGLE SHEETS WRITE --------------------
def write_sheet(sheet_key, worksheet_name, df):
    """Write a DataFrame to a Google Sheet tab, with rate-limit backoff."""
    if df is None or df.empty:
        raise RuntimeError(f"Refusing to write an empty DataFrame to '{worksheet_name}'")

    print(f"🔄 Updating sheet: {worksheet_name} ({len(df)} rows)")

    for attempt in range(1, 6):
        try:
            time.sleep(2)  # Rate-limit protection
            sheet = gc.open_by_key(sheet_key)
            print(f"📄 Writing to spreadsheet: '{sheet.title}' → tab: '{worksheet_name}'")
            ws = sheet.worksheet(worksheet_name)
            ws.clear()
            set_with_dataframe(ws, df, include_index=False, include_column_header=True)
            print(f"✅ Successfully updated: {worksheet_name}")
            return
        except gspread.exceptions.WorksheetNotFound:
            available = [w.title for w in gc.open_by_key(sheet_key).worksheets()]
            print(f"❌ Worksheet '{worksheet_name}' not found. Available tabs: {available}")
            raise
        except gspread.exceptions.APIError as e:
            if "RESOURCE_EXHAUSTED" in str(e) or "Quota exceeded" in str(e):
                wait_time = 60 * attempt
                print(f"⏳ Rate limit hit, waiting {wait_time}s...")
                time.sleep(wait_time)
            else:
                print(f"[Sheets] Attempt {attempt} failed for {worksheet_name}: {e}")
                if attempt < 5:
                    time.sleep(20)
                else:
                    raise
        except Exception as e:
            print(f"[Sheets] Attempt {attempt} failed for {worksheet_name}: {e}")
            if attempt < 5:
                time.sleep(20)
            else:
                raise


def write_many(df, targets):
    """
    Write the same DataFrame to several sheet/tab targets.
    Each write is isolated so one failure does not skip the rest.
    """
    errors = []
    for sheet_key, tab in targets:
        try:
            write_sheet(sheet_key, tab, df)
        except Exception as e:
            print(f"   ❌ Write failed for tab '{tab}': {e}")
            errors.append(f"{tab}: {e}")
    if errors:
        raise RuntimeError("Sheet write failures → " + " | ".join(errors))


# -------------------- SECTION 1: AI MOCK LECTURES --------------------
def run_ai_mock_lectures():
    print("\n📌 Running: AI Mock Lectures")
    df_ai_mock = fetch_card_df(9913)
    print(f"📊 shape={df_ai_mock.shape} | cols={list(df_ai_mock.columns)[:8]}")
    check_freshness(df_ai_mock, 9913)

    # If Metabase returns UTC and consumers read the sheet as IST, rows after
    # 18:30 IST land on the previous day. Re-enable only if the card is not
    # already returning IST.
    # if "date" in df_ai_mock.columns:
    #     df_ai_mock["date"] = pd.to_datetime(df_ai_mock["date"]) + pd.Timedelta(hours=5.5)

    write_many(df_ai_mock, [
        (SHEET_AI_MOCK_RAW, "Raw"),
        (SHEET_AI, "AI"),
    ])


# -------------------- SECTION 2: LECTURE QUALITY --------------------
def run_lecture_quality():
    print("\n📌 Running: Lecture Quality")

    df1 = fetch_card_df(8646)
    df2 = fetch_card_df(9192)

    keys = ["lecture_id", "lecture_date"]

    dup_left = df1.duplicated(subset=keys).sum()
    dup_right = df2.duplicated(subset=keys).sum()
    if dup_left or dup_right:
        print(f"   ℹ️ duplicate merge keys — 8646: {dup_left}, 9192: {dup_right}")

    df3 = pd.merge(df1, df2, on=keys, how="inner")
    print(f"   🔗 merge 8646×9192: left={len(df1)}, right={len(df2)} → {len(df3)} rows")

    if len(df3) > len(df1):
        print(f"   🚨 FANOUT: result ({len(df3)}) exceeds left table ({len(df1)}) — "
              f"duplicate keys in 9192 are multiplying rows. Verify the card's grain.")

    df4 = df3.rename(columns={"course_name": "Batch", "lecture_date": "date"})

    try:
        df_in_class = fetch_card_df(9166).rename(
            columns={"batch_name": "Batch", "release_date": "date"}
        )
        before = len(df4)
        df5 = pd.merge(df4, df_in_class, on=["Batch", "date"], how="left")
        if len(df5) != before:
            print(f"   🚨 FANOUT on 9166 merge: {before} → {len(df5)} rows — "
                  f"duplicate (Batch, date) in card 9166.")
    except Exception as e:
        print(f"   ⚠️ Card 9166 unavailable ({e}) — writing without in-class merge")
        df5 = df4

    print(f"📊 Final Lecture_Quality shape={df5.shape}")
    write_sheet(SHEET_LECTURE_QUALITY, "Lecture_Quality", df5)


# -------------------- SECTION 3: LECTURE SUBJECTIVE FEEDBACK --------------------
def run_lecture_subjective_feedback():
    print("\n📌 Running: Lecture Subjective Feedback")
    df = fetch_card_df(5037)
    print(f"📊 shape={df.shape}")
    write_sheet(SHEET_LECTURE_FEEDBACK, "Lecture_Subjective_Feedback", df)


# -------------------- SECTION 4: ASSIGNMENT QUESTIONS BUCKET --------------------
def run_assignment_questions_bucket():
    print("\n📌 Running: Assignment Questions Bucket")
    df = fetch_card_df(7939)
    print(f"📊 shape={df.shape} | cols={list(df.columns)[:8]}")

    df = df.fillna(0)
    df = df.rename(columns={"batch_name": "Batch", "module_name": "Module"})
    write_sheet(SHEET_PROJECTS_MAIN, "Assignments_questions_bucket", df)


# -------------------- MAIN --------------------
def main():
    print("🚀 Starting Data Pipeline Automation...")
    print(f"📅 Start time: "
          f"{datetime.now(ZoneInfo('Asia/Kolkata')).strftime('%d-%b-%Y %H:%M:%S IST')}\n")

    tasks = [
        ("AI Mock Lectures",            run_ai_mock_lectures),
        ("Lecture Quality",             run_lecture_quality),
        ("Lecture Subjective Feedback", run_lecture_subjective_feedback),
        ("Assignment Questions Bucket", run_assignment_questions_bucket),
    ]

    success_count = 0
    failed_tasks = []
    timings = {}

    for name, fn in tasks:
        t0 = time.time()
        try:
            fn()
            success_count += 1
        except Exception as e:
            print(f"❌ Error in {name}: {e}")
            traceback.print_exc()
            failed_tasks.append(f"{name} ({type(e).__name__})")
        finally:
            timings[name] = time.time() - t0

    print("\n" + "=" * 60)
    print("📊 EXECUTION SUMMARY")
    print("=" * 60)
    print(f"✅ Successful: {success_count}/{len(tasks)}")
    for name, secs in timings.items():
        mark = "❌" if any(name in f for f in failed_tasks) else "✅"
        print(f"   {mark} {name:<32} {secs:6.0f}s")
    if failed_tasks:
        print(f"\n❌ Failed: {len(failed_tasks)} → {', '.join(failed_tasks)}")

    print(f"\n📅 End time: "
          f"{datetime.now(ZoneInfo('Asia/Kolkata')).strftime('%d-%b-%Y %H:%M:%S IST')}")
    mins, secs = divmod(time.time() - start_time, 60)
    print(f"⏱️  Total execution time: {int(mins)}m {int(secs)}s")

    if failed_tasks:
        print("\n⚠️  Pipeline completed with errors — exiting non-zero so CI fails.")
        return 1

    print("\n🎯 Data Pipeline Automation completed successfully!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
