import os
import time
import json
import calendar
import requests
import pandas as pd
import gspread
import re
from datetime import datetime
from zoneinfo import ZoneInfo
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
import traceback

# -------------------- START TIMER --------------------
start_time = time.time()

# -------------------- ENV & AUTH --------------------
sec = os.getenv("ASHRITHA_SECRET_KEY")
User_name = os.getenv("USERNAME")
service_account_json = os.getenv("SERVICE_ACCOUNT_JSON")
MB_URL = os.getenv("METABASE_URL")

if not sec or not service_account_json:
    raise ValueError("❌ Missing environment variables. Check GitHub secrets.")

# -------------------- GOOGLE AUTH --------------------
service_info = json.loads(service_account_json)
creds = Credentials.from_service_account_info(
    service_info,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)
gc = gspread.authorize(creds)

# -------------------- METABASE AUTH --------------------
token = None
METABASE_HEADERS = {}

METABASE_BASE = "https://metabase-lierhfgoeiwhr.newtonschool.co"


def refresh_metabase_token():
    """Refresh Metabase session token"""
    global token, METABASE_HEADERS
    try:
        res = requests.post(
            MB_URL,
            headers={"Content-Type": "application/json"},
            json={"username": User_name, "password": sec},
            timeout=30
        )
        res.raise_for_status()
        token = res.json()['id']
        METABASE_HEADERS = {
            'Content-Type': 'application/json',
            'X-Metabase-Session': token
        }
        print("✅ Metabase session token refreshed")
    except Exception as e:
        print(f"❌ Failed to refresh Metabase token: {e}")
        raise


# Initial authentication
refresh_metabase_token()

# -------------------- ENVIRONMENT DIAGNOSTICS --------------------
# Prints which identities the SCHEDULED run is actually using.
# Compare this output against Colab to catch wrong-account / missing-secret issues.
try:
    print("🔎 ENV CHECK")
    print(f"   MB user (USERNAME): {User_name!r}")
    print(f"   MB secret present : {bool(sec)}")
    print(f"   MB URL            : {MB_URL!r}")
    print(f"   SA client_email   : {service_info.get('client_email')}")
    print(f"   Token acquired    : {bool(token)}")
except Exception as e:
    print(f"⚠️ Env check failed: {e}")

# -------------------- SHEET KEYS --------------------
SHEET_PROJECTS_MAIN      = '1e9BxI2N6ms1hh_OdfHgktntxi41_2Y2Jv5Oo-WnQDAo'
SHEET_AI_MOCK_RAW        = '1-nHZ8plywsj2qCjjW5cTVbYhZW81gN5HVfERk4YmvCM'
SHEET_AI                 = '1sUCcOrOgvZJc66Oyqjqo_ranftT1QApXoexKGdx3NG8'
SHEET_LECTURE_QUALITY    = '1D-nPNTKoma5mS6B5_gAH5vRWysoGbE6inPghK969n8A'
SHEET_LECTURE_FEEDBACK   = '1QKoJUTCM_rz7y0DVh_xEwqV1aGGd9dPHcZaLxBUtpiU'


# -------------------- UTILITIES --------------------
def card_url(card_id):
    """Build the /query/json URL for a Metabase card"""
    return f"{METABASE_BASE}/api/card/{card_id}/query/json"


# How long to wait for a Metabase card to run (seconds), and how many
# times to retry a slow card. Some cards (e.g. 7939, 9913) are heavy and
# can take several minutes — a short timeout kills them before data returns.
MB_TIMEOUT = 360
MB_MAX_TIMEOUT_RETRIES = 2


def mb_post(card_url, retry_count=0, timeout_retries=0):
    """
    Make POST request to Metabase with automatic token refresh on auth failure
    and retries for slow queries that time out.
    """
    try:
        r = requests.post(card_url, headers=METABASE_HEADERS, timeout=MB_TIMEOUT)

        # If unauthorized and haven't retried yet, refresh token and retry
        if r.status_code == 401 and retry_count == 0:
            print("⚠️ Metabase token expired, refreshing...")
            refresh_metabase_token()
            return mb_post(card_url, retry_count=1, timeout_retries=timeout_retries)

        r.raise_for_status()
        return r
    except requests.exceptions.Timeout:
        if timeout_retries < MB_MAX_TIMEOUT_RETRIES:
            attempt = timeout_retries + 1
            print(f"⏱️ Timeout for {card_url} — retry {attempt}/{MB_MAX_TIMEOUT_RETRIES} "
                  f"(a slow first run often warms the cache)")
            time.sleep(10)
            return mb_post(card_url, retry_count=retry_count,
                           timeout_retries=attempt)
        print(f"⏱️ Timeout for {card_url} — giving up after "
              f"{MB_MAX_TIMEOUT_RETRIES} retries")
        raise
    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed for {card_url}: {e}")
        raise


def write_sheet(sheet_key, worksheet_name, df):
    """
    Write DataFrame to Google Sheet with retry logic and rate limit handling
    """
    print(f"🔄 Updating sheet: {worksheet_name} ({len(df)} rows)")

    for attempt in range(1, 6):
        try:
            time.sleep(2)  # Rate limit protection
            sheet = gc.open_by_key(sheet_key)
            print(f"📄 Writing to spreadsheet: '{sheet.title}' → tab: '{worksheet_name}'")
            ws = sheet.worksheet(worksheet_name)
            ws.clear()
            set_with_dataframe(ws, df, include_index=False, include_column_header=True)
            print(f"✅ Successfully updated: {worksheet_name}")
            return
        except gspread.exceptions.WorksheetNotFound:
            available = [w.title for w in gc.open_by_key(sheet_key).worksheets()]
            print(f"❌ Worksheet '{worksheet_name}' not found. "
                  f"Available tabs: {available}")
            raise
        except gspread.exceptions.APIError as e:
            if "RESOURCE_EXHAUSTED" in str(e) or "Quota exceeded" in str(e):
                wait_time = 60 * attempt
                print(f"⏳ Rate limit hit, waiting {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"[Sheets] Attempt {attempt} failed for {worksheet_name}: {e}")
                if attempt < 5:
                    time.sleep(20)
                else:
                    print(f"❌ All attempts failed for {worksheet_name}.")
                    raise
        except Exception as e:
            print(f"[Sheets] Attempt {attempt} failed for {worksheet_name}: {e}")
            if attempt < 5:
                time.sleep(20)
            else:
                print(f"❌ All attempts failed for {worksheet_name}.")
                raise


def validate_response(response, card_id):
    """Validate API response and return JSON data"""
    try:
        data = response.json()
        if not data:
            print(f"⚠️ Warning: Card {card_id} returned empty data")
            return None
        return data
    except ValueError:
        print(f"❌ Invalid JSON response from card {card_id}")
        return None


# -------------------- SECTION 1: AI MOCK LECTURES --------------------
def run_ai_mock_lectures():
    print("\n📌 Running: AI Mock Lectures")

    try:
        r = mb_post(card_url(9913))
        data = validate_response(r, '9913')
        if not data:
            print("⚠️ Card 9913 returned NO data — skipping write.")
            return

        df_ai_mock = pd.DataFrame(data)
        print(f"📊 Fetched {len(df_ai_mock)} records | shape={df_ai_mock.shape} | "
              f"cols={list(df_ai_mock.columns)[:8]}")

        # df_ai_mock['date'] = df_ai_mock['date'] + pd.Timedelta(hours=5.5)  # (kept from original, disabled)

        # Same DataFrame goes to two different sheets/tabs
        write_sheet(SHEET_AI_MOCK_RAW, "Raw", df_ai_mock)
        write_sheet(SHEET_AI, "AI", df_ai_mock)

    except Exception as e:
        print(f"❌ Error in run_ai_mock_lectures: {e}")
        traceback.print_exc()
        raise


# -------------------- SECTION 2: LECTURE QUALITY --------------------
def run_lecture_quality():
    print("\n📌 Running: Lecture Quality")

    try:
        r1 = mb_post(card_url(8646))
        d1 = validate_response(r1, '8646')

        r2 = mb_post(card_url(9192))
        d2 = validate_response(r2, '9192')

        r3 = mb_post(card_url(9166))
        d3 = validate_response(r3, '9166')

        if not d1 or not d2:
            print("⚠️ Missing base data (card 8646 / 9192) — skipping write.")
            return

        df1 = pd.DataFrame(d1)
        df2 = pd.DataFrame(d2)

        df3 = pd.merge(df1, df2, on=['lecture_id', 'lecture_date'], how='inner')
        df4 = df3.rename(columns={'course_name': 'Batch', 'lecture_date': 'date'})

        if d3:
            df_in_class = pd.DataFrame(d3).rename(
                columns={'batch_name': 'Batch', 'release_date': 'date'}
            )
            df5 = pd.merge(df4, df_in_class, on=['Batch', 'date'], how='left')
        else:
            print("⚠️ Card 9166 returned no data — writing lecture quality "
                  "without the in-class merge.")
            df5 = df4

        print(f"📊 Final Lecture_Quality shape={df5.shape}")
        write_sheet(SHEET_LECTURE_QUALITY, "Lecture_Quality", df5)

    except Exception as e:
        print(f"❌ Error in run_lecture_quality: {e}")
        traceback.print_exc()
        raise


# -------------------- SECTION 3: LECTURE SUBJECTIVE FEEDBACK --------------------
def run_lecture_subjective_feedback():
    print("\n📌 Running: Lecture Subjective Feedback")

    try:
        r = mb_post(card_url(5037))
        data = validate_response(r, '5037')
        if not data:
            print("⚠️ Card 5037 returned NO data — skipping write.")
            return

        df = pd.DataFrame(data)
        print(f"📊 Fetched {len(df)} records | shape={df.shape}")
        write_sheet(SHEET_LECTURE_FEEDBACK, "Lecture_Subjective_Feedback", df)

    except Exception as e:
        print(f"❌ Error in run_lecture_subjective_feedback: {e}")
        traceback.print_exc()
        raise


# -------------------- SECTION 4: ASSIGNMENT QUESTIONS BUCKET --------------------
def run_assignment_questions_bucket():
    print("\n📌 Running: Assignment Questions Bucket")

    try:
        r = mb_post(card_url(7939))
        data = validate_response(r, '7939')
        if not data:
            print("⚠️ Card 7939 returned NO data — skipping write. "
                  "Check the card directly in Metabase (it may depend on a "
                  "parameter/filter not supplied by the /query/json call).")
            return

        df = pd.DataFrame(data)
        print(f"📊 Fetched {len(df)} records | shape={df.shape} | "
              f"cols={list(df.columns)[:8]}")

        df = df.fillna(0)
        df = df.rename(columns={'batch_name': 'Batch', 'module_name': 'Module'})
        write_sheet(SHEET_PROJECTS_MAIN, "Assignments_questions_bucket", df)

    except Exception as e:
        print(f"❌ Error in run_assignment_questions_bucket: {e}")
        traceback.print_exc()
        raise


# -------------------- MAIN: RUN ALL --------------------
if __name__ == "__main__":
    print("🚀 Starting Data Pipeline Automation...")
    print(f"📅 Start time: {datetime.now(ZoneInfo('Asia/Kolkata')).strftime('%d-%b-%Y %H:%M:%S IST')}\n")

    tasks = [
        ("AI Mock Lectures",             run_ai_mock_lectures),
        ("Lecture Quality",              run_lecture_quality),
        ("Lecture Subjective Feedback",  run_lecture_subjective_feedback),
        ("Assignment Questions Bucket",  run_assignment_questions_bucket),
    ]

    success_count = 0
    failed_tasks = []

    for name, fn in tasks:
        try:
            fn()
            success_count += 1
        except Exception as e:
            print(f"❌ Error in {name}: {e}")
            failed_tasks.append(name)

    # Summary
    print("\n" + "=" * 60)
    print("📊 EXECUTION SUMMARY")
    print("=" * 60)
    print(f"✅ Successful: {success_count}/{len(tasks)}")
    if failed_tasks:
        print(f"❌ Failed: {len(failed_tasks)}")
        print(f"   Tasks: {', '.join(failed_tasks)}")

    current_time = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%d-%b-%Y %H:%M:%S IST")
    print(f"\n📅 End time: {current_time}")

    end_time = time.time()
    mins, secs = divmod(end_time - start_time, 60)
    print(f"⏱️  Total execution time: {int(mins)}m {int(secs)}s")

    if failed_tasks:
        print("\n⚠️  Pipeline completed with errors")
    else:
        print("\n🎯 Data Pipeline Automation completed successfully!")
