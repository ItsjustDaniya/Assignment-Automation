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

# -------------------- SHEET KEYS --------------------

SHEET_PROJECTS_MAIN = '1e9BxI2N6ms1hh_OdfHgktntxi41_2Y2Jv5Oo-WnQDAo'



# -------------------- UTILITIES --------------------
def mb_post(card_url, retry_count=0):
    """
    Make POST request to Metabase with automatic token refresh on auth failure
    """
    try:
        r = requests.post(card_url, headers=METABASE_HEADERS, timeout=120)

        # If unauthorized and haven't retried yet, refresh token and retry
        if r.status_code == 401 and retry_count == 0:
            print("⚠️ Metabase token expired, refreshing...")
            refresh_metabase_token()
            return mb_post(card_url, retry_count=1)

        r.raise_for_status()
        return r
    except requests.exceptions.Timeout:
        print(f"⏱️ Timeout for {card_url}")
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



# -------------------- SECTION 4: ASSIGNMENT QUESTIONS BUCKET --------------------
def run_assignment_questions_bucket():
    print("\n📌 Running: Assignment Questions Bucket")

    try:
        r = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/7939/query/json')
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

        ("Assignment Questions Bucket", run_assignment_questions_bucket)

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
