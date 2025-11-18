#!/usr/bin/env python3
"""
activity_tracker_phase1.py
Phase 1:
 - Move DueDate/ReminderDate/Status logic into Python
 - Business-day aware reminders (skip Sat/Sun)
 - Monthly rollover on the 1st
 - Reminder emails (yagmail SMTP)
 - Single overdue reminder (sent once) after a configurable delay
 - dry-run mode for safe testing
"""

import os
import argparse
import logging
from datetime import datetime, timedelta, date
import pandas as pd
import gspread
from google.oauth2.credentials import Credentials
from dateutil.relativedelta import relativedelta
import yagmail

# ---------------- CONFIG ---------------- #
CLIENT_SECRET_FILE = r"./credentials.json"   # update path if needed
TOKEN_FILE = r"./token.json"                # update path if needed
SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]
SHEET_NAME = "Activity Tracker"        # Your Google Sheet name
WORKSHEET_NAME = "Task_Tracker"        # Worksheet inside the sheet
COMPLETE_URL_BASE = os.environ.get(
    "COMPLETE_URL_BASE",
    "https://activity-tracker-arjd.onrender.com/complete_task"
)

COMPLETE_API_TOKEN = os.environ.get("COMPLETE_API_TOKEN", "")

GMAIL_USER = "gpchr@ambit.co"          # Gmail account to send emails
GMAIL_APP_PASSWORD = "rfnd mmbe iyno rnaf"   # App password (or SMTP password)
CC_EMAIL = "pulkit.handa@ambit.co"
# Business rules
REMINDER_OFFSET_BUSINESS_DAYS = 3
OVERDUE_REMINDER_DELAY_DAYS = 2  # single overdue reminder sent once when today >= DueDate + this many days

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")


# ---------------- Utilities ---------------- #
def load_credentials(token_file, client_secret_file):
    creds = None
    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(token_file, SCOPES)
    # refresh if expired
    if creds and creds.expired and creds.refresh_token:
        from google.auth.transport.requests import Request
        creds.refresh(Request())
    if not creds or not creds.valid:
        from google_auth_oauthlib.flow import InstalledAppFlow
        flow = InstalledAppFlow.from_client_secrets_file(client_secret_file, SCOPES)
        creds = flow.run_local_server(port=0)
        with open(token_file, "w") as token:
            token.write(creds.to_json())
    return creds


def subtract_business_days(orig_date: date, n: int):
    """Subtract n business days (skip Sat/Sun) from orig_date."""
    d = orig_date
    count = 0
    while count < n:
        d = d - timedelta(days=1)
        if d.weekday() >= 5:  # Saturday=5, Sunday=6
            continue
        count += 1
    return d


def next_monthly_due_date_from_due_day(due_day: int, reference: date):
    """Return next due date for a monthly task with numeric due_day.
        If reference.day <= due_day -> this month's due_day, else next month's due_day."""
    year = reference.year
    month = reference.month
    # try this month
    try:
        candidate = date(year, month, int(due_day))
    except ValueError:
        # invalid day (e.g., 31 in Feb) -> fallback: last day of month
        candidate = (date(year, month, 1) + relativedelta(months=1) - timedelta(days=1))
    if reference.day <= candidate.day and (candidate >= reference):
        return candidate
    # next month
    next_month = reference + relativedelta(months=1)
    try:
        return date(next_month.year, next_month.month, int(due_day))
    except ValueError:
        return (date(next_month.year, next_month.month, 1) + relativedelta(months=1) - timedelta(days=1))


def compute_status(row, today: date):
    """Compute status according to your rules."""
    completion = row.get("CompletionDate")
    due = row.get("DueDate")
    reminder = row.get("ReminderDate")

    # Completed
    if pd.notna(completion):
        try:
            compd = pd.to_datetime(completion).date()
            if pd.notna(due):
                if compd <= pd.to_datetime(due).date():
                    return "Completed"
                else:
                    return "Delayed"
            else:
                return "Completed"
        except Exception:
            return "Completed"

    # Not completed
    if pd.isna(due):
        return "Pending"
    due_date = pd.to_datetime(due).date()

    if today > due_date:
        return "Overdue"
    if pd.notna(reminder):
        if today >= pd.to_datetime(reminder).date():
            return "Reminder Due"
    return "Pending"


# ---------------- Main ---------------- #
def main(dry_run=False):
    today_dt = datetime.today()
    today = today_dt.date()
    logging.info("Starting Activity Tracker Phase 1 (dry_run=%s). Today: %s", dry_run, today.isoformat())

    creds = load_credentials(TOKEN_FILE, CLIENT_SECRET_FILE)
    client = gspread.authorize(creds)
    sheet = client.open(SHEET_NAME).worksheet(WORKSHEET_NAME)

    # Load sheet to DataFrame
    data = sheet.get_all_records()
    df = pd.DataFrame(data)
    # strip column spaces
    df.columns = df.columns.str.strip()

    # Ensure required columns exist (add missing columns)
    required_cols = ['TaskID', 'TaskName', 'Owner', 'Email', 'Department',
                     'Frequency', 'DueDay', 'DueDate', 'ReminderDate',
                     'Status', 'CompletionDate', 'Comments', 'LastReminderSent', 'IsActive']
    for col in required_cols:
        if col not in df.columns:
            logging.info("Adding missing column: %s", col)
            df[col] = pd.NA

    # Normalize types
    df['DueDate'] = pd.to_datetime(df['DueDate'], errors='coerce')
    df['ReminderDate'] = pd.to_datetime(df['ReminderDate'], errors='coerce')
    df['LastReminderSent'] = pd.to_datetime(df['LastReminderSent'], errors='coerce')
    df['CompletionDate'] = pd.to_datetime(df['CompletionDate'], errors='coerce')
    df['IsActive'] = df['IsActive'].fillna(True)

    # Tracking set for rows that have date/status changes and need to be written back
    changed_row_indices = set()

    # ---------------- Monthly rollover on 1st ----------------
    # This logic forces a reset ONLY on the 1st of the month
    if today.day == 1:
        logging.info("Monthly rollover processing (today is 1st of month).")
        for i, row in df.iterrows():
            freq = str(row.get('Frequency') or "").strip().lower()
            if freq == "monthly" or freq == "month":
                due_day = row.get('DueDay')
                if pd.isna(due_day):
                    continue
                try:
                    due_day_int = int(due_day)
                except Exception:
                    logging.warning("Invalid DueDay for row %s: %s", i+2, due_day)
                    continue
                # Check if DueDate is not in the current month/year
                existing_due = row.get('DueDate')
                set_new = False
                if pd.isna(existing_due):
                    set_new = True
                else:
                    existing_due_dt = pd.to_datetime(existing_due).date()
                    if existing_due_dt.month != today.month or existing_due_dt.year != today.year:
                        set_new = True
                if set_new:
                    new_due = next_monthly_due_date_from_due_day(due_day_int, today)
                    df.at[i, 'DueDate'] = pd.Timestamp(new_due)
                    df.at[i, 'ReminderDate'] = pd.Timestamp(subtract_business_days(new_due, REMINDER_OFFSET_BUSINESS_DAYS))
                    df.at[i, 'Status'] = "Pending"
                    df.at[i, 'LastReminderSent'] = pd.NaT
                    changed_row_indices.add(i) # Track change from Day 1 rollover
                    logging.info("Row %d: monthly reset -> DueDate=%s, ReminderDate=%s", i+2, new_due, df.at[i,'ReminderDate'].date())

    # ---------------- Compute DueDate/ReminderDate/Status for all rows ----------------
    for i, row in df.iterrows():
        if not bool(row.get('IsActive', True)):
            df.at[i, 'Status'] = "Inactive"
            continue

        freq = str(row.get('Frequency') or "").strip().lower()

        # --- UPDATED LOGIC TO CALCULATE DATES IF MISSING OR PAST (Runs every day) ---
        existing_due = row.get('DueDate')
        needs_due_date_update = False

        if freq in ("monthly", "month"):
            # Condition 1: DueDate is missing
            if pd.isna(existing_due):
                needs_due_date_update = True
            # Condition 2: DueDate is in the past
            elif pd.to_datetime(existing_due).date() < today:
                needs_due_date_update = True

        if needs_due_date_update:
            due_day = row.get('DueDay')
            if pd.notna(due_day):
                try:
                    due_day_int = int(due_day)
                    
                    # Compute the next DueDate for the current month
                    new_due = next_monthly_due_date_from_due_day(due_day_int, today)
                    
                    # Update all relevant fields
                    df.at[i, 'DueDate'] = pd.Timestamp(new_due)
                    df.at[i, 'ReminderDate'] = pd.Timestamp(
                        subtract_business_days(new_due, REMINDER_OFFSET_BUSINESS_DAYS)
                    )
                    # Reset status and reminder tracking
                    df.at[i, 'Status'] = "Pending"
                    df.at[i, 'LastReminderSent'] = pd.NaT
                    
                    # ADDED: Track this index for sheet write-back
                    changed_row_indices.add(i) 
                    
                    logging.info("Row %d: Date reset/updated: DueDate=%s, ReminderDate=%s", 
                                 i+2, new_due, df.at[i,'ReminderDate'].date())

                except Exception:
                    logging.warning("Could not compute DueDate for row %d; invalid DueDay: %s", i+2, due_day)
        # --- END UPDATED LOGIC ---
        
        # Compute status (runs on every iteration, using the newly computed dates if updated above)
        status = compute_status(df.loc[i], today)
        df.at[i, 'Status'] = status

    # ---------------- Identify reminders to send ----------------
    reminders_to_send = []
    overdue_reminders_to_send = []

    for i, row in df.iterrows():
        # Safe-check for required columns
        email = (row.get('Email') or "").strip()
        if not email:
            continue
        status = row.get('Status')
        if status == "Inactive" or status == "Completed":
            continue

        due = row.get('DueDate')
        reminder = row.get('ReminderDate')
        last_sent = row.get('LastReminderSent')

        # Reminder emails: when today == ReminderDate and status is Reminder Due or Pending (we compute status earlier)
        if pd.notna(reminder) and pd.to_datetime(reminder).date() == today and status != "Completed":
            # Only send if not already sent today
            if pd.isna(last_sent) or pd.to_datetime(last_sent).date() < today:
                reminders_to_send.append((i, row))

        # Overdue single reminder: send once when today >= due + OVERDUE_REMINDER_DELAY_DAYS
        if pd.notna(due):
            due_date = pd.to_datetime(due).date()
            overdue_trigger_date = due_date + timedelta(days=OVERDUE_REMINDER_DELAY_DAYS)
            if today >= overdue_trigger_date and status != "Completed":
                # send overdue only if not already sent on/after the trigger date
                if pd.isna(last_sent) or pd.to_datetime(last_sent).date() < overdue_trigger_date:
                    overdue_reminders_to_send.append((i, row))

    logging.info("Reminders to send: %d, Overdue reminders to send: %d", len(reminders_to_send), len(overdue_reminders_to_send))

    # ---------------- Send emails ----------------
    if not dry_run:
        yag = yagmail.SMTP(GMAIL_USER, GMAIL_APP_PASSWORD)
    else:
        yag = None

    def send_reminder_email(row, kind="reminder"):
        owner = row.get('Owner') or row.get('TaskName') or ""
        to_email = row.get('Email')
        task_name = row.get('TaskName') or "Unnamed Task"
        due = row.get('DueDate')
        due_str = pd.to_datetime(due).strftime("%d-%b-%Y") if pd.notna(due) else "N/A"
        complete_link = f"{COMPLETE_URL_BASE}?task_id={row.get('TaskID')}&token={COMPLETE_API_TOKEN}"


        if kind == "reminder":
            subject = f"⏰ Reminder: {task_name} due on {due_str} "
            body = f"""Hi {owner},

This is a friendly reminder that your task '{task_name}' is due on {due_str}.
Please ensure it is submitted on time.
If it's completed kindly click here to mark it complete (One Click): {complete_link}

Regards,
GPC HR TEAM
"""
        elif kind == "overdue":
            subject = f"❗ Overdue: {task_name} was due on {due_str}"
            body = f"""Hi {owner},

The task '{task_name}' was due on {due_str} and is now overdue.
Please complete it as soon as possible.
Click here to mark it complete: {complete_link}

Regards,
GPC HR TEAM
"""
        else:
            subject = f"Notification: {task_name}"
            body = f"Hi {owner},\n\nThis is a notification about the task '{task_name}'.\n\nRegards,\nActivity Tracker"

        logging.info("Preparing to send %s email to %s for task '%s' (due=%s)", kind, to_email, task_name, due_str)
        if dry_run:
            logging.info("[DRY RUN] Would send email: To=%s Subject=%s", to_email, subject)
            return True
        try:
            yag.send(to=[to_email],cc=[CC_EMAIL], subject=subject, contents=body)
            logging.info("Email sent to %s", to_email)
            return True
        except Exception as e:
            logging.exception("Failed to send email to %s: %s", to_email, e)
            return False

    # Track which rows we update LastReminderSent for
    updated_rows = {}

    # Send standard reminders
    for (i, row) in reminders_to_send:
        ok = send_reminder_email(row, kind="reminder")
        if ok:
            df.at[i, 'LastReminderSent'] = pd.Timestamp(today)
            updated_rows[i] = df.at[i, 'LastReminderSent']
            changed_row_indices.add(i) # Also track rows that sent reminders

    # Send overdue reminders (single)
    for (i, row) in overdue_reminders_to_send:
        ok = send_reminder_email(row, kind="overdue")
        if ok:
            df.at[i, 'LastReminderSent'] = pd.Timestamp(today)
            updated_rows[i] = df.at[i, 'LastReminderSent']
            changed_row_indices.add(i) # Also track rows that sent overdue reminders

    # ---------------- Update Google Sheet (LastReminderSent, DueDate, ReminderDate, Status) ----------------
    
    # Get the union of all indices that had a date/status change or an email sent
    all_indices_to_write = changed_row_indices
    
    if dry_run:
        logging.info("Dry-run: not writing back to Google Sheet. Updated rows: %s", list(all_indices_to_write))
    else:
        logging.info("Updating sheet for %d rows.", len(all_indices_to_write))
        
        # Find column indices in the sheet header
        header = sheet.row_values(1)
        header = [h.strip() for h in header]
        # mapping
        col_map = {name: idx+1 for idx, name in enumerate(header)}
        def safe_update_cell(row_number, col_name, value):
            if col_name not in col_map:
                logging.warning("Column %s not in sheet header; skipping update.", col_name)
                return
            col_idx = col_map[col_name]
            # Convert pandas.Timestamp/NaT to string or blank
            if pd.isna(value):
                val = ""
            elif isinstance(value, pd.Timestamp):
                val = value.strftime("%Y-%m-%d")
            elif isinstance(value, (datetime, date)):
                val = value.isoformat()
            else:
                val = str(value)
            try:
                sheet.update_cell(row_number, col_idx, val)
            except Exception as e:
                logging.exception("Failed to update sheet cell (r=%d c=%s): %s", row_number, col_name, e)

        # Update all changed rows
        cols_to_write = ["DueDate", "ReminderDate", "Status", "LastReminderSent", "CompletionDate"]
        for i in all_indices_to_write:
            sheet_row = i + 2  # because DataFrame index 0 => sheet row 2
            for col in cols_to_write:
                val = df.at[i, col] if col in df.columns else pd.NA
                safe_update_cell(sheet_row, col, val)

    logging.info("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Don't send emails or update sheet; just print what would happen.")
    args = parser.parse_args()
    main(dry_run=args.dry_run)