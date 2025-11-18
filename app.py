#!/usr/bin/env python3
# app.py
from flask import Flask, request, jsonify, abort, render_template_string, render_template
import os
import logging
from datetime import datetime, date
import pandas as pd
import gspread
from google.oauth2.credentials import Credentials
from dateutil.relativedelta import relativedelta
from datetime import timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
app = Flask(__name__, template_folder='templates', static_folder='static')

def write_google_credentials():
    cred_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    token_json = os.environ.get("GOOGLE_TOKEN_JSON")

    if cred_json:
        logging.info("Writing credentials.json from environment")
        with open("credentials.json", "w") as f:
            f.write(cred_json)

    if token_json:
        logging.info("Writing token.json from environment")
        with open("token.json", "w") as f:
            f.write(token_json)

write_google_credentials()   # <-- runs automatically both on Render & local



# Config via env
CLIENT_SECRET_FILE = os.environ.get("CLIENT_SECRET_FILE", "./credentials.json")
TOKEN_FILE = os.environ.get("TOKEN_FILE", "./token.json")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
SHEET_NAME = os.environ.get("SHEET_NAME", "Activity Tracker")
WORKSHEET_NAME = os.environ.get("WORKSHEET_NAME", "Task_Tracker")
SECRET_TOKEN = os.environ.get("COMPLETE_API_TOKEN")  # required
GMAIL_USER = os.environ.get("GMAIL_USER", "")  # used to send completion mail
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")  # used to send completion mail

# helper identical to your script
def load_credentials(token_file, client_secret_file):
    creds = None
    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(token_file, SCOPES)
    if creds and creds.expired and creds.refresh_token:
        from google.auth.transport.requests import Request
        creds.refresh(Request())
    if not creds or not creds.valid:
        raise RuntimeError("No valid credentials available. Place token.json and credentials.json.")
    return creds

def next_monthly_due_date_from_due_day(due_day: int, reference: date):
    from dateutil.relativedelta import relativedelta
    year = reference.year
    month = reference.month
    try:
        candidate = date(year, month, int(due_day))
    except ValueError:
        candidate = (date(year, month, 1) + relativedelta(months=1) - timedelta(days=1))
    if reference.day <= candidate.day and (candidate >= reference):
        return candidate
    next_month = reference + relativedelta(months=1)
    try:
        return date(next_month.year, next_month.month, int(due_day))
    except ValueError:
        return (date(next_month.year, next_month.month, 1) + relativedelta(months=1) - timedelta(days=1))

# very small mailer using yagmail (optional)
def send_completion_email(to_email, owner, task_name, completion_date):
    try:
        import yagmail
        if not GMAIL_USER or not GMAIL_APP_PASSWORD:
            logging.info("GMAIL_USER or app password not provided; skipping completion email.")
            return
        yag = yagmail.SMTP(GMAIL_USER, GMAIL_APP_PASSWORD)
        subject = f"✅ Update: {task_name} Task Completed"
        body = f"Hi {owner},\n\nThank you for the update. We have recorded the completion of '{task_name}' task as of  {completion_date}.\n\nRegards,\nGPC HR TEAM\n"
        yag.send(to=to_email, subject=subject, contents=body)
        logging.info("Sent completion mail to %s", to_email)
    except Exception as e:
        logging.exception("Failed to send completion mail: %s", e)

@app.route("/health", methods=["GET"])
def health():
    return "OK", 200

@app.route("/complete_task", methods=["GET"])
def complete_task():
    # Validate token
    token = request.args.get("token", "")
    print("SECRET_TOKEN:", repr(SECRET_TOKEN))
    print("URL token:", repr(token))
    if not SECRET_TOKEN:
        logging.error("COMPLETE_API_TOKEN env var not set.")
        return jsonify({"error": "Server misconfigured."}), 500
    if token != SECRET_TOKEN:
        logging.warning("Invalid token attempt.")
        return jsonify({"error": "Unauthorized"}), 401

    task_id = request.args.get("task_id")
    if not task_id:
        return jsonify({"error": "task_id required"}), 400

    # Load sheet
    try:
        creds = load_credentials(TOKEN_FILE, CLIENT_SECRET_FILE)
        client = gspread.authorize(creds)
        sheet = client.open(SHEET_NAME).worksheet(WORKSHEET_NAME)
    except Exception as e:
        logging.exception("Failed to open Google Sheet: %s", e)
        return jsonify({"error": "Failed to open sheet"}), 500

    # Read all rows
    rows = sheet.get_all_records()
    df = pd.DataFrame(rows)
    df.columns = df.columns.str.strip()

    if 'TaskID' not in df.columns:
        return jsonify({"error": "TaskID column not found in sheet"}), 500

    # find row index
    match = df.index[df['TaskID'].astype(str) == str(task_id)].tolist()
    if not match:
        return jsonify({"error": "Task not found"}), 404
    i = match[0]

    # Check already completed (for current cycle)
    completion = df.at[i, 'CompletionDate'] if 'CompletionDate' in df.columns else None
    if pd.notna(completion) and str(completion).strip() != "":
        return jsonify({"message": "Task already completed for current cycle."}), 200

    # Compute updates
    today = datetime.utcnow().date()  # use UTC for reproducibility; or use localdate()
    due_day = df.at[i, 'DueDay'] if 'DueDay' in df.columns else None
    task_name = df.at[i, 'TaskName'] if 'TaskName' in df.columns else f"{task_id}"
    owner = df.at[i, 'Owner'] if 'Owner' in df.columns else ""
    to_email = df.at[i, 'Email'] if 'Email' in df.columns else ""

    # Update sheet cells for this row: CompletionDate, Status, LastReminderSent cleared, compute next due date for monthly tasks
    # We need to map to sheet row number
    sheet_header = sheet.row_values(1)
    sheet_header = [h.strip() for h in sheet_header]
    col_map = {name: idx+1 for idx, name in enumerate(sheet_header)}
    sheet_row = i + 2

    def update_cell(col_name, value):
        if col_name not in col_map:
            logging.warning("Column %s not found in sheet header; skipping", col_name)
            return
        col = col_map[col_name]
        val = ""
        if value is None:
            val = ""
        elif isinstance(value, (date, datetime)):
            val = value.strftime("%Y-%m-%d")
        else:
            val = str(value)
        try:
            sheet.update_cell(sheet_row, col, val)
        except Exception as e:
            logging.exception("Failed to update sheet cell %s: %s", col_name, e)

    # Set completion
    update_cell("CompletionDate", today)
    update_cell("Status", "Completed")
    update_cell("LastReminderSent", "")  # clear so next cycle starts clean

    # If monthly, compute next due date immediately and write it (so next cycle is prepared)
    try:
        freq = str(df.at[i, 'Frequency']).strip().lower() if 'Frequency' in df.columns else ""
        if freq in ("monthly", "month") and pd.notna(due_day):
            try:
                dd = int(due_day)
                # compute next month's due date (strictly next occurrence after today)
                # pass reference = today + 1 day so function returns next occurrence
                next_ref = today + relativedelta(days=1)
                new_due = next_monthly_due_date_from_due_day(dd, next_ref)
                update_cell("DueDate", new_due)
                # compute new reminder date (3 business days back)
                def subtract_business_days(orig_date, n):
                    d = orig_date
                    cnt = 0
                    while cnt < n:
                        d = d - timedelta(days=1)
                        if d.weekday() >= 5:
                            continue
                        cnt += 1
                    return d
                new_reminder = subtract_business_days(new_due, int(os.environ.get("REMINDER_OFFSET_BUSINESS_DAYS", "3")))
                update_cell("ReminderDate", new_reminder)
                # update_cell("Status", "Pending")
            except Exception:
                logging.exception("Could not compute next month due date.")
    except Exception:
        logging.exception("Error while computing next due date.")

    # Send completion email
    try:
        send_completion_email(to_email, owner, task_name, today)
    except Exception:
        logging.exception("Failed to send completion email.")

    return render_template(
    "task_completed.html",
    task_id=task_id,
    task_name=task_name,
    owner=owner,
    completed_on=today
)
 

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
