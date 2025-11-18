#!/usr/bin/env python3
import os
import logging
from flask import Flask, request, jsonify, render_template
import pandas as pd
import gspread
from google.oauth2.credentials import Credentials
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
import threading

logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")

app = Flask(__name__, template_folder="templates", static_folder="static")

# ---------------------------------------------------------
# WRITE GOOGLE CREDS FROM ENV → credentials.json + token.json
# ---------------------------------------------------------
def write_google_credentials():
    cred_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    token_json = os.environ.get("GOOGLE_TOKEN_JSON")

    if cred_json:
        with open("credentials.json", "w") as f:
            f.write(cred_json)

    if token_json:
        with open("token.json", "w") as f:
            f.write(token_json)

write_google_credentials()


# ---------------------------------------------------------
# GLOBAL CACHED CLIENT to avoid timeout
# ---------------------------------------------------------
gspread_client = None
worksheet_cache = None

SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
SHEET_NAME = os.environ.get("SHEET_NAME", "Activity Tracker")
WORKSHEET_NAME = os.environ.get("WORKSHEET_NAME", "Task_Tracker")
SECRET_TOKEN = os.environ.get("COMPLETE_API_TOKEN")

# ---------------------------------------------------------
# LOAD CREDENTIALS
# ---------------------------------------------------------
def load_gspread():
    global gspread_client, worksheet_cache

    if gspread_client is None:
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
        gspread_client = gspread.authorize(creds)

    if worksheet_cache is None:
        sheet = gspread_client.open(SHEET_NAME)
        worksheet_cache = sheet.worksheet(WORKSHEET_NAME)

    return worksheet_cache


# ---------------------------------------------------------
# EMAIL (RUN IN BACKGROUND THREAD)
# ---------------------------------------------------------
def send_completion_email(to_email, owner, task_name, completion_date):
    try:
        import yagmail
        gmail_user = os.environ.get("GMAIL_USER")
        gmail_pass = os.environ.get("GMAIL_APP_PASSWORD")

        if not gmail_user or not gmail_pass:
            logging.info("Email not configured. Skipping email…")
            return

        yag = yagmail.SMTP(gmail_user, gmail_pass)
        subject = f"Task Completed: {task_name}"
        body = (
            f"Hi {owner},\n\n"
            f"The task '{task_name}' has been marked completed on {completion_date}.\n\n"
            f"Regards,\nGPC HR Team"
        )
        yag.send(to_email, subject, body)
        logging.info("Email sent to %s", to_email)

    except Exception as e:
        logging.exception("Email send failed: %s", e)


# ---------------------------------------------------------
# HELPER: COMPUTE NEXT MONTHLY DUE DATE
# ---------------------------------------------------------
def next_monthly_due_date(due_day, reference_date):
    year = reference_date.year
    month = reference_date.month

    try:
        candidate = date(year, month, int(due_day))
    except:
        candidate = date(year, month, 1) + relativedelta(months=1) - timedelta(days=1)

    if reference_date <= candidate:
        return candidate

    next_m = reference_date + relativedelta(months=1)
    try:
        return date(next_m.year, next_m.month, int(due_day))
    except:
        return date(next_m.year, next_m.month, 1) + relativedelta(months=1) - timedelta(days=1)


# ---------------------------------------------------------
# SIMPLE TEST ROUTE
# ---------------------------------------------------------
@app.route("/")
def home():
    return "Service is running!", 200


@app.route("/test_template")
def test_template():
    return render_template(
        "task_completed.html",
        task_id="DEMO",
        task_name="Demo Task",
        owner="Arjun",
        completed_on=date.today()
    )


# ---------------------------------------------------------
# MAIN COMPLETE TASK ROUTE
# ---------------------------------------------------------
@app.route("/complete_task", methods=["GET"])
def complete_task():
    token = request.args.get("token")
    if token != SECRET_TOKEN:
        return jsonify({"error": "Unauthorized"}), 401

    task_id = request.args.get("task_id")
    if not task_id:
        return jsonify({"error": "task_id required"}), 400

    try:
        sheet = load_gspread()
    except Exception as e:
        logging.exception("Failed loading sheet")
        return jsonify({"error": "Failed to connect to Google Sheet"}), 500

    # Read sheet data
    df = pd.DataFrame(sheet.get_all_records())
    df.columns = df.columns.str.strip()

    if "TaskID" not in df.columns:
        return jsonify({"error": "TaskID column missing"}), 500

    match = df.index[df["TaskID"].astype(str) == str(task_id)].tolist()
    if not match:
        return jsonify({"error": "Task not found"}), 404

    row_index = match[0]
    sheet_row = row_index + 2  # account for header row

    # Extract details
    task_name = df.at[row_index, "TaskName"]
    owner = df.at[row_index, "Owner"]
    email = df.at[row_index, "Email"]
    due_day = df.at[row_index, "DueDay"]
    freq = str(df.at[row_index, "Frequency"]).lower().strip()

    today = date.today()

    # Update fields
    col_map = {name.strip(): idx + 1 for idx, name in enumerate(sheet.row_values(1))}

    def update(col, value):
        if col not in col_map:
            return
        col_num = col_map[col]
        val = value.strftime("%Y-%m-%d") if isinstance(value, (date, datetime)) else str(value)
        sheet.update_cell(sheet_row, col_num, val)

    update("CompletionDate", today)
    update("Status", "Completed")
    update("LastReminderSent", "")

    if freq == "monthly" and due_day:
        new_due = next_monthly_due_date(int(due_day), today + timedelta(days=1))
        update("DueDate", new_due)

        # Compute reminder date (3 business days before)
        rem = new_due
        count = 0
        while count < 3:
            rem -= timedelta(days=1)
            if rem.weekday() < 5:
                count += 1
        update("ReminderDate", rem)

    # EMAIL → run async
    threading.Thread(
        target=send_completion_email,
        args=(email, owner, task_name, today)
    ).start()

    # RETURN HTML IMMEDIATELY
    return render_template(
        "task_completed.html",
        task_id=task_id,
        task_name=task_name,
        owner=owner,
        completed_on=today
    )


# ---------------------------------------------------------
# RUN APP
# ---------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
