from flask import Flask, request
import gspread
from google.oauth2.service_account import Credentials
import datetime

app = Flask(__name__)

# Google Sheet setup
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SERVICE_ACCOUNT_FILE = r"D:\Arjun\Activity_tracker\credentials.json"
SHEET_NAME = "Activity Tracker"
WORKSHEET_NAME = "Task_Tracker"

creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
client = gspread.authorize(creds)
sheet = client.open(SHEET_NAME).worksheet(WORKSHEET_NAME)

@app.route("/complete_task")
def complete_task():
    task_name = request.args.get("task")
    if not task_name:
        return "❌ No task specified."

    data = sheet.get_all_values()
    headers = data[0]
    task_col = headers.index("TaskName")
    status_col = headers.index("Status")
    completion_col = headers.index("CompletionDate")

    for i, row in enumerate(data[1:], start=2):
        if row[task_col].strip() == task_name.strip():
            if row[status_col].strip() == "Completed":
                return "❌ Task already completed."
            else:
                sheet.update_cell(i, status_col + 1, "Completed")
                sheet.update_cell(i, completion_col + 1, datetime.datetime.now().strftime("%Y-%m-%d"))
                return "✅ Task marked as completed!"
    return "❌ Task not found."

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
