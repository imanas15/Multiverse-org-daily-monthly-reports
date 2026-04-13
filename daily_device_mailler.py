import gspread
from oauth2client.service_account import ServiceAccountCredentials
import datetime
import pytz
import pandas as pd
from pymongo import MongoClient
import smtplib
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv
from send_email import send_mail

load_dotenv(override=True)

# ------------------ CONFIG ------------------
MONGO_URI = os.environ.get("MONGO_URI")
print("MONGO_URI:", MONGO_URI)
DB_NAME = "kazam-platform"
COLLECTION_NAME = "device_uptime_daily_agg_new"

EMAIL_SENDER = os.environ.get("EMAIL_SENDER")
EMAIL_PASSWORD = os.environ.get("EMAIL_PASSWORD")

UTC = pytz.utc

# ------------------ MONGO SETUP ------------------
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

SHEET_NAME = "Testing Monthly Report"
WORKSHEET_NAME = "Daily Report"

# ------------------ GOOGLE SHEETS SETUP ------------------
def connect_sheet():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = ServiceAccountCredentials.from_json_keyfile_name(
        "agent-Gdocs-loader.json", scope
    )
    client = gspread.authorize(creds)

    sheet = client.open(SHEET_NAME).worksheet(WORKSHEET_NAME)
    return sheet

# ------------------ TIME RANGE ------------------
def get_epoch_range():
    now = datetime.datetime.now(UTC)

    # Today 00:00 UTC
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    # Yesterday 00:00 UTC
    yesterday_start = today_start - datetime.timedelta(days=1)

    from_epoch = int(yesterday_start.timestamp())
    to_epoch = int(today_start.timestamp())

    month_name = yesterday_start.strftime("%B")

    return from_epoch, to_epoch, month_name

# ------------------ PIPELINE ------------------
def run_pipeline():
    from_epoch, to_epoch, month = get_epoch_range()

    print(f"Running for range: {from_epoch} → {to_epoch}")

    pipeline = [
        {
            '$match': {
                'from': {'$gte': from_epoch},
                'to': {'$lte': to_epoch}
            }
        },
        {
            '$lookup': {
                'from': 'device-details',
                'localField': 'device_id',
                'foreignField': 'device_id',
                'as': 'result'
            }
        },
        {'$unwind': {'path': '$result'}},
        {
            '$match': {
                'result.host_details.host_id': {
                    '$nin': ['null', None, '', ' ', 'undefined']
                },
                'result.output_type': {
                    '$in': ['ac', 'dc']
                }
            }
        },
        {
            '$group': {
                '_id': ['$org', '$result.output_type'],
                'count': {'$sum': 1},
                'from': {'$first': '$from'}
            }
        },
        {
            '$project': {
                '_id': 0,
                'org': {'$arrayElemAt': ['$_id', 0]},
                'output_type': {'$arrayElemAt': ['$_id', 1]},
                'active_devices': '$count',
                'date': {
                    '$dateToString': {
                        'format': '%Y-%m-%d',
                        'date': {
                            '$toDate': {'$multiply': ['$from', 1000]}
                        },
                        'timezone': 'Asia/Kolkata'
                    }
                }
            }
        }
    ]

    result = list(collection.aggregate(pipeline))

    if result:
        print("Length of data: ", len(result))
        df = pd.DataFrame(result)
        df.sort_values(by=['org'], inplace=True, ignore_index=True,
                       ascending=True, key=lambda x: x.str.lower())
        df['month'] = month
        return df

    return result

# ------------------ MAIN ------------------
def main():
    result_df = run_pipeline()

    if isinstance(result_df, pd.DataFrame) and not result_df.empty:
        sheet = connect_sheet()

        # Ensure column order
        result_df = result_df[
            ["date", "month", "org", "output_type", "active_devices"]
        ]

        # Append header if empty
        if not sheet.row_values(1):
            sheet.append_row(result_df.columns.tolist())

        # Append data
        sheet.append_rows(
            result_df.values.tolist(),
            value_input_option="USER_ENTERED"
        )

        print("✅ Data appended successfully")

        send_mail(
            subject="Daily Org Wise Device Uptime Mailer",
            body=f"""
Hello Team,

The daily Org wise Device Uptime report has successfully completed.

Updated Google Spreadsheet: {SHEET_NAME}
Sheet Name: {WORKSHEET_NAME}

Date: {datetime.datetime.now().strftime('%A, %d %B %Y')}

All the latest data is now available in the Google Sheet:
https://docs.google.com/spreadsheets/d/1aRElH0Sndyow6QAl2jlcQ-oMFmdoqAH0cljfhJFeCuM/edit

This mail is automatically generated.

Thanks and Regards,  
Manas Barnwal  
Data Science Engineer  
+91 7054418401
""",
            gmail_user=EMAIL_SENDER,
            gmail_pass=EMAIL_PASSWORD
        )

    else:
        print("No data to append")

# ------------------ RUN ------------------
if __name__ == "__main__":
    main()
