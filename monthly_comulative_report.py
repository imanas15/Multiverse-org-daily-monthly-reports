import pytz
import datetime
from pymongo import MongoClient
import gspread
import pandas as pd
import os
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
from send_email import send_mail

load_dotenv(override=True)
BASE_DIR = os.path.abspath(os.curdir)

# ------------------ CONFIG ------------------
MONGO_URI = os.environ.get("MONGO_URI")
DB_NAME = "kazam-platform"
VEHICLE_COL = "fms-vehicles"
TXN_COL = "transactions"

SHEET_NAME = "Testing Monthly Report"
WORKSHEET_NAME = "Monthly Report"

EMAIL_SENDER = os.environ.get("EMAIL_SENDER")
EMAIL_PASSWORD = os.environ.get("EMAIL_PASSWORD")

# ✅ Always use module-style
now = datetime.datetime.now()

# ------------------ GOOGLE SHEETS SETUP ------------------
import json

def connect_sheet():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]

    creds_dict = json.loads(os.environ["GOOGLE_CREDS"])

    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        creds_dict, scope
    )

    client = gspread.authorize(creds)

    sheet = client.open(SHEET_NAME).worksheet(WORKSHEET_NAME)
    return sheet


# ------------------ TIME RANGE ------------------
def get_epoch_range():
    # ✅ Always use datetime.datetime
    current_time = datetime.datetime.now()

    start = current_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    first_day_current = start.replace(day=1)
    previous_month = first_day_current - datetime.timedelta(days=1)

    month_name = previous_month.strftime("%B")

    if start.month == 12:
        next_month = start.replace(year=current_time.year + 1, month=1, day=1,
                                  hour=0, minute=0, second=0, microsecond=0)
    else:
        next_month = start.replace(month=start.month + 1, day=1,
                                  hour=0, minute=0, second=0, microsecond=0)

    end = next_month

    return (
        int(start.timestamp() * 1000),
        int(end.timestamp() * 1000),
        int(start.timestamp()),
        int(end.timestamp()),
        str(month_name)
    )


# ------------------ MONGO ------------------
client = MongoClient(MONGO_URI)
db = client[DB_NAME]


def get_vehicle_count():
    pipeline = [
            {
                '$group': {
                    '_id': '$org', 
                    'vehicle_count': {
                        '$sum': 1
                    }
                }
            }, {
                '$lookup': {
                    'from': 'drivers', 
                    'let': {
                        'org': '$_id'
                    }, 
                    'pipeline': [
                        {
                            '$match': {
                                'org': {
                                    '$ne': '', 
                                    '$exists': True
                                }, 
                                '$expr': {
                                    '$eq': [
                                        '$org', '$$org'
                                    ]
                                }
                            }
                        }
                    ], 
                    'as': 'user_count'
                }
            }, {
                '$addFields': {
                    'user_count': {
                        '$size': '$user_count'
                    }
                }
            }
        ]

    result = list(db[VEHICLE_COL].aggregate(pipeline))
    if result:
        return pd.DataFrame(result)
    return pd.DataFrame()


def get_txn_data(from_sec, to_sec):
    print(f"Fetching transactions from {from_sec} to {to_sec}")

    pipeline = [
        {
            '$match': {
                'start_time': {
                    '$gte': from_sec,
                    '$lt': to_sec
                }
            }
        },
        {
            '$group': {
                '_id': '$org',
                'txn_count': {'$sum': 1},
                'total_usage': {
                    '$sum': {
                        '$ifNull': [
                            '$rectified_values.total_usage', '$total_usage'
                        ]
                    }
                # },
                # 'total_users': {
                #     '$addToSet': '$user_id'
               }
            }
        },
        {
            '$project': {
                '_id': 0,
                'org': '$_id',
                'total_usage_kwh': {'$divide': ['$total_usage', 1000]},
                # 'total_unique_drivers': {'$size': '$total_users'},
                'txn_count': 1
            }
        }
    ]

    result = list(db[TXN_COL].aggregate(pipeline))
    if result:
        df = pd.DataFrame(result)
        df.sort_values(by=['org'], inplace=True, ignore_index=True,
                       ascending=True, key=lambda x: x.str.lower())
        return df
    return pd.DataFrame()


# ------------------ MAIN LOGIC ------------------
def run_job():
    sheet = connect_sheet()

    from_ms, to_ms, from_sec, to_sec, month = get_epoch_range()

    vehicle_count_today = get_vehicle_count()
    txn_data_today = get_txn_data(from_sec, to_sec)

    final_df = pd.merge(
        txn_data_today,
        vehicle_count_today,
        left_on='org',
        right_on='_id',
        how='left'
    ).fillna("")

    final_df['month'] = month

    final_df = final_df[
        ["month", "org", "txn_count", "total_usage_kwh",
         "user_count", "vehicle_count"]
    ]

    # ✅ FIX: remove NaN
    final_df = final_df.fillna(0)

    if not sheet.row_values(1):
        sheet.append_row(final_df.columns.tolist())

    sheet.append_rows(final_df.values.tolist(), value_input_option="USER_ENTERED")

    send_mail(
        subject="Monthly Org Wise Transactions Report Mailer",
        body=f"""
Hello Team,

The monthly org wise report has successfully completed.

Updated Google Spreadsheet: {SHEET_NAME}
Sheet Name: {WORKSHEET_NAME}

Month: {month}

This mail is automatically generated.

Thanks and Regards,
Manas Barnwal
""",
        gmail_user=EMAIL_SENDER,
        gmail_pass=EMAIL_PASSWORD
    )

    print("✅ Data appended successfully")


if __name__ == "__main__":
    run_job()
