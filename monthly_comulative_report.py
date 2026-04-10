import pytz
# from datetime import datetime, timedelta
import datetime
from pymongo import MongoClient
import gspread
import pandas as pd
import os
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv

load_dotenv(override=True)
BASE_DIR = os.path.abspath(os.curdir)

# ------------------ CONFIG ------------------
MONGO_URI = os.environ.get("MONGO_URI")
DB_NAME = "kazam-platform"
VEHICLE_COL = "fms-vehicles"
TXN_COL = "transactions"

SHEET_NAME = "Testing Monthly Report"
WORKSHEET_NAME = "Monthly Report"

# IST = pytz.timezone("Asia/Kolkata")
# now = datetime.now()
now = datetime.datetime.now()

# ------------------ GOOGLE SHEETS SETUP ------------------
def connect_sheet():
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]

    creds = ServiceAccountCredentials.from_json_keyfile_name(
        "service_account.json", scope
    )
    client = gspread.authorize(creds)

    sheet = client.open(SHEET_NAME).worksheet(WORKSHEET_NAME)
    return sheet


# ------------------ TIME RANGE ------------------
def get_epoch_range():
    # ✅ First day of current month (00:00 IST)
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    first_day_current = start.replace(day=1)
    previous_month = first_day_current - datetime.timedelta(days=1)

    # Format to name
    month_name = previous_month.strftime("%B")

    # ✅ First day of next month (00:00 IST)
    if start.month == 12:
        next_month = start.replace(year=now.year + 1, month=1, day=1,
                                hour=0, minute=0, second=0, microsecond=0)
    else:
        next_month = start.replace(month=start.month + 1, day=1,
                                hour=0, minute=0, second=0, microsecond=0)

    # 👉 Use next_month as END (exclusive)
    end = next_month

    return (
        int(start.timestamp() * 1000),   # ms (vehicle)
        int(end.timestamp() * 1000),
        int(start.timestamp()),          # sec (txn)
        int(end.timestamp()),
        str(month_name)
    )


# ------------------ MONGO ------------------
client = MongoClient(MONGO_URI)
db = client[DB_NAME]


def get_vehicle_count(from_ms, to_ms):
    pipeline = [
        {
            '$match': {
                'timestamp': {
                    '$gte': from_ms, 
                    '$lt': to_ms
                }
            }
        }, {
            '$group': {
                '_id': '$org', 
                'vehicle_count': {
                    '$sum': 1
                }
            }
        }
    ]

    result = list(db[VEHICLE_COL].aggregate(pipeline))
    if result:
        df = pd.DataFrame(result)
        return df
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
        }, {
            '$group': {
                '_id': '$org', 
                'txn_count': {
                    '$sum': 1
                }, 
                'total_usage': {
                    '$sum': {
                        '$ifNull': [
                            '$rectified_values.total_usage', '$total_usage'
                        ]
                    }
                }, 
                'total_users': {
                    '$addToSet': '$user_id'
                }
            }
        }, {
            '$project': {
                '_id': 0, 
                'org': '$_id', 
                'total_usage_kwh': {
                    '$divide': [
                        '$total_usage', 1000
                    ]
                }, 
                'total_unique_drivers': {
                    '$size': '$total_users'
                }, 
                'txn_count': 1
            }
        }
    ]

    result = list(db[TXN_COL].aggregate(pipeline))
    if result:
        df = pd.DataFrame(result)
        df.sort_values(by=['org'], inplace=True, ignore_index=True, ascending=True, key=lambda x: x.str.lower())
        return df
    return pd.DataFrame()

# ------------------ MAIN LOGIC ------------------
def run_job():
    sheet = connect_sheet()

    # Get today's data
    from_ms, to_ms, from_sec, to_sec, month = get_epoch_range()

    vehicle_count_today = get_vehicle_count(from_ms, to_ms)
    txn_data_today = get_txn_data(from_sec, to_sec)

    final_df = pd.merge(txn_data_today, vehicle_count_today, left_on='org', right_on='_id', how='left').fillna("")
    final_df['month'] = month

    # # ------------------ CUMULATIVE ADD ------------------
    # new_row = {
    #     "month": month,

    #     "vehicle_count":
    #         last_row.get("vehicle_count", 0) + vehicle_count_today,

    #     "txn_count":
    #         last_row.get("txn_count", 0) + txn_data_today["txn_count"],

    #     "total_usage":
    #         last_row.get("total_usage", 0) + txn_data_today["total_usage"],

    #     "total_unique_drivers":
    #         last_row.get("total_unique_drivers", 0) + txn_data_today["total_unique_drivers"]
    # }

    # Append to sheet
    final_df = final_df[["month", "org", "txn_count", "total_usage_kwh", "total_unique_drivers", "vehicle_count"]]

    # Append header if sheet empty
    if not sheet.row_values(1):
        sheet.append_row(final_df.columns.tolist())

    # Append data
    sheet.append_rows(final_df.values.tolist(), value_input_option="USER_ENTERED")

    print("✅ Data appended successfully")


if __name__ == "__main__":
    run_job()