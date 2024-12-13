import os
import schedule
import time
import datetime
from utils import anomaly_checkerV3
from dotenv import load_dotenv

load_dotenv()

# Define the tasks
def daily_job():
    print("Running daily report...")
    anomaly_checkerV3.execute()
    # Run the daily analysis and send the report

def weekly_job():
    print("Running weekly report...")
    anomaly_checkerV3.execute()
    # Run the weekly analysis and send the report

def monthly_job():
    print("Running monthly report...")
    anomaly_checkerV3.execute()
    # Run the monthly analysis and send the report

def yearly_job():
    print("Running yearly report...")
    anomaly_checkerV3.execute()
    # Run the yearly analysis and send the report

# Utility function to check end of month
def is_end_of_month():
    today = datetime.date.today()
    tomorrow = today + datetime.timedelta(days=1)
    return tomorrow.month != today.month  # True if today is the last day of the month

# Utility function to check end of year
def is_end_of_year():
    today = datetime.date.today()
    return today.month == 12 and today.day == 31

daily_alert_time = os.getenv("DAILY_ALERT_TIME") 
week_alert_time = os.getenv("WEEKLY_ALERT_TIME") 
month_year_alert_time = os.getenv("MONTH_YEAR_ALERT_TIME") 

# Schedule daily job
# schedule.every().day.at("18:00").do(daily_job)
schedule.every().day.at(daily_alert_time).do(daily_job)

# Schedule weekly job (every Sunday at 8 PM)
# schedule.every().sunday.at("20:00").do(weekly_job)
schedule.every().sunday.at(week_alert_time).do(weekly_job)

# Schedule end-of-month job
# schedule.every().day.at("23:59").do(lambda: monthly_job() if is_end_of_month() else None)
schedule.every().day.at(month_year_alert_time).do(lambda: monthly_job() if is_end_of_month() else None)

# Schedule end-of-year job
# schedule.every().day.at("23:59").do(lambda: yearly_job() if is_end_of_year() else None)
schedule.every().day.at(month_year_alert_time).do(lambda: yearly_job() if is_end_of_year() else None)


def scheduler_execute():
    # Keep running the scheduler
    while True:
        schedule.run_pending()
        time.sleep(1)
