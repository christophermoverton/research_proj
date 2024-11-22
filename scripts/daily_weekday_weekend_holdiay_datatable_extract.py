import os
import pandas as pd
import numpy as np
from pandas.tseries.holiday import USFederalHolidayCalendar
from datetime import datetime, timedelta

# Directory where the research project data is stored
base_directory = "/media/christopher/Extreme SSD"
output_dir = os.path.join(base_directory, "research_project_data", "weekends_weekday_holiday")  # Directory to store final data
os.makedirs(output_dir, exist_ok=True)  # Ensure the output directory exists

# Create a date range for the years 2019 through 2024 up to the present date
start_date = "2019-01-01"
end_date = datetime.now().strftime("%Y-%m-%d")
date_range = pd.date_range(start=start_date, end=end_date)

# Create a DataFrame with year, date in YYYYMMDD format, day type (weekday/weekend), and US holidays
daily_df = pd.DataFrame()
daily_df["date"] = date_range

# Add year column
daily_df["year"] = daily_df["date"].dt.year

# Format date as YYYYMMDD
daily_df["date"] = daily_df["date"].dt.strftime("%Y%m%d")

# Add is_weekday and is_weekend columns
daily_df["is_weekday"] = daily_df["date"].astype(str).apply(lambda x: datetime.strptime(x, "%Y%m%d").weekday() < 5)
daily_df["is_weekend"] = ~daily_df["is_weekday"]

# Add is_holiday attribute
cal = USFederalHolidayCalendar()
holidays = cal.holidays(start=start_date, end=end_date)
daily_df["is_holiday"] = daily_df["date"].astype(str).apply(lambda x: datetime.strptime(x, "%Y%m%d") in holidays)

# Save to CSV
output_file = os.path.join(output_dir, "weekends_weekdays_holidays_2019_2024.csv")
daily_df.to_csv(output_file, index=False)

print(f"Daily dataframe saved to {output_file}")
