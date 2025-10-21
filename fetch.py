from pybaseball import statcast
import pandas as pd
from datetime import datetime, timedelta

def fetch_yearly_statcast_data(chunk_days=7):
    today = datetime.today()

    year_ranges = {
        "2023": ("2023-01-01", "2023-12-31"),
        "2024": ("2024-01-01", "2024-12-31"),
        "2025": ("2025-01-01", today.strftime("%Y-%m-%d"))
    }

    for year, (start_date, end_date) in year_ranges.items():
        print(f"Fetching data for {year} from {start_date} to {end_date}...")
        all_data = []
        current_start = datetime.strptime(start_date, "%Y-%m-%d")
        current_end = datetime.strptime(end_date, "%Y-%m-%d")

        while current_start <= current_end:
            chunk_end = current_start + timedelta(days=chunk_days - 1)
            if chunk_end > current_end:
                chunk_end = current_end
            
            print(f"  Fetching from {current_start.date()} to {chunk_end.date()}")
            chunk = statcast(start_dt=current_start.strftime("%Y-%m-%d"),
                             end_dt=chunk_end.strftime("%Y-%m-%d"))
            all_data.append(chunk)
            
            current_start = chunk_end + timedelta(days=1)

        year_data = pd.concat(all_data, ignore_index=True)
        filename = f"pitches_{year}.csv"
        year_data.to_csv(filename, index=False)
        print(f"Saved {filename} with {len(year_data)} rows.\n")

# code used to fetch the data using the py baseball api, can rerun if something is cooekd
