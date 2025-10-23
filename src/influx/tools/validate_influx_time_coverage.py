import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from src.influx.connection import get_influx_client
import os


def month_range(start, end):
    current = start.replace(day=1)
    while current <= end:
        yield current
        current += relativedelta(months=1)


def has_data_for_month(client, measurement, device_id, month_start):
    month_end = (month_start + relativedelta(months=1)) - relativedelta(seconds=1)
    
    query = (
        f'SELECT COUNT(*) FROM "{measurement}" '
        f'WHERE "deviceId" = \'{device_id}\' '
        f'AND time >= \'{month_start.strftime("%Y-%m-%dT%H:%M:%SZ")}\' '
        f'AND time <= \'{month_end.strftime("%Y-%m-%dT%H:%M:%SZ")}\' '
        f'LIMIT 1'
    )

    result = client.query(query)
    return any(len(series) > 0 for series in result.raw.get("series", []))


def validate_coverage(csv_path="data/exports/time_bounds.csv"):
    df_control = pd.read_csv(csv_path, parse_dates=["time_min", "time_max"])
    client = get_influx_client()
    results = []

    for _, row in df_control.iterrows():
        measurement = row["measurement"]
        device_id = row["deviceId"]
        start = row["time_min"]
        end = row["time_max"]

        for month_start in month_range(start, end):
            has_data = has_data_for_month(client, measurement, device_id, month_start)
            results.append({
                "measurement": measurement,
                "deviceId": device_id,
                "year": month_start.year,
                "month": month_start.month,
                "file_exists": has_data
            })
            print(f" Verificado: {measurement}/{device_id} {month_start.year}-{month_start.month:02d} => {has_data}")

    df_result = pd.DataFrame(results)
    df_result.sort_values(by=["measurement", "deviceId", "year", "month"], inplace=True)
    os.makedirs("data/exports", exist_ok=True)
    df_result.to_csv("data/exports/influx_time_coverage.csv", index=False)
    print(" Relatório salvo ")


if __name__ == "__main__":
    validate_coverage()
