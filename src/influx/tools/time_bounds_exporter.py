import os
import pandas as pd
from src.influx.connection import get_influx_client

def get_time_bounds(measurements, device_ids, units, field="value"):
    client = get_influx_client()
    rows = []

    for measurement, device_id, unit in zip(measurements, device_ids, units):
        query_min = (
            f'SELECT first("{field}") AS first_val '
            f'FROM "{measurement}" '
            f'WHERE "deviceId" = \'{device_id}\''
        )

        query_max = (
            f'SELECT last("{field}") AS last_val '
            f'FROM "{measurement}" '
            f'WHERE "deviceId" = \'{device_id}\''
        )

        result_min = client.query(query_min)
        result_max = client.query(query_max)

        points_min = list(result_min.get_points())
        points_max = list(result_max.get_points())

        if not points_min or not points_max:
            print(f" Nenhum dado para {measurement} / {device_id}")
            continue

        time_min = points_min[0].get('time')
        time_max = points_max[0].get('time')

        rows.append({
            "measurement": measurement,
            "deviceId": device_id,
            "unit": unit,
            "time_min": time_min,
            "time_max": time_max
        })

    return rows


def export_time_bounds(measurements, device_ids, units, output_path):
    bounds = get_time_bounds(measurements, device_ids, units)
    df = pd.DataFrame(bounds)

    if df.empty:
        print(" Nenhum dado exportado.")
        return

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f" Exportação finalizada: {output_path}")

if __name__ == "__main__":
    measurements = ["stateBat", 
                    "eInBat", 
                    "eOutBat",
                    "iBat",
                    "nCycles",
                    "sOc",
                    "sOh",
                    "tBat",
                    "vBat"]
    
    device_ids = ["https://react2020.eu/device/VIC-GXHQ20503L3SA-100", 
                  "https://react2020.eu/device/VIC-GXHQ20503L3SA-1",
                  "https://react2020.eu/device/VIC-GXHQ20503L3SA-1",
                  "https://react2020.eu/device/VIC-GXHQ20503L3SA-1",
                  "https://react2020.eu/device/VIC-GXHQ20503L3SA-1",
                  "https://react2020.eu/device/VIC-GXHQ20503L3SA-1",
                  "https://react2020.eu/device/VIC-GXHQ20503L3SA-1",
                  "https://react2020.eu/device/VIC-GXHQ20503L3SA-1",
                  "https://react2020.eu/device/VIC-GXHQ20503L3SA-1"]

    units = ["", 
            "Wh",
            "Wh",
            "A",
            "",
            "",
            "",
            "DEG_C",
            "V"]
    
    output_path = "data/exports/time_bounds.csv"

    export_time_bounds(measurements, device_ids, units, output_path)
