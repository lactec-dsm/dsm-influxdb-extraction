import pandas as pd
from dateutil.relativedelta import relativedelta
from datetime import datetime
from src.influx.connection import get_influx_client


def has_data_for_month(client, measurement, device_id, start_date, end_date):
    #month_end = (start_date + relativedelta(months=1)) - relativedelta(seconds=1)
    
    query = (
        f'SELECT * FROM "{measurement}" '
        f'WHERE "deviceId" = \'{device_id}\' '
        f'AND time >= \'{start_date}\' '
        f'AND time <= \'{end_date}\' '
    )

    result = client.query(query)
    points = list(result.get_points())
    df = pd.DataFrame(points)
    return df


if __name__ == "__main__":
    client = get_influx_client()
    df_result = has_data_for_month(
        client,
        measurement="stateBat",
        device_id="https://react2020.eu/device/VIC-GXHQ20503L3SA-1",
        start_date="2024-01-01T00:00:00.000000Z",
        end_date="2024-01-31T00:00:00.000000Z"
    )
    print(df_result)
