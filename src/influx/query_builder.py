def build_query(measurement_id, device_Id, start, end, fields="*", limit=1000000):
    return (
        f'SELECT {fields} FROM "{measurement_id}" '
        f'WHERE "deviceId" = \'{device_Id}\' AND time >= \'{start}\' AND time <= \'{end}\' '
        f'ORDER BY time ASC '
    )
