def build_query(measurement, deviceId, start, end, fields="*", limit=1000000):
    return (
        f'SELECT {fields} FROM "{measurement}" '
        f'WHERE deviceId = \'{deviceId}\' AND time >= \'{start}\' AND time <= \'{end}\' '
        f'LIMIT {limit}'
    )
