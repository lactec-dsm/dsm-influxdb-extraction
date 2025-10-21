def build_query(measurement, deviceId, start, end, fields="*", limit=10000):
    return (
        f'SELECT {fields} FROM "{measurement}" '
        f'WHERE deviceId = \'{deviceId}\' AND time >= \'{start}\' AND time <= \'{end}\' '
        f'LIMIT {limit}'
    )



# measurement = "stateBat"
# deviceId = "https://react2020.eu/device/VIC-GXHQ20503L3SA-100"
# start = "2023-10-01T00:00:00Z"
# end = "2023-10-01T23:59:59Z"

# def build_query(measurement, deviceId, start, end, fields="*", limit=10000):
#     print (
#         f'SELECT {fields} FROM "{measurement}" '
#         f'WHERE deviceId = \'{deviceId}\' AND time >= \'{start}\' AND time <= \'{end}\' '
#          f'LIMIT {limit}'
#     )

# build_query(measurement,deviceId,start,end)