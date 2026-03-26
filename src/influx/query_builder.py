def build_query(
    measurement_id: str,
    device_id: str,
    measurement_index: str,
    start: str,
    end: str,
) -> str:
    """
    Monta a query de extracao no InfluxDB.

    Parametros internos do projeto:
    - measurement_id
    - device_id
    - measurement_index

    Nome real da tag no Influx:
    - deviceId
    """
    query = (
        f'SELECT * '
        f'FROM "{measurement_id}" '
        f'WHERE "deviceId" = \'{device_id}\' '
        f"AND \"measurementIndex\" = '{measurement_index}' "
        f"AND time >= '{start}' "
        f"AND time <= '{end}' "
        f'ORDER BY time ASC'
    )

    return query