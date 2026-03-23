def build_query(
    measurement_id: str,
    device_id: str,
    start: str,
    end: str,
) -> str:
    """
    Monta a query de extracao no InfluxDB.

    Parametros internos do projeto:
    - measurement_id
    - device_id

    Nome real da tag no Influx:
    - deviceId
    """
    query = (
        f'SELECT * '
        f'FROM "{measurement_id}" '
        f'WHERE "deviceId" = \'{device_id}\' '
        f"AND time >= '{start}' "
        f"AND time <= '{end}' "
        f'ORDER BY time ASC'
    )

    return query