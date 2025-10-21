from influxdb import InfluxDBClient
from dotenv import load_dotenv
import os
load_dotenv()

def get_influx_client():
    return InfluxDBClient(
        host=os.getenv("INFLUX_HOST"),
        port=int(os.getenv("INFLUX_PORT", 8086)),
        username=os.getenv("INFLUX_USER"),
        password=os.getenv("INFLUX_PASSWORD"),
        database=os.getenv("INFLUX_DATABASE"),
        ssl=True,
        verify_ssl=False
    )
