import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from dotenv import load_dotenv
from src.influx.connection import get_influx_client
from src.influx.query_builder import build_query
from src.influx.extractor import extract_data
from src.storage.file_writer import save_to_csv, save_to_parquet
from datetime import datetime, timedelta


load_dotenv()

def run_pipeline(measurement, deviceId, start, end, fmt="csv", output_path=None):
    client = get_influx_client()
    query = build_query(measurement, deviceId, start, end)
    df = extract_data(client, query)

    if df.empty:
        print(" Nenhum dado retornado.")
        return

    if not output_path:
        now = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        output_path = f"data/raw/influx_dump_{now}.{fmt}"
        
    if fmt == "parquet":
        save_to_parquet(df, output_path)
    elif fmt == "csv":
        save_to_csv(df, output_path)
    else:
        raise ValueError(f"Formato não suportado: {fmt}")

    print(f" Dados salvos em: {output_path}")

try:
    # https://react2020.eu/device/VIC-GXHQ20503L3SA-1,2022-02-24T14:13:10.050000Z,2025-10-21T17:07:50.014000Z
    run_pipeline("nCycles","https://react2020.eu/device/VIC-GXHQ20503L3SA-1", "2022-02-24T14:13:10.050000Z", "2022-02-25T14:13:10.050000Z")
except Exception as e:
    print(f" Erro ao executar o pipeline: {e}")
    sys.exit(1)