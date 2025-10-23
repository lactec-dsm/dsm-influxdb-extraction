import os, sys
from src.influx.connection import get_influx_client
from src.influx.query_builder import build_query
from src.influx.extractor import extract_data
from src.storage.file_writer import save_to_csv, save_to_parquet
import pandas as pd

import re

def sanitize_device_id(device_id):
    # Remove protocolo e substitui caracteres problemáticos por underscore
    safe_id = re.sub(r'^https?://', '', device_id)           # remove http:// ou https://
    safe_id = re.sub(r'[^a-zA-Z0-9_\-]', '_', safe_id)        # substitui caracteres inválidos
    return safe_id

def run_pipeline(measurement, deviceId, unit, start, end, fmt="csv", output_path=None):
    client = get_influx_client()
    query = build_query(measurement, deviceId, start, end)
    df = extract_data(client, query)

    if df.empty:
        print(f" Nenhum dado retornado para {measurement} / {deviceId} entre {start} e {end}")
        return

    df['unit'] = unit
    
    df['time'] = pd.to_datetime(df['time'], format='ISO8601', errors='coerce')
    df = df.dropna(subset=['time'])
    
    # Adicionando colunas auxiliares para particionamento
    df['year'] = df['time'].dt.year
    df['month'] = df['time'].dt.month

    for (year, month), group_df in df.groupby(['year', 'month']):
        safe_device_id = sanitize_device_id(deviceId)
        dir_path = f"data/raw/{measurement}/{safe_device_id}/year={year}/month={month:02d}/"
        os.makedirs(dir_path, exist_ok=True)

        file_name = f"{measurement}_{safe_device_id}_{year}{month:02d}.{fmt}"
        file_path = os.path.join(dir_path, file_name)
        
        if fmt == "parquet":
            save_to_parquet(group_df.drop(columns=['year', 'month']), file_path)
        elif fmt == "csv":
            save_to_csv(group_df.drop(columns=['year', 'month']), file_path)
        else:
            raise ValueError(f"Formato não suportado: {fmt}")

        print(f" {measurement}/{deviceId} ({year}-{month:02d}): {len(group_df)} registros salvos em {file_path}")


def run_from_control(csv_path="data/exports/time_bounds.csv"):
    df = pd.read_csv(csv_path, parse_dates=["time_min", "time_max"])
    
    for _, row in df.iterrows():
        measurement = row["measurement"]
        deviceId = row["deviceId"]
        unit = str(row["unit"])
        start =  pd.to_datetime(row["time_min"]).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")
        end = pd.to_datetime(row["time_max"]).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")

        #print(f"Measurement: {measurement}, DeviceId: {deviceId}, Unit: {unit}, Start: {start}, End: {end}")

        try:
            run_pipeline(
            measurement=measurement,
            deviceId=deviceId,
            unit=unit,
            start=start,
            end=end,
            fmt="csv",
            output_path=None  
            )
        except Exception as e:
            print(f" Erro ao executar o pipeline: {e}")
            sys.exit(1)

if __name__ == "__main__":
    run_from_control()

