import os, sys, re
import pandas as pd

from src.influx.connection import get_influx_client
from src.influx.query_builder import build_query
from src.influx.extractor import extract_data
from src.storage.file_writer import save_to_csv, save_to_parquet

EXTRACTION_PLAN_PATH = "data/exports/extraction_plan.csv"

def sanitize_device_id(device_id):
    # Remove protocolo e substitui caracteres problemáticos por underscore
    safe_id = re.sub(r'^https?://', '', device_id)           # remove http:// ou https://
    safe_id = re.sub(r'[^a-zA-Z0-9_\-]', '_', safe_id)        # substitui caracteres inválidos
    return safe_id

def normalize_bool(value) -> bool:
    if pd.isna(value):
        return False

    value_str = str(value).strip().lower()
    if value_str in {'true', '1', 'yes', 'y'}:
        return True
    
    if value_str in {'false', '0', 'no', 'n'}:
        return False
    
    raise ValueError(f"Valor booleano active: {value}")

def read_csv_flexible(path: str) -> pd.DataFrame:
    """
    Lê CSV tentando primeiro vírgula e depois ponto-e-vírgula.
    """
    df_comma = pd.read_csv(path)

    if "measurement_id" in df_comma.columns:
        return df_comma

    df_semicolon = pd.read_csv(path, sep=";")

    if "measurement_id" in df_semicolon.columns:
        return df_semicolon

    raise ValueError(
        "Nao foi possivel identificar o separador do extraction_plan.csv.\n"
        f"Colunas encontradas com virgula: {list(df_comma.columns)}\n"
        f"Colunas encontradas com ponto-e-virgula: {list(df_semicolon.columns)}"
    )

def validate_plan(df: pd.DataFrame) -> None:
    expected_columns = {"active", "measurement_id", "device_id", "unit", "measurement_index", "time_min", "time_max", "output_format"}

    missing_columns = expected_columns.difference(df.columns)
    if missing_columns:
        raise ValueError(f"Colunas faltando no plano de extração: {sorted(missing_columns)}"
                         f"\nColunas encontradas: {sorted(df.columns)}"
                         )
def build_device_tag(device_id: str) -> str:

    """
        Converte o device_id interno para o formato esperado pela tag 'deviceId' no Influx.
        Se o device_id já estiver no formato completo (começando com "https://react2020.eu/device/"), retorna como está.
    """
    device_id = str(device_id).strip()
    
    if device_id.startswith("https://react2020.eu/device/"):
        return device_id
    
    return f"https://react2020.eu/device/{device_id}"


def run_pipeline(measurement_id: str, device_id: str, unit: str, measurement_index: str, start: str, end: str, fmt: str = "csv") -> None:
    client = get_influx_client()
    
    device_tag = build_device_tag(device_id)
    query = build_query(measurement_id, device_tag, measurement_index, start, end)
    
    df = extract_data(client, query)

    if df.empty:
        print(f"[INFO]Nenhum dado retornado para {measurement_id} / {device_id} / {measurement_index} entre {start} e {end}")
        return

    df['unit'] = unit    
    df['time'] = pd.to_datetime(df['time'], format='ISO8601', errors='coerce')
    df = df.dropna(subset=['time'])
    
    if df.empty:
        print(
            f"[INFO] Dados retornados sem coluna de tempo valida para "
            f"{measurement_id} / {device_tag} / {measurement_index}"
        )
        return    
    
    # Adicionando colunas auxiliares para particionamento
    df['year'] = df['time'].dt.year
    df['month'] = df['time'].dt.month

    safe_device_id = sanitize_device_id(device_id)

    for (year, month), group_df in df.groupby(['year', 'month']):        
        dir_path = (f"data/raw/{measurement_id}/{safe_device_id}/{measurement_index}/year={year}/month={month:02d}/")
        
        os.makedirs(dir_path, exist_ok=True)

        file_name = f"{measurement_id}_{safe_device_id}_{measurement_index}_{year}{month:02d}.{fmt}"
        file_path = os.path.join(dir_path, file_name)
        
        output_df = group_df.drop(columns=['year', 'month'])

        if fmt == "parquet":
            save_to_parquet(output_df, file_path)
        elif fmt == "csv":
            save_to_csv(output_df, file_path)
        else:
            raise ValueError(f"Formato não suportado: {fmt}")

        print(f" {measurement_id}/{device_id}/{measurement_index} ({year}-{month:02d}): {len(group_df)} registros salvos em {file_path}")

def run_from_extraction_plan(path: str = EXTRACTION_PLAN_PATH) -> None:
    df = read_csv_flexible(path)
    validate_plan(df)

    df["active"] = df["active"].apply(normalize_bool)
    df = df[df['active']].copy()
    
    if df.empty:
        print(f"[INFO] O plano de extração está vazio: {path}")
        return
    
    for idx, row in df.iterrows():
        try:
            measurement_id = row['measurement_id'].strip()
            device_id = row['device_id'].strip()
            unit = "" if pd.isna(row['unit']) else str(row['unit']).strip()
            measurement_index = row['measurement_index']
            start = str(row['time_min']).strip()
            end = str(row['time_max']).strip()
            fmt = str(row['output_format']).strip().lower() if not pd.isna(row['output_format']) else "csv"

            print(
                f"[RUN] Linha {idx + 1}: "
                f"measurement_id={measurement_id}, "
                f"device_id={device_id}, "
                f"measurement_index={measurement_index}, "
                f"start={start}, end={end}, fmt={fmt}"
            )

            run_pipeline(
                measurement_id=measurement_id,
                device_id=device_id,
                unit=unit,
                measurement_index=measurement_index,
                start=start,
                end=end,
                fmt=fmt,
            )

        except Exception as e:
            print(f"[ERRO] Falha na linha {idx + 1}: {e}")
            sys.exit(1)
            
if __name__ == "__main__":
    run_from_extraction_plan()

