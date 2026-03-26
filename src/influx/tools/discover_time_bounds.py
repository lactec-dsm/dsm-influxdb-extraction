import os
import pandas as pd
from src.influx.connection import get_influx_client

PLAN_PATH = "data/exports/extraction_plan.csv"
OUTPUT_PATH = "data/exports/measurement_time_bounds_A11_A12_A13_A15_A18.csv"


def read_csv_flexible(path: str, nrows: int | None = None) -> pd.DataFrame:
    """
    Lê CSV tentando vírgula e ponto-e-vírgula.
    Permite limitar a quantidade de linhas lidas com nrows.
    """
    df = pd.read_csv(path, nrows=nrows)

    if "measurement_id" in df.columns:
        return df

    df = pd.read_csv(path, sep=";", nrows=nrows)

    df = df[df["active"] == True]

    if "measurement_id" in df.columns:
        return df

    raise ValueError(
        f"Nao foi possivel identificar as colunas esperadas no arquivo {path}. "
        f"Colunas encontradas: {list(df.columns)}"
    )

def build_device_tag(device_id: str) -> str:
    """
    Monta o valor da tag deviceId no formato esperado pelo Influx.
    Se já vier em formato URL, mantém.
    """
    device_id = str(device_id).strip()

    if device_id.startswith("https://react2020.eu/device/"):
        return device_id

    return f"https://react2020.eu/device/{device_id}"


def get_time_bounds_from_plan(plan_df: pd.DataFrame, field: str = "value") -> list[dict]:
    client = get_influx_client()
    rows = []

    # remove duplicidade para não consultar a mesma combinação várias vezes
    plan_df = (
        plan_df[["measurement_id", "device_id","measurement_index", "unit"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    for _, row in plan_df.iterrows():
        measurement_id = str(row["measurement_id"]).strip()
        device_id = str(row["device_id"]).strip()
        device_tag = build_device_tag(device_id)
        measurement_index = str(row["measurement_index"])
        #print(measurement_id)
        #print(device_id)
        #print(device_tag)
        unit = "" if pd.isna(row["unit"]) else str(row["unit"]).strip()

        query_min = (
            f'SELECT first("{field}") AS first_val '
            f'FROM "{measurement_id}" '
            f'WHERE "deviceId" = \'{device_tag}\' '
            f'AND "measurementIndex" = \'{measurement_index}\''
        )

        query_max = (
             f'SELECT last("{field}") AS last_val '
             f'FROM "{measurement_id}" '
             f'WHERE "deviceId" = \'{device_tag}\' '
             f'AND "measurementIndex" = \'{measurement_index}\''
        )

        print(f"[RUN] measurement={measurement_id} / deviceId={device_tag} / measurementIndex={measurement_index}")

        result_min = client.query(query_min)
        result_max = client.query(query_max)

        points_min = list(result_min.get_points())
        points_max = list(result_max.get_points())

        if not points_min or not points_max:
             print(f"[INFO] Nenhum dado para {measurement_id} / {device_tag} / {measurement_index}. Pulando.")
             rows.append({
                 "measurement_id": measurement_id,
                 "device_id": device_id,
                 "device_tag": device_tag,
                 "measurement_index": measurement_index,
                 "unit": unit,
                 "time_min": None,
                 "time_max": None
             })
             continue

        time_min = points_min[0].get("time")
        time_max = points_max[0].get("time")

        rows.append({
             "measurement_id": measurement_id,
             "device_id": device_id,
             "device_tag": device_tag,
             "measurement_index": measurement_index,
             "unit": unit,
             "time_min": time_min,
             "time_max": time_max
         })

    return rows

def export_time_bounds():
    plan_df = read_csv_flexible(PLAN_PATH, nrows=50)
    bounds = get_time_bounds_from_plan(plan_df, field="value")
    df_out = pd.DataFrame(bounds)
    df_out.to_csv(OUTPUT_PATH, index=False)
    print(f"\n[OK] Arquivo salvo em: {OUTPUT_PATH}")

if __name__ == "__main__":
    export_time_bounds()