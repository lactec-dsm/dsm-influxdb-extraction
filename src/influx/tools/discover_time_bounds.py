import os
import pandas as pd

from src.influx.connection import get_influx_client


PLAN_PATH = "data/exports/extraction_plan.csv"
OUTPUT_PATH = "data/exports/measurement_time_bounds.csv"


def read_plan_csv(path: str) -> pd.DataFrame:
    """
    Lê o extraction_plan.csv tentando vírgula e ponto-e-vírgula.
    """
    df = pd.read_csv(path)

    if "measurement_id" in df.columns:
        return df

    df = pd.read_csv(path, sep=";")

    if "measurement_id" in df.columns:
        return df

    raise ValueError(
        "Não foi possível encontrar a coluna 'measurement_id' no extraction_plan.csv. "
        f"Colunas encontradas: {list(df.columns)}"
    )


def build_min_max_query(measurement_id: str, device_id: str) -> str:
    """
    Monta a query para descobrir o menor e maior timestamp disponíveis.
    """
    return f"""
    SELECT
        FIRST(value) AS first_value,
        LAST(value) AS last_value
    FROM "{measurement_id}"
    WHERE "deviceId" = '{device_id}'
    """


def discover_time_bounds() -> None:
    client = get_influx_client()
    plan = read_plan_csv(PLAN_PATH)

    expected_columns = {"measurement_id", "device_id", "unit"}
    missing = expected_columns.difference(plan.columns)

    if missing:
        raise ValueError(f"Colunas ausentes no extraction_plan.csv: {sorted(missing)}")

    # Remove duplicidades para não consultar o mesmo par várias vezes
    plan_unique = (
        plan[["measurement_id", "device_id", "unit"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    results = []

    for _, row in plan_unique.iterrows():
        measurement_id = str(row["measurement_id"]).strip()
        device_id = str(row["device_id"]).strip()
        unit = "" if pd.isna(row["unit"]) else str(row["unit"]).strip()

        print(f"[RUN] {measurement_id} / {device_id}")

        # menor data
        query_min = f'''
        SELECT * FROM "{measurement_id}"
        WHERE "deviceId" = '{device_id}'
        ORDER BY time ASC
        LIMIT 1
        '''

        # maior data
        query_max = f'''
        SELECT * FROM "{measurement_id}"
        WHERE "deviceId" = '{device_id}'
        ORDER BY time DESC
        LIMIT 1
        '''

        result_min = client.query(query_min)
        result_max = client.query(query_max)

        points_min = list(result_min.get_points())
        points_max = list(result_max.get_points())

        time_min = points_min[0]["time"] if points_min else None
        time_max = points_max[0]["time"] if points_max else None

        results.append(
            {
                "measurement_id": measurement_id,
                "device_id": device_id,
                "unit": unit,
                "time_min": time_min,
                "time_max": time_max,
            }
        )

    df_out = pd.DataFrame(results)

    os.makedirs("data/exports", exist_ok=True)
    df_out.to_csv(OUTPUT_PATH, index=False)

    print(f"\n[OK] Arquivo salvo em: {OUTPUT_PATH}")


if __name__ == "__main__":
    discover_time_bounds()