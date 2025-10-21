import pandas as pd

def extract_data(client, query):
    result = client.query(query)
    points = list(result.get_points())
    return pd.DataFrame(points)
