import pandas as pd
import os

def silver_to_gold(silver_path: str, gold_path: str):
    files = [os.path.join(silver_path, f) for f in os.listdir(silver_path) if f.endswith(".parquet")]
    if not files:
        raise FileNotFoundError(f"Nenhum arquivo encontrado no diretório {silver_path}")

    dfs = [pd.read_parquet(file) for file in files]
    data = pd.concat(dfs)
    
    aggregated = data.groupby(["state_province", "brewery_type"]).size().reset_index(name="count")

    os.makedirs(gold_path, exist_ok=True)
    aggregated.to_parquet(os.path.join(gold_path, "aggregated.parquet"))