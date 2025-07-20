import pandas as pd
import os

def bronze_to_silver(bronze_path: str, silver_path: str):
    try:
        # Ler o arquivo JSON da camada Bronze
        file_path = os.path.join(bronze_path, "breweries.json")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Arquivo {file_path} não encontrado.")
        
        print(f"Lendo dados do arquivo: {file_path}")
        data = pd.read_json(file_path)
        
        # Validar se a coluna 'state_province' existe nos dados
        if "state_province" not in data.columns:
            raise KeyError("A coluna 'state_province' não foi encontrada nos dados.")
        
        # Transformar os dados e particionar por estado
        os.makedirs(silver_path, exist_ok=True)
        for state, group in data.groupby("state_province"):
            if state:  # Ignorar estados vazios
                file_name = f"{state}.parquet"
                output_path = os.path.join(silver_path, file_name)
                group.to_parquet(output_path)
                print(f"Dados do estado '{state}' salvos em: {output_path}")
                
        print("Transformação Bronze -> Silver concluída com sucesso!")
    
    except Exception as e:
        print(f"Erro ao transformar dados Bronze -> Silver: {e}")
        raise()
    