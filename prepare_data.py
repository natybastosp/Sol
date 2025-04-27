import pandas as pd
import json
import os
import zipfile

# Pasta onde estão os arquivos CSV e ZIP
INPUT_FOLDER = 'dataset/'    # <-- onde estão os arquivos originais
OUTPUT_FOLDER = 'database/'  # <-- onde vamos salvar agora (mudamos para 'database/')

# Criar pasta de saída se não existir
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Definir quais arquivos vão para JSONL
jsonl_targets = [
    'musicas_perifericas_emocoes.csv',  # arquivos que devem virar .jsonl
]

# 1. Converter todos CSVs
for filename in os.listdir(INPUT_FOLDER):
    if filename.endswith('.csv'):
        filepath = os.path.join(INPUT_FOLDER, filename)
        print(f"Processando {filename}...")

        df = pd.read_csv(filepath)

        if filename in jsonl_targets:
            # Salvar como JSONL
            jsonl_path = os.path.join(OUTPUT_FOLDER, filename.replace('.csv', '.jsonl'))
            with open(jsonl_path, 'w', encoding='utf-8') as f_jsonl:
                for record in df.to_dict(orient='records'):
                    f_jsonl.write(json.dumps(record, ensure_ascii=False) + '\n')
            print(f"Salvo como JSONL: {jsonl_path}")
        else:
            # Salvar como PARQUET
            parquet_path = os.path.join(OUTPUT_FOLDER, filename.replace('.csv', '.parquet'))
            df.to_parquet(parquet_path)
            print(f"Salvo como PARQUET: {parquet_path}")

# 2. Descompactar o ZIP (GPT Results)
for filename in os.listdir(INPUT_FOLDER):
    if filename.endswith('.zip'):
        zip_path = os.path.join(INPUT_FOLDER, filename)
        output_zip_folder = os.path.join(OUTPUT_FOLDER, 'gpt_result/')
        os.makedirs(output_zip_folder, exist_ok=True)

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(output_zip_folder)
        print(f"Arquivo ZIP extraído para {output_zip_folder}")

print("\n✅ Conversão concluída!")
