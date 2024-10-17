import os
import re
import sys
from datetime import time

import chardet
import pandas as pd
import pandera as pa

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from schema.infered_schema import Report


def detect_encoding(file_path):
    with open(file_path, "rb") as f:
        result = chardet.detect(f.read())
    return result["encoding"]


def detect_separator(file_path, num_lines=5):
    """
    Detect if a CSV file uses a comma or semicolon as a separator.
    """
    enco = detect_encoding(file_path)
    change = {
        "ISO-8859-1": "latin1",
        "Windows-1252": "latin1",
        "UTF-8-SIG": "utf_8_sig",
    }
    encoding = change.get(enco)

    with open(file_path, "r", encoding=encoding) as f:
        sample = [f.readline() for _ in range(num_lines)]
    sample_text = "".join(sample)
    comma_count = sample_text.count(",")
    semicolon_count = sample_text.count(";")
    return ";" if semicolon_count > comma_count else ","


def excel_loader(folder: str) -> pd.DataFrame:
    column_map = {
        "TIPO": "ESTABELECIMENTO",
        "NÚMERO DE INSCRIÇÃO DO CNPJ": "CNPJ",
        "MUNICÍPIO": "MUNICIPIO",
    }
    column_selection = [
        "PERIODO",
        "ESTABELECIMENTO",
        "CNPJ",
        "UF",
        "MUNICIPIO",
        "ESPECIALIDADE",
    ]

    all_df = []
    files = os.listdir(folder)

    for file in files:
        file_path = os.path.join(folder, file)  # Construct full file path

        if file.endswith(".xls") or file.endswith(".xlsx"):
            try:
                df = pd.read_excel(file_path, engine=None, dtype=str)
                df.columns = [x.upper() for x in df.columns]
                df = df.rename(columns=column_map)
                filename = os.path.basename(file)
                date_part = filename.replace(".xls", "").replace("x", "")[-7:]
                df["PERIODO"] = date_part

                df = df[df.columns.intersection(column_selection)]
                
                df['CNPJ'] = df['CNPJ'].replace(r'[./-]', '', regex=True)

                df["CNPJ"] = df["CNPJ"].apply(lambda x: x.zfill(14))

                df["ESPECIALIDADE"] = df["ESPECIALIDADE"].apply(
                    lambda x: " | ".join(sorted(set(x.split("|"))))
                )

                all_df.append(df)

            except FileNotFoundError as e:
                print(f"Error: {e}. The file does not exist.")

    xl_df = pd.concat(all_df, axis=0, ignore_index=True)

    return xl_df


def csv_loader(folder: str) -> pd.DataFrame:
    column_map = {
        "TIPO DE ATIVIDADE": "ESTABELECIMENTO",
        "NÚMERO DE INSCRIÇÃO DO CNPJ": "CNPJ",
        "MUNICÍPIO": "MUNICIPIO",
        "ESPECIALIDADES": "ESPECIALIDADE",
        "LOCALIDADE": "MUNICIPIO",
        "TIPO": "ESTABELECIMENTO",
        "SUBTIPO ATIVIDADE": "ESTABELECIMENTO",
        "ESTRUTURA BÁSICA": "ESTABELECIMENTO"
    }
    column_selection = [
        "PERIODO",
        "ESTABELECIMENTO",
        "CNPJ",
        "UF",
        "MUNICIPIO",
        "ESPECIALIDADE",
    ]

    all_df = []
    files = os.listdir(folder)

    for file in files:
        file_path = os.path.join(folder, file)  # Construct full file path
        if file.endswith(".csv"):
            try:
                enco = detect_encoding(file_path)
                change = {
                    "ISO-8859-1": "latin1",
                    "Windows-1252": "latin1",
                    "UTF-8-SIG": "utf_8_sig",
                }
                encoding = change.get(enco)

                sep = detect_separator(file_path)

                df = pd.read_csv(
                    file_path, encoding=encoding, sep=sep, on_bad_lines="skip", header=0, dtype=str
                )
                df.columns = df.columns.str.strip().str.upper()
                

                df = df.rename(columns=column_map)
                filename = os.path.basename(file)
                date_part = filename.replace(".csv", "")[-7:]
                df["PERIODO"] = date_part

                df = df[df.columns.intersection(column_selection)]

                if 'CNPJ' not in df.columns:
                    print(f"'CNPJ' column missing in file: {file}")
                    continue  # Skip this file

                df['CNPJ'] = df['CNPJ'].replace(r'[./-]', '', regex=True)

                df["CNPJ"] = df["CNPJ"].apply(lambda x: x.zfill(14))

                all_df.append(df)

            except UnicodeDecodeError as e:
                print(f"Encoding error: {e}. Filename: {file}.")

            except FileNotFoundError as e:
                print(f"Error: {e}. The file does not exist.")

    # Check if there are any DataFrames to concatenate
    if not all_df:  # No files were processed
        print("No dataframes to concatenate. Check the folder for files.")
        return pd.DataFrame()  # Return an empty DataFrame
    
    # Combine all dataframes after the loop
    csv_df = pd.concat(all_df, axis=0, ignore_index=True)
    
    csv_df["ESPECIALIDADE"] = csv_df["ESPECIALIDADE"].str.replace(r'[\d./-]', '', regex=True)

    csv_df["ESPECIALIDADE"] = csv_df["ESPECIALIDADE"].apply(
        lambda x: " | ".join(sorted(set(x.split("|")))) if isinstance(x, str) else x
    )

    return csv_df

def grouping(excel_file: str, csv_file: str)-> pd.DataFrame:
    excel = pd.read_csv(excel_file)
    csv = pd.read_csv(csv_file)
    
    single = pd.concat([excel,csv],axis=0, ignore_index=True)
    
    especialidade = single[['CNPJ', 'ESPECIALIDADE']]
    
    especialidade = especialidade[especialidade["ESPECIALIDADE"].str.len() > 3]
    
    especialidade = especialidade[(~especialidade["ESPECIALIDADE"].str.contains("emp", case=False))]
    
    especialidade = especialidade.drop_duplicates(subset=['CNPJ', 'ESPECIALIDADE'])
    
    estabelecimento = single[['CNPJ', 'ESTABELECIMENTO']]
    
    keywords = ["bar", "similar", "restaurante", "cafeteria"]
    
    estabelecimento = estabelecimento[
        estabelecimento['ESTABELECIMENTO'].str.lower().isin([keyword.lower() for keyword in keywords])
    ]
    
    
    estabelecimento = estabelecimento.drop_duplicates(subset=['CNPJ', 'ESTABELECIMENTO'])

    final = especialidade.merge(estabelecimento, how='outer', on="CNPJ")
    
    model = single[['PERIODO', 'CNPJ', 'UF', 'MUNICIPIO']]
    
    model = model.merge(final, how='left', on='CNPJ')
        
    return especialidade, estabelecimento, final, model


# #@pa.check_output(Report, lazy=True)



if __name__ == "__main__":

    folder = "data/"
    # excel_dirt = excel_loader(folder)
    # excel_dirt.to_csv("excel_dirt.csv", index=False)
    csv_dirt = csv_loader(folder)
    csv_dirt.to_csv("csv_dirt.csv", index=False)
    
    excel = 'excel_dirt.csv'
    csv = 'csv_dirt.csv'
    
    especialidade, estabelecimento, final, model = grouping(excel, csv)
    
    especialidade.to_csv("especialidade.csv", index=False)
    estabelecimento.to_csv("estabelecimento.csv", index=False)
    final.to_csv("final.csv", index=False)
    model.to_csv("model.csv", index=False)
    
    