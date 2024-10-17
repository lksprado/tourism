import pandas as pd 
import glob 
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import chardet
import re
import unicodedata
from utils.time_tracker import track_time


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

# exploring structure of xls and xlsx files
def exploring_excel_files(folder: str)-> pd.DataFrame: 
    
    all_df = [] 
    
    # column_map = {
    #     "TIPO DE ATIVIDADE": "ESTABELECIMENTO",
    #     "NÚMERO DE INSCRIÇÃO DO CNPJ": "CNPJ",
    #     "MUNICÍPIO": "MUNICIPIO",
    #     "ESPECIALIDADES": "ESPECIALIDADE",
    #     "LOCALIDADE": "MUNICIPIO",
    #     "TIPO": "ESTABELECIMENTO"
    # }
    
    files = os.listdir(folder)
    
    for file in files:
        file_path = os.path.join(folder, file)
        
        if file.endswith(".xls") or file.endswith(".xlsx"):
            try:
                df = pd.read_excel(file_path, engine=None, dtype=str)
                df.columns = [x.upper() for x in df.columns]
#                df = df.rename(columns=column_map)
                filename = os.path.basename(file)
                df['FILENAME'] = filename
                date_part = filename.replace(".xls", "").replace("x", "")[-7:]
                df["PERIODO"] = date_part
                
                move = ['FILENAME','PERIODO']
                order = move + [col for col in df.columns if col not in move]
                
                df = df[order]

                all_df.append(df)
            
            except Exception as e: 
                print(f"Something went wrong with the file: {e}")
    
    xl_df = pd.concat(all_df, axis=0, ignore_index=True)
    
    print("All files processed")

    return xl_df 

def excel_validator(file_path: str) -> pd.DataFrame:

    tipo = ["Similar", "Restaurante", "Bar", "Cafeteria", "-"]
    df = pd.read_csv(file_path, sep = ";", dtype=str, on_bad_lines='skip')
            
    df_cities = pd.read_csv("data/references/municipios.csv", sep = ";", encoding = "latin1" )
    official_ufs = df_cities["UF"].str.strip().unique()

    df["UF"] = df["UF"].str.replace(r"\s+", "", regex=True).str.strip().fillna("")

    valid_df = df[(df["UF"].isin(official_ufs)) & (df["TIPO"].isin(tipo))]
    
    invalid_df = df[~(df["UF"].isin(official_ufs) & df["TIPO"].isin(tipo))]

    return valid_df, invalid_df

def collect_columns_from_csv(directory)->pd.DataFrame:
    # List to store the filename and corresponding columns
    data = []

    # Iterate over all files in the specified directory
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):  # Check if the file is a CSV file
            file_path = os.path.join(directory, filename)

            try:
                enco = detect_encoding(file_path)
                change = {
                    "ISO-8859-1": "latin1",
                    "Windows-1252": "windows-1252"
                }
                encoding = change.get(enco)

                sep = detect_separator(file_path)

                df = pd.read_csv(
                    file_path, encoding=encoding, sep=sep, on_bad_lines="skip", nrows=0, dtype=str
                )
                # Read the CSV file without loading the data, just getting columns
                columns = df.columns.str.strip().str.upper().tolist()  # Get the column names as a list

                # Append the filename and columns to the list
                data.append({'FILENAME': filename, 'COLUMNS': ', '.join(columns)})
            
            except Exception as e:
                print(f"Error processing {filename}: {e}")
    
    # Create a DataFrame from the collected data
    df_output = pd.DataFrame(data)
    
    return df_output

def exploring_csv_files(folder: str)-> pd.DataFrame:
    
    all_df = []
    model_1 = [
        'NÚMERO DE INSCRIÇÃO DO CNPJ',
        'NOME DA PESSOA JURÍDICA',
        'NOME FANTASIA',
        'SITUAÇÃO CADASTRAL',
        'SITUAÇÃO DA ATIVIDADE',
        'TIPO DE ESTABELECIMENTO',
        'NATUREZA JURÍDICA',
        'PORTE',
        'ENDEREÇO COMPLETO RECEITA FEDERAL',
        'UF',
        'MUNICÍPIO',
        'DATA DE ABERTURA',
        'CNAE(S) RELACIONADOS À ATIVIDADE',
        'ENDEREÇO COMPLETO COMERCIAL',
        'TELEFONE COMERCIAL',
        'E-MAIL COMERCIAL',
        'WEBSITE',
        'NÚMERO DO CERTIFICADO',
        'VALIDADE DO CERTIFICADO',
        'TIPO DE ATIVIDADE',
        'ESPECIALIDADE',
        'IDIOMAS'
    ]
    
    model_a_map = {
        'NÚMERO DE INSCRIÇÃO DO CNPJ': 'CNPJ',
        "MUNICÍPIO": "MUNICIPIO",
        "TIPO DE ATIVIDADE":"NEGOCIO",
    }
    
    model_2 = [
        'RAZÃO SOCIAL',
        'NOME FANTASIA',
        'CNPJ',
        'NATUREZA JURIDICA',
        'DATA INICIO OPERAÇÃO',
        'PORTE',
        'SITUAÇÃO',
        'TIPO ATIVIDADE',
        'SUBTIPO ATIVIDADE',
        'CEP',
        'UF',
        'LOCALIDADE',
        'BAIRRO',
        'LOGRADOURO',
        'TELEFONE',
        'FAX',
        'E-MAIL2',
        'E-MAIL3',
        'SITE',
        'CÓDIGO CERTIFICADO',
        'CÓDIGO E DESCRIÇÃO CNAE',
        'CAPACIDADE DE LUGARES',
        'LINGUAS'
    ]
    
    model_b_map = {
        'SUBTIPO ATIVIDADE': "NEGOCIO",
        'LOCALIDADE': 'MUNICIPIO',
    }

    model_3 = [
        'RAZÃO SOCIAL',
        'NOME FANTASIA',
        'CNPJ',
        'PORTE',
        'DATA DE INICIO DA OPERAÇÃO',
        'SITUAÇÃO',
        'ATIVIDADE',
        'CEP',
        'UF',
        'LOCALIDADE',
        'BAIRRO',
        'LOGRADOURO',
        'COMPLEMENTO',
        'TELEFONE',
        'EMAIL INSTITUCIONAL',
        'EMAIL COMERCIAL',
        'SITE',
        'DATA DE GERAÇÃO DO CERTIFICADO',
        'DATA DE VALIDADE DO CERTIFICADO',
        'CÓDIGO DO CERTIFICADO',
        'CNAE',
        'ESTRUTURA BÁSICA',
        'TIPO DE ATIVIDADE',
        'NATUREZA JURÍDICA',
        'LÍNGUAS',
        'CAPACIDADE DE LUGARES'
    ]
    
    model_c_map = {
        'LOCALIDADE': "MUNICIPIO",
        'ESTRUTURA BÁSICA': 'NEGOCIO',
    }

    model_4 = [
        'RAZÃO SOCIAL',
        'NOME FANTASIA',
        'CNPJ',
        'PORTE',
        'DATA DE INICIO DA OPERAÇÃO',
        'SITUAÇÃO',
        'ATIVIDADE',
        'CEP',
        'UF',
        'LOCALIDADE',
        'BAIRRO',
        'LOGRADOURO',
        'COMPLEMENTO',
        'TELEFONE',
        'EMAIL INSTITUCIONAL',
        'EMAIL COMERCIAL',
        'SITE',
        'DATA DE GERAÇÃO DO CERTIFICADO',
        'DATA DE VALIDADE DO CERTIFICADO',
        'CÓDIGO DO CERTIFICADO',
        'CNAE',
        'TIPO DE ATIVIDADE',
        'NATUREZA JURÍDICA',
        'LÍNGUAS',
        'ESPECIALIDADES'
    ]
    
    model_d_map = {
        'LOCALIDADE': "MUNICIPIO",
        'TIPO DE ATIVIDADE': 'NEGOCIO',
        'ESPECIALIDADES': "ESPECIALIDADE"
    }
    
    model_5 = [
        'RAZÃO SOCIAL',
        'NOME FANTASIA',
        'CNPJ',
        'PORTE',
        'DATA DE INICIO DA OPERAÇÃO',
        'SITUAÇÃO',
        'ATIVIDADE',
        'CEP',
        'UF',
        'LOCALIDADE',
        'BAIRRO',
        'LOGRADOURO',
        'COMPLEMENTO',
        'TELEFONE',
        'EMAIL INSTITUCIONAL',
        'EMAIL COMERCIAL',
        'SITE',
        'DATA DE GERAÇÃO DO CERTIFICADO',
        'DATA DE VALIDADE DO CERTIFICADO',
        'CÓDIGO DO CERTIFICADO',
        'CNAE',
        'TIPO DE ATIVIDADE',
        'NATUREZA JURÍDICA',
        'LÍNGUAS',
        'ESPECIALIDADES'
    ]

    model_6 = [
        'RAZÃO SOCIAL',
        'NOME FANTASIA',
        'CNPJ',
        'PORTE',
        'DATA DE INICIO DA OPERAÇÃO',
        'SITUAÇÃO',
        'ATIVIDADE',
        'CEP',
        'UF',
        'LOCALIDADE',
        'BAIRRO',
        'LOGRADOURO',
        'COMPLEMENTO',
        'SITE',
        'DATA DE GERAÇÃO DO CERTIFICADO',
        'DATA DE VALIDADE DO CERTIFICADO',
        'CÓDIGO DO CERTIFICADO',
        'CNAE',
        'TIPO DE ATIVIDADE',
        'NATUREZA JURÍDICA',
        'LÍNGUAS',
        'ESPECIALIDADES',
        'UNNAMED: 22',
        'UNNAMED: 23',
        'UNNAMED: 24'
    ]

    model_7 = [
        'RAZÃO SOCIAL',
        'NOME FANTASIA',
        'CNPJ',
        'PORTE',
        'DATA DE INICIO DA OPERAÇÃO',
        'SITUAÇÃO',
        'ATIVIDADE',
        'CEP',
        'UF',
        'LOCALIDADE',
        'BAIRRO',
        'LOGRADOURO',
        'COMPLEMENTO',
        'SITE',
        'DATA DE GERAÇÃO DO CERTIFICADO',
        'DATA DE VALIDADE DO CERTIFICADO',
        'CÓDIGO DO CERTIFICADO',
        'CNAE',
        'TIPO DE ATIVIDADE',
        'NATUREZA JURÍDICA',
        'LÍNGUAS',
        'ESPECIALIDADES'
    ]
    
    model_8 = [
        'RAZÃO SOCIAL',
        'NOME FANTASIA',
        'CNPJ',
        'PORTE',
        'DATA DE INICIO DA OPERAÇÃO',
        'SITUAÇÃO',
        'ATIVIDADE',
        'CEP',
        'UF',
        'LOCALIDADE',
        'BAIRRO',
        'LOGRADOURO',
        'COMPLEMENTO',
        'TELEFONE',
        'EMAIL INSTITUCIONAL',
        'EMAIL COMERCIAL',
        'SITE',
        'DATA DE GERAÇÃO DO CERTIFICADO',
        'DATA DE VALIDADE DO CERTIFICADO',
        'CÓDIGO DO CERTIFICADO',
        'CNAE',
        'TIPO DE ATIVIDADE',
        'NATUREZA JURÍDICA',
        'LÍNGUAS',
        'ESPECIALIDADES'
    ]

    files = os.listdir(folder)

    
    for file in files:
        file_path = os.path.join(folder, file)  # Construct full file path
        if file.endswith(".csv"):
            try:
                enco = detect_encoding(file_path)
                change = {
                    "ISO-8859-1": "latin1",
                    "Windows-1252": "windows-1252"
                }
                encoding = change.get(enco)

                sep = detect_separator(file_path)

                df = pd.read_csv(
                    file_path, encoding=encoding, sep=sep, on_bad_lines="skip", header=0, dtype=str
                )
                df.columns = df.columns.str.strip().str.upper()
                
                list_schema = df.columns.to_list()
                
                if list_schema == model_1:
                    df = df.rename(columns=model_a_map)
                elif list_schema == model_2:
                    df = df.rename(columns=model_b_map)
                elif list_schema == model_3:
                    df = df.rename(columns=model_c_map)                
                elif list_schema == model_4:
                    df = df.rename(columns=model_d_map)
                elif list_schema == model_5:
                    df = df.rename(columns=model_d_map)
                elif list_schema == model_6:
                    df = df.rename(columns=model_d_map)                    
                elif list_schema == model_7:
                    df = df.rename(columns=model_d_map)
                elif list_schema == model_8:
                    df = df.rename(columns=model_d_map)

                filename = os.path.basename(file)
                df['FILENAME'] = filename
                date_part = filename.replace(".csv", "")[-7:]
                df["PERIODO"] = date_part   
                
                df = df.reset_index(drop=True)
                
                move = ['FILENAME','PERIODO']
                order = move + [col for col in df.columns if col not in move]
                
                df = df[order]

                all_df.append(df)
            
            except Exception as e: 
                print(f"Something went wrong with the file: {e}")
    
    try:
        csv_df = pd.concat(all_df, axis=0, ignore_index=True)
    except pd.errors.InvalidIndexError as e:
        print(f"Error concatenating dataframes: {e}")
        for i, df in enumerate(all_df):
            print(f"Dataframe {i} columns: {df.columns.tolist()}")
        raise

    return csv_df

def csv_validator(file_path: str) -> pd.DataFrame:

    tipo = ["Similar", "Restaurante", "Bar", "Cafeteria", "", "'-", "-"]
    df = pd.read_csv(file_path, sep = ";", dtype=str, on_bad_lines='skip')
    
        
    df_cities = pd.read_csv("data/references/municipios.csv", sep = ";", encoding = "latin1" )
    official_ufs = df_cities["UF"].str.strip().unique()

    df["UF"] = df["UF"].str.replace(r"\s+", "", regex=True).str.strip().fillna("")
    
    df["NEGOCIO"] = df["NEGOCIO"].str.replace(r"\s+", "", regex=True).str.strip().fillna("")

    valid_df = df[(df["UF"].isin(official_ufs)) & (df["NEGOCIO"].isin(tipo))]
    
    invalid_df = df[~(df["UF"].isin(official_ufs) & df["NEGOCIO"].isin(tipo))]

    return valid_df, invalid_df

def excel_valid_ready(file_path: str)-> pd.DataFrame:
    column_map = {
        "TIPO": "NEGOCIO",
        "NÚMERO DE INSCRIÇÃO DO CNPJ": "CNPJ",
        "MUNICÍPIO": "MUNICIPIO",
    }
    column_selection = [
        "PERIODO",
        "NEGOCIO",
        "CNPJ",
        "UF",
        "MUNICIPIO",
        "ESPECIALIDADE",
    ]
    
    df = pd.read_csv(file_path, sep=";",dtype=str )    
    
    df = df.rename(columns=column_map)
    
    df = df[df.columns.intersection(column_selection)]

    df['CNPJ'] = df['CNPJ'].replace(r'[./-]', '', regex=True)
    df["CNPJ"] = df["CNPJ"].apply(lambda x: x.zfill(14))
    df["ESPECIALIDADE"] = df["ESPECIALIDADE"].apply(
        lambda x: " | ".join(sorted(set(x.split("|")))) if isinstance(x, str) else x
    )
    
    df['NEGOCIO'] = df['NEGOCIO'].str.replace("-", "", regex=False)
    df['ESPECIALIDADE'] = df['ESPECIALIDADE'].str.replace("-", "", regex=False) 
    return df 


def normalize_text(text: str) -> str:
    # Normalize the string to NFKD (Compatibility Decomposition)
    if isinstance(text, str):
        return unicodedata.normalize("NFKD", text)
    return text


def csv_valid_ready(file_path: str)-> pd.DataFrame:
    column_selection = [
        "PERIODO",
        "NEGOCIO",
        "CNPJ",
        "UF",
        "MUNICIPIO",
        "ESPECIALIDADE",
    ]

    
    df = pd.read_csv(file_path, sep=";",dtype=str )    
    
    df = df[df.columns.intersection(column_selection)]

    df['CNPJ'] = df['CNPJ'].replace(r'[./-]', '', regex=True)
    df["CNPJ"] = df["CNPJ"].apply(lambda x: x.zfill(14))
    df["ESPECIALIDADE"] = df["ESPECIALIDADE"].apply(
        lambda x: " | ".join(sorted(set(x.split("|")))) if isinstance(x, str) else x
    )

    df['NEGOCIO'] = df['NEGOCIO'].str.replace("-", "", regex=False)
    df['ESPECIALIDADE'] = df['ESPECIALIDADE'].str.replace("-", "", regex=False) 

    return df 



def csv_invalid_model():
    tipo = ["Similar", "Restaurante", "Bar", "Cafeteria"]
    try:
        file = "data/processed/invalid_csv_processed.csv"
        if os.path.isfile(file):
            df = pd.read_csv(file, sep=";")
            df['CNPJ'] = df.apply(lambda row: row['NEGOCIO'] if len(row['CNPJ']) < 12 else row['CNPJ'], axis=1)
            df['UF'] = df.apply(lambda row: row['BAIRRO'] if len(row['UF']) > 2 else row['UF'], axis=1)
            df['MUNICIPIO'] = df.apply(lambda row: row['LOGRADOURO'] if re.search(r'\d', row['MUNICIPIO']) else row['MUNICIPIO'], axis=1)
            df['NEGOCIO'] = df.apply(lambda row: row['LÍNGUAS'] if row['LÍNGUAS'] in tipo else (row['ESPECIALIDADE'] if row['ESPECIALIDADE'] in tipo else None), axis=1)
            df.drop(columns=['ESPECIALIDADE'],inplace=True)
            df['UNNAMED: 23'] = df['UNNAMED: 23'].replace("-","")
            df['UNNAMED: 23'] = df['UNNAMED: 24'].combine_first(df['UNNAMED: 23'])
            df.rename(columns={'UNNAMED: 23': 'ESPECIALIDADE'}, inplace=True)
            
            df = df[['PERIODO', 'UF','MUNICIPIO','NEGOCIO','CNPJ','ESPECIALIDADE']]
            df.to_csv("data/processed/fixed_csv.csv", sep=";", index=False)
            
    except Exception as e:
        print(f"An error occurred: {e}")
        return None            
                    
def final_model()->pd.DataFrame:
    file1 = "data/processed/csv_valid_model.csv"
    file2 = "data/processed/excel_valid_model.csv"
    file3 = "data/processed/fixed_csv.csv"
    
    df1 = pd.read_csv(file1, sep=";")
    df2 = pd.read_csv(file2, sep=";")
    df3 = pd.read_csv(file3, sep=";")
    
    
    df_concat = pd.concat([df1,df2,df3], axis=0, ignore_index=True)
    
    df_negocio = df_concat[['CNPJ','NEGOCIO']]
    
    df_negocio = df_negocio[df_negocio["NEGOCIO"].str.len() >= 3]
    
    df_negocio = df_negocio.drop_duplicates(subset=['CNPJ', 'NEGOCIO'])
    
    df_especialidade = df_concat[['CNPJ', 'ESPECIALIDADE']]
    
    df_especialidade = df_especialidade[df_especialidade["ESPECIALIDADE"].str.len() > 3]
    
    df_especialidade= df_especialidade.drop_duplicates(subset=['CNPJ', 'ESPECIALIDADE'])

    final = df_especialidade.merge(df_negocio, how='outer', on="CNPJ")
    
    model = df_concat[['PERIODO', 'CNPJ', 'UF', 'MUNICIPIO']]
    
    model = model.merge(final, how='left', on='CNPJ')
    
    return model             
        

if __name__=="__main__":
    
    folder = "data/raw/"
    output = "data/processed/"

    
    # file = exploring_excel_files(folder)
    # file.to_csv(f"{output}excel_processed.csv", sep=";", index=False)
    
    # valid_data, invalid_data = excel_validator(f"{output}excel_processed.csv")
    # valid_data.to_csv(f"{output}valid_excel_processed.csv", sep=";", index=False)
    # invalid_data.to_csv(f"{output}invalid_excel_processed.csv", sep=";", index=False)

    # file = exploring_csv_files(folder)
    # file.to_csv(f"{output}csv_processed.csv", sep=";", index=False)    
    # output_csv = 'csv_columns.csv'
    # file = collect_columns_from_csv(folder)
    # file.to_csv(f"{output}csv_columns.csv", index=False)
    
    # valid_data, invalid_data = csv_validator(f"{output}csv_processed.csv")
    # valid_data.to_csv(f"{output}valid_csv_processed.csv",sep=";" ,index=False)
    # invalid_data.to_csv(f"{output}invalid_csv_processed.csv",sep=";", index=False)
    
    # file = excel_valid_ready(f"{output}valid_excel_processed.csv")
    # file.to_csv(f"{output}excel_valid_model.csv", sep=";", index=False)

    # file = csv_valid_ready(f"{output}valid_csv_processed.csv")
    # file.to_csv(f"{output}csv_valid_model.csv", sep=";", index=False)

    #csv_invalid_model()
        
    file = final_model()
    file.to_csv(f"{output}final_valid_model.csv", sep=";", index=False)
    

    