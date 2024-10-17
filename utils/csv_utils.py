import pandas as pd 
import sys
import os 
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import chardet
import unicodedata

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