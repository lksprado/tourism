import glob
import pandas as pd
import chardet


def detect_encoding(file_path):
    with open(file_path, "rb") as f:
        result = chardet.detect(f.read())
    return result["encoding"]


if __name__ == "__main__":

    file = "data/raw/restaurantescafeteriasebares201802trimestrecadasturpj-2018-Q2.csv"

    encoding = detect_encoding(file)
    print(f"File: {file} | Currently Encoding: {encoding}")
    
    # Use glob to find all CSV files in the data/ directory
    # csv_files = glob.glob("data/*.csv")
    # Iterate over each file and print its encoding
    # for file in csv_files:
    #     encoding = detect_encoding(file)
    #     print(f"File: {file} | Encoding: {encoding}")

    df = pd.read_csv(file, sep=';', encoding='latin1') 
    print(df.head())


