# SCRIPT IS JUST FOR PRACTICE. IT is easier to automate the first schema than write from zero

import pandas as pd
import pandera as pa
from pandera import Check, Column, DataFrameSchema


def get_schema(path: str) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name="Sheet1", engine=None)
    return df


def main() -> None:
    path = "data/restaurante-bares-cafeterias-e-similares-2023-Q2.xls"

    df = get_schema(path)

    file_inferred_schema = pa.infer_schema(df)

    with open("schema/infered_schema.py", "w") as file:
        file.write(file_inferred_schema.to_script())


if __name__ == "__main__":
    main()
