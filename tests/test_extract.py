import os
from unittest import mock

import pandas as pd
import pytest

from app.extract import make_df

# Mocked data for Excel and CSV files
mock_excel_data = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
mock_csv_data = pd.DataFrame({"col1": [5, 6], "col2": [7, 8]})


@mock.patch("pandas.read_excel")
@mock.patch("pandas.read_csv")
def test_make_df(mock_read_csv, mock_read_excel):
    # Mocking pandas read_excel and read_csv behavior
    mock_read_excel.return_value = mock_excel_data
    mock_read_csv.return_value = mock_csv_data

    # List of filenames (Excel and CSV)
    folder = [
        "restaurantescafeteriasebares202103trimestrecadasturpj-2021-Q3.csv",
        "restaurante-cafeteria-bar-e-similares-2024-Q3.xlsx",
    ]

    # Run the function
    result_df = make_df(folder)

    # Assert that read_excel was called once (for the .xlsx file)
    assert mock_read_excel.call_count == 1
    # Assert that read_csv was called once (for the .csv file)
    assert mock_read_csv.call_count == 1

    # Ensure that the result dataframe has the expected number of rows (4 rows in total)
    assert result_df.shape[0] == 4

    # Check that the 'Period' column exists in the final DataFrame
    assert "Period" in result_df.columns

    # Ensure the 'Period' column has the correct extracted periods
    assert result_df["Period"].iloc[0] == "2021-Q3"
    assert result_df["Period"].iloc[2] == "2024-Q3"
