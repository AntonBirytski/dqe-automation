import time
import os
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By


def read_table(table, filter_visit_date=None):
    all_columns = table.find_elements(By.CLASS_NAME, "y-column")

    headers = []
    rows = []

    for columns in all_columns:
        # Extract header text
        header = columns.find_element(By.ID, "header").text.strip()
        headers.append(header)

        # Extract all cell values inside particular column
        cell_data = columns.find_elements(By.CLASS_NAME, "cell-text")
        cell_value = [value.text.strip() for value in cell_data if value.text.strip() != header]
        rows.append(cell_value)

    df = pd.DataFrame({headers[i]: rows[i] for i in range(len(headers))})

    # Apply filtering on dataframe
    if filter_visit_date is not None:
        df = df[df["Visit Date"] == filter_visit_date]

    return print(df)


html_file_path = os.path.abspath("src/report.html")

driver = webdriver.Chrome()
driver.maximize_window()
time.sleep(2)
driver.get(f"file:///{html_file_path}")
read_table(driver, "2025-11-01")

driver.quit()

##############################################################################

def read_parquet(path, filter_visit_date=None):
    df = pd.read_parquet(path)
    column = ['partition_date']
    df = df.drop(columns=column)
    if filter_visit_date is not None:
        df = df[df["visit_date"] == filter_visit_date]
    return print(df)


read_parquet(r"C:\tmp\parquet_data\facility_type_avg_time_spent_per_visit_date", "2025-11-01")


def compare_dataframes(df1, df2):
    df1_sorted = df1.sort_values(by=df1.columns.tolist()).reset_index(drop=True)
    df2_sorted = df2.sort_values(by=df2.columns.tolist()).reset_index(drop=True)
    try:
        if df1_sorted.shape != df2_sorted.shape:
            return False, f"Shape mismatch: df1={df1_sorted.shape}, df2={df2_sorted.shape}"

        comparison = df1_sorted.eq(df2_sorted)

        if comparison.all().all():
            return True, "✅ HTML table and parquet data are equal"
    except AssertionError as e:
        # Differences will be described in the AssertionError message
        return False, f"❌ HTML table and parquet data are NOT equal: {str(e)}"


# ${parquet_df} = Read
# Parquet    ${PARQUET_FOLDER}    ${FILTER_DATE}

