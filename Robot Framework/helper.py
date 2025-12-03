import pandas as pd
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

    # Apply filtering on dataframe if any
    if filter_visit_date is not None:
        df = df[df["Visit Date"] == filter_visit_date]

    return df


def read_parquet(path, filter_visit_date=None):
    df = pd.read_parquet(path)
    column = ['partition_date']
    df = df.drop(columns=column)

    # Apply filtering on dataframe if any
    if filter_visit_date is not None:
        df = df[df["visit_date"] == filter_visit_date]

    return df


def normalize_dataframe(df):
    # Standardize column names
    col_mapping = {
        "Facility Type": "facility_type",
        "Visit Date": "visit_date",
        "Average Time Spent": "avg_time_spent",
    }
    df = df.rename(columns={col: col_mapping[col] for col in df.columns if col in col_mapping})

    # Keep only the expected columns in order
    expected_cols = ["facility_type", "visit_date", "avg_time_spent"]
    df = df[[c for c in expected_cols if c in df.columns]]

    # Trim string columns
    if "facility_type" in df.columns:
        df["facility_type"] = df["facility_type"].astype(str).str.strip()

    # Convert visit_date to datetime
    if "visit_date" in df.columns:
        df["visit_date"] = pd.to_datetime(df["visit_date"]).dt.date

    # Ensure avg_time_spent is float
    if "avg_time_spent" in df.columns:
        df["avg_time_spent"] = df["avg_time_spent"].astype(float)

    return df


def compare_dataframes(df1, df2):
    # Sort both dataframes identically
    df1_sorted = df1.sort_values(by=df1.columns.tolist()).reset_index(drop=True)
    df2_sorted = df2.sort_values(by=df2.columns.tolist()).reset_index(drop=True)

    # Shape mismatch
    if df1_sorted.shape != df2_sorted.shape:
        return False, (
            f"❌ DF shape mismatch:\n"
            f"   HTML table: {df1_sorted.shape}\n"
            f"   Parquet table: {df2_sorted.shape}"
        )

    # Compare element-wise
    comparison = df1_sorted.eq(df2_sorted)

    if comparison.all().all():
        return True, "✅ HTML table data and Parquet data match"

    # Identify mismatched rows & columns
    diff_rows = comparison.index[~comparison.all(axis=1)].tolist()
    diff_cols = comparison.columns[~comparison.all(axis=0)].tolist()

    # Build readable diff table
    diffs = []
    for row in diff_rows:
        row_diff = {"row": row}
        for col in diff_cols:
            row_diff[col] = {
                "html": df1_sorted.loc[row, col],
                "parquet": df2_sorted.loc[row, col]
            }
        diffs.append(row_diff)

    message = (
        "❌ HTML table data and Parquet data are NOT match\n"
        f"Different rows: {diff_rows}\n"
        f"Different columns: {diff_cols}\n\n"
        f"Detailed mismatches:\n{diffs}"
    )

    return False, message
