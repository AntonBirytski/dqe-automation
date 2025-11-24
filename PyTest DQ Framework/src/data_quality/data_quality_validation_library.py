import pandas as pd


class DataQualityLibrary:
    """
    A library of static methods for performing data quality checks on pandas DataFrames.

    This class is intended to be used in a PyTest-based testing framework to validate
    the quality of data in DataFrames. Each method performs a specific data quality
    check and uses assertions to ensure that the data meets the expected conditions.
    """

    @staticmethod
    def check_duplicates(df: pd.DataFrame, column_names=None):
        """
        Check for duplicated rows in the entire Dataframe or duplicated values by columns.
        """
        if column_names:
            duplicates = df[df[column_names].duplicated(keep=False)]
        else:
            duplicates = df[df.duplicated(keep=False)]
        assert duplicates.empty, f"Duplicated rows:\n{duplicates}"

    @staticmethod
    def check_count(df1: pd.DataFrame, df2: pd.DataFrame, df1_name: str, df2_name: str):
        """
        Check that two Dataframes have the same number of rows.
        """
        count_mismatch = len(df1) - len(df2)
        assert count_mismatch == 0, (
            f"Row count mismatch: {df1_name} has {len(df1)} rows, "
            f"{df2_name} has {len(df2)} rows (difference: {count_mismatch}).")

    @staticmethod
    def check_data_completeness(df1: pd.DataFrame, df2: pd.DataFrame):
        """
        Check that two Dataframes contain the same data (ignor order).
        """
        assert set(df1.columns) == set(df2.columns), (
            f"Column mismatch between Dataframes.\n"
            f"df1 columns: {sorted(df1.columns.tolist())}\n"
            f"df2 columns: {sorted(df2.columns.tolist())}")

        df1_sorted = df1.sort_values(by=df1.columns.tolist()).reset_index(drop=True)
        df2_sorted = df2.sort_values(by=df2.columns.tolist()).reset_index(drop=True)
        df_diff = pd.concat([df1_sorted, df2_sorted]).drop_duplicates(keep=False)
        assert df_diff.empty, f"Source to target completeness violation. Differences:\n{df_diff}"

    @staticmethod
    def check_dataset_is_not_empty(df: pd.DataFrame):
        """
        Check if the Dataframe is not empty.
        """
        assert not df.empty, "Dataframe is empty."

    @staticmethod
    def check_not_null_values(df: pd.DataFrame, column_names=None):
        """
        Check if columns in the Dataframe contain null values. If column_names is None, check all columns.
        """
        column_names_with_nulls = []
        for column_name in column_names:
            if df[column_name].isnull().any():
                column_names_with_nulls.append(column_name)
        assert not column_names_with_nulls, f"Null values found in column(s): {', '.join(column_names_with_nulls)}"

    @staticmethod
    def check_column_rules(df: pd.DataFrame, column_rules: dict) -> pd.DataFrame:
        """
        Validate Dataframe columns based on user-defined rules.
        """
        invalid_rows_list = []

        for column, rules in column_rules.items():

            if column not in df.columns:
                raise ValueError(f"Column '{column}' not found in DataFrame.")

            # mask for this column
            invalid_mask = pd.Series(False, index=df.index)

            if "min" in rules:
                invalid_mask |= df[column] < rules["min"]

            if "max" in rules:
                invalid_mask |= df[column] > rules["max"]

            if "expected_values" in rules:
                invalid_mask |= ~df[column].isin(rules["expected_values"])

            if "condition" in rules:
                try:
                    valid_condition = df.apply(rules["condition"], axis=1)
                except Exception as e:
                    raise ValueError(f"Error evaluating custom condition for '{column}': {e}")
                invalid_mask |= ~valid_condition

            if invalid_mask.any():
                bad = df.loc[invalid_mask].copy()
                bad["invalid_column"] = column
                invalid_rows_list.append(bad)

        # always define invalid_df
        invalid_df = (
            pd.concat(invalid_rows_list)
            if invalid_rows_list
            else pd.DataFrame(columns=[*df.columns, "invalid_column"])
        )

        assert invalid_df.empty, (
            "Invalid data detected:\n"
            f"{invalid_df}\n"
            f"(Total {len(invalid_df)} invalid row(s) across "
            f"{invalid_df['invalid_column'].nunique() if not invalid_df.empty else 0} column(s))"
        )

        return invalid_df
