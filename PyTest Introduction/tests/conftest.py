import pytest
import pandas as pd
import os


# Fixture to read the CSV file
@pytest.fixture(scope="session", params=["src/data/data.csv"])
def csv_file_path(request):
    """Fixture to returns absolute CSV file path"""
    base_dir = os.path.dirname(os.path.dirname(__file__))
    csv_path = os.path.join(base_dir, request.param)
    return csv_path


@pytest.fixture(scope="session")
def data_df(csv_file_path):
    """Fixture to load CSV into a dataframe"""
    df = pd.read_csv(csv_file_path)
    return df


# Fixture to validate the schema of the file
@pytest.fixture(scope="session")
def validate_schema():
    """Fixture to validate schema of a dataframe"""
    def _validate_schema(actual_schema, expected_schema):
        if type(actual_schema) != type(expected_schema):
            raise TypeError(f"Schema type mismatch: actual={type(actual_schema)}, expected={type(expected_schema)}")

        missing = set(expected_schema) - set(actual_schema)
        extra = set(actual_schema) - set(expected_schema)

        if missing or extra:
            print(f"Missing columns: {missing}, Extra columns: {extra}")
            return False

        return True

    return _validate_schema


# Pytest hook to mark unmarked tests with a custom mark
def pytest_collection_modifyitems(items):
    """Hook to mark tests that have no explicit marks as 'unmarked'"""
    for item in items:
        if not item.own_markers:
            item.add_marker(pytest.mark.unmarked)
            print(f"[INFO] Automatically marked as unmarked: {item.name}")
