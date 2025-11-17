import pytest
import re


def test_file_not_empty(data_df):
    assert not data_df.empty, f"Dataset is empty."


@pytest.mark.validate_csv
def test_validate_schema(validate_schema, data_df):
    actual_schema = list(data_df.columns)
    expected_schema = ['id', 'name', 'age', 'email', 'is_active']
    is_valid = validate_schema(actual_schema, expected_schema)
    assert is_valid, (f"Schema validation failed:"
                      f"\nExpected schema: {expected_schema}"
                      f"\nActual schema: {actual_schema}")


@pytest.mark.validate_csv
@pytest.mark.skip(reason="Test is temporarily disabled.")
def test_age_column_valid(data_df):
    invalid_ages = data_df[(data_df['age'] < 0) | (data_df['age'] > 100)]
    assert invalid_ages.empty, f"Invalid age values found:\n{invalid_ages}"


@pytest.mark.validate_csv
def test_email_column_valid(data_df):
    email_pattern = re.compile(r"^[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9-]+\.[A-Za-z0-9-.]+$")
    invalid_emails = data_df[~data_df['email'].astype(str).str.match(email_pattern, na=False)]
    assert invalid_emails.empty, f"Invalid email found:\n{invalid_emails}"


@pytest.mark.validate_csv
@pytest.mark.xfail(reason="Known issue")
def test_duplicates(data_df):
    duplicates = data_df[data_df.duplicated(keep=False)]
    assert duplicates.empty, f"Duplicated rows:\n{duplicates}"


@pytest.mark.validate_csv
@pytest.mark.parametrize("id, expected_is_active", [(1, False), (2, True)])
def test_active_players(id, expected_is_active, data_df):
    row = data_df[data_df['id'] == id]
    mismatched = row[row['is_active'] != expected_is_active]
    if not mismatched.empty:
        for _, rec in mismatched.iterrows():
            mismatch_list = [f"{rec['is_active']}"]
            mismatch_str = ", ".join(mismatch_list)
            assert False, (f"Incorrect 'is_active' attribute for player with id = {id}:"
                           f"\nexpected: {expected_is_active}, actual: {mismatch_str}")


def test_active_player(data_df):
    verified_id = 2
    expected_is_active = True
    row = data_df[data_df['id'] == verified_id]
    mismatched = row[row['is_active'] != expected_is_active]
    assert False, (f"Incorrect 'is_active' attribute for player with id = {verified_id}:"
                   f"\nexpected: {expected_is_active}, actual: {mismatched['is_active'][1]}")
