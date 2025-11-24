"""
Description: Data Quality checks for facility_name_min_time_spent_per_visit_date file
Requirement(s): TICKET-1
Author(s): Anton Birytski
"""

import pytest


@pytest.fixture(scope='module')
def source_data(db_connection):
    source_query = """
    select
        f.facility_name,
        v.visit_timestamp::date as visit_date,
        min(v.duration_minutes) as min_time_spent
    from visits v
    join facilities f on f.id = v.facility_id
    group by
        f.facility_name,
        visit_date
    """
    source_data = db_connection.get_data_sql(source_query)
    return source_data


@pytest.fixture(scope='module')
def target_data(parquet_reader):
    target_path = r"/parquet_data/facility_name_min_time_spent_per_visit_date"
    target_data = parquet_reader.load(target_path, recursive=True)
    return target_data


@pytest.mark.parquet_data
@pytest.mark.smoke
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_check_dataset_is_not_empty(target_data, data_quality_library):
    data_quality_library.check_dataset_is_not_empty(df=target_data)


@pytest.mark.parquet_data
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_check_count(source_data, target_data, data_quality_library):
    data_quality_library.check_count(df1=source_data, df2=target_data, df1_name="Source data", df2_name="Target data")


@pytest.mark.parquet_data
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_check_data_completeness(source_data, target_data, data_quality_library):
    data_quality_library.check_data_completeness(df1=source_data, df2=target_data)


@pytest.mark.parquet_data
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_check_uniqueness(target_data, data_quality_library):
    data_quality_library.check_duplicates(df=target_data)


@pytest.mark.parquet_data
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_check_not_null_values(target_data, data_quality_library):
    data_quality_library.check_not_null_values(df=target_data,
                                               column_names=['facility_name', 'visit_date', 'min_time_spent'])


@pytest.mark.parquet_data
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_min_time_spent_validity(target_data, data_quality_library):
    data_quality_library.check_column_rules(df=target_data, column_rules={"min_time_spent": {"min": 1}})
