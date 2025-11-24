"""
Description: Data Quality checks for patient_sum_treatment_cost_per_facility_type file
Requirement(s): TICKET-3
Author(s): Anton Birytski
"""

import pytest


@pytest.fixture(scope='module')
def source_data(db_connection):
    source_query = """
    select
        f.facility_type,
        concat(p.first_name, ' ', p.last_name) as full_name,
        sum(v.treatment_cost) as sum_treatment_cost
    from visits v
    join facilities f on f.id = v.facility_id
    join patients p on p.id = v.patient_id
    group by
        f.facility_type,
        full_name
    """
    source_data = db_connection.get_data_sql(source_query)
    return source_data


@pytest.fixture(scope='module')
def target_data(parquet_reader):
    target_path = r"/parquet_data/patient_sum_treatment_cost_per_facility_type"
    target_data = parquet_reader.load(target_path, recursive=True)
    return target_data


@pytest.mark.parquet_data
@pytest.mark.smoke
@pytest.mark.patient_sum_treatment_cost_per_facility_type
def test_check_dataset_is_not_empty(target_data, data_quality_library):
    data_quality_library.check_dataset_is_not_empty(df=target_data)


@pytest.mark.parquet_data
@pytest.mark.patient_sum_treatment_cost_per_facility_type
def test_check_count(source_data, target_data, data_quality_library):
    data_quality_library.check_count(df1=source_data, df2=target_data, df1_name="Source data", df2_name="Target data")


@pytest.mark.parquet_data
@pytest.mark.patient_sum_treatment_cost_per_facility_type
def test_check_data_completeness(source_data, target_data, data_quality_library):
    data_quality_library.check_data_completeness(df1=source_data, df2=target_data)


@pytest.mark.parquet_data
@pytest.mark.patient_sum_treatment_cost_per_facility_type
def test_check_uniqueness(target_data, data_quality_library):
    data_quality_library.check_duplicates(df=target_data)


@pytest.mark.parquet_data
@pytest.mark.patient_sum_treatment_cost_per_facility_type
def test_check_not_null_values(target_data, data_quality_library):
    data_quality_library.check_not_null_values(df=target_data,
                                               column_names=['facility_type', 'full_name', 'sum_treatment_cost'])


@pytest.mark.parquet_data
@pytest.mark.patient_sum_treatment_cost_per_facility_type
def test_sum_treatment_cost_validity(target_data, data_quality_library):
    data_quality_library.check_column_rules(df=target_data, column_rules={"sum_treatment_cost": {"min": 0}})
