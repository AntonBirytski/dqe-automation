*** Settings ***
Library    SeleniumLibrary
Library    helper.py


*** Variables ***
${REPORT_FILE}      ${EXECDIR}/src/report.html
${PARQUET_FOLDER}   C:/tmp/parquet_data/facility_type_avg_time_spent_per_visit_date
${HTML_FILTER_DATE}      2025-10-28
${PARQUET_FILTER_DATE}    2025-10-28


*** Test Cases ***
Compare HTML table data with PARQUET data
    [Tags]    html_2_parquet
    Open Browser    file://${REPORT_FILE}    Chrome
    ${table}=    Get WebElement    class:table
    ${html_df}=    Read Table    ${table}    ${HTML_FILTER_DATE}
    ${parquet_df}=    Read Parquet    ${PARQUET_FOLDER}    ${PARQUET_FILTER_DATE}
    ${html_df_norm}=    Normalize Dataframe    ${html_df}

    ${match}    ${diff}=    Compare Dataframes    ${html_df_norm}    ${parquet_df}
    Run Keyword If    not ${match}    Fail    Data mismatch:\n${diff}

    [Teardown]    Close Browser
