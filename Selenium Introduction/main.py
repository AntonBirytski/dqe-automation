import time
import os
import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException


class SeleniumWebDriverContextManager:
    def __init__(self):
        self.driver = webdriver.Chrome()

    def __enter__(self):
        return self.driver

    def __exit__(self, exc_type, exc_value, traceback):
        if self.driver:
            self.driver.quit()


def load_table_data(driver: WebDriver):
    print(f"\n=======Load table data STARTED=======\n")
    try:
        # Find table by class name
        table = WebDriverWait(driver, 2).until(
            EC.presence_of_element_located((By.CLASS_NAME, "table"))
        )

        # Find container for all columns
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

            # Build Dataframe
            df = pd.DataFrame({headers[i]: rows[i] for i in range(len(headers))})
            df.to_csv("table.csv", index=False)

        print("Successfully created table.csv")
        print(f"\n=======Load table data FINISHED=======\n")

    except TimeoutException:
        print("Error: Table not found before timeout.")

    except NoSuchElementException as e:
        print(f"Error: Missing element during table load - {e}")

    except Exception as e:
        print(f"Error: Failed to load table: {e}")


def load_filter_data(driver: WebDriver, counter: int):
    try:
        doughnut = WebDriverWait(driver, 2).until(
            EC.presence_of_element_located((By.CLASS_NAME, "pielayer"))
        )

        doughnut_slice = doughnut.find_elements(By.CSS_SELECTOR, "g.slicetext text")

        doughnut_data = []

        for slice in doughnut_slice:
            value_data = slice.find_elements(By.TAG_NAME, "tspan")
            facility_type = value_data[0].text.strip()
            min_average_time_spent = value_data[1].text.strip()
            doughnut_data.append({"Facility Type": facility_type, "Min Average Time Spent": min_average_time_spent})

        # Build Dataframe
        df = pd.DataFrame(doughnut_data)
        df.to_csv(f"doughnut{counter}.csv", index=False)
        print(f"filter data saved in doughnut{counter}.csv file")

    except TimeoutException:
        print("Error: Filters data not found before timeout period.")

    except NoSuchElementException as e:
        print(f"Error: Missing element of filter - {e}")


def doughnut_chart_interaction(driver: WebDriver):
    print(f"\n=======Verification doughnut chart STARTED=======\n")
    try:
        filter_box = WebDriverWait(driver, 2).until(
            EC.presence_of_element_located((By.CLASS_NAME, "scrollbox"))
        )

        filter_elements = filter_box.find_elements(By.CSS_SELECTOR, "g.traces")
        for counter, element in enumerate(filter_elements):
            if counter == 0:
                # No filters applied
                driver.save_screenshot(f"screenshot{counter}.png")
                print(f"doughnut chart interaction saved in screenshot{counter}.png file")
                load_filter_data(driver, counter)

            # Select next filter value and wait for the chart to update
            element.click()
            time.sleep(1)
            driver.save_screenshot(f"screenshot{counter + 1}.png")
            print(f"doughnut chart interaction saved in screenshot{counter + 1}.png file")
            load_filter_data(driver, counter + 1)

        print(f"\n=======Verification doughnut chart FINISHED=======\n")

    except TimeoutException:
        print("Error: Chart filters not found before timeout period.")

    except NoSuchElementException as e:
        print(f"Error: Missing element of chart - {e}")


if __name__ == "__main__":
    with SeleniumWebDriverContextManager() as driver:
        html_file_path = os.path.abspath(r"report.html")
        driver.get(f"file://{html_file_path}")
        driver.maximize_window()
        time.sleep(2)
        load_table_data(driver)
        doughnut_chart_interaction(driver)
