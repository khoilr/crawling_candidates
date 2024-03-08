import json
import logging

import selenium
from airflow.decorators import dag, task
from fake_useragent import FakeUserAgent
from pendulum import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


@dag(
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["qode"],
)
def candidate():
    """
    ### Basic ETL Dag
    This is a simple ETL data pipeline example that demonstrates the use of
    the TaskFlow API using three simple tasks for extract, transform, and load.
    For more information on Airflow's TaskFlow API, reference documentation here:
    https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html
    """

    @task()
    def extract():
        # init selenium
        options = webdriver.EdgeOptions()
        driver = webdriver.Remote(
            command_executor="http://selenium:4444/wd/hub",
            options=options,
        )

        # get imdb popular movies
        imdb_popular_movies_url = "https://www.imdb.com/chart/moviemeter/?ref_=nv_mv_mpm"
        driver.get(imdb_popular_movies_url)

        movie_url_css_selector = "ul.ipc-metadata-list li.ipc-metadata-list-summary-item a"
        # wait for ul.ipc-metadata-list li.ipc-metadata-list-summary-item to appear
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, movie_url_css_selector)))

        # find all li.ipc-metadata-list-summary-item inside ul.ipc-metadata-list
        lis = driver.find_elements(by=By.CSS_SELECTOR, value=movie_url_css_selector)
        urls = [li.get_attribute("href") for li in lis]

        return urls

    urls = extract()
    print(urls)


candidate()
