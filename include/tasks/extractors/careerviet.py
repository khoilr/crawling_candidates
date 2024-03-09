import logging
import math
from datetime import datetime

import trio
from airflow.decorators import task
from selenium import webdriver
from selenium.webdriver.common.by import By

from include.utils import create_driver_trio

candidates = []


def get_candidates(trs):
    """
    Extracts candidate information from a list of table rows (trs).

    Args:
        trs (list): A list of table rows containing candidate information.

    Returns:
        list: A list of dictionaries, where each dictionary represents a candidate and contains the following keys:
            - job_title (str): The job title of the candidate.
            - name (str): The name of the candidate.
            - literacy (str): The literacy level of the candidate.
            - rank (str): The rank of the candidate.
            - experience (str): The experience of the candidate.
            - salary (str): The salary of the candidate.
            - workplace (str): The workplace of the candidate.
            - updated_at (str): The date and time when the candidate's information was last updated.
            - scraped_at (str): The date and time when the candidate's information was scraped.
    """
    for tr in trs:
        # Extract candidate information
        candidate = tr.find_element(By.CSS_SELECTOR, "td:first-child")
        job_title = candidate.find_element(By.CSS_SELECTOR, "a.job-title").text.strip()
        name = (
            candidate.find_element(By.CSS_SELECTOR, "a.name").text.split("#")[0].strip()
        )
        literacy = (
            candidate.find_element(By.CSS_SELECTOR, "ul.info-list li:first-child")
            .text.split(":")[1]
            .strip()
        )

        experience = tr.find_element(By.CSS_SELECTOR, "td:nth-child(2)").text.strip()
        salary = tr.find_element(By.CSS_SELECTOR, "td:nth-child(3)").text.strip()
        workplace = tr.find_element(By.CSS_SELECTOR, "td:nth-child(4)").text.strip()
        updated_at = tr.find_element(By.CSS_SELECTOR, "td:nth-child(5)").text.strip()
        scraped_at = datetime.now().isoformat()

        # Create a dictionary for the candidate and append it to the candidates list
        candidates.append(
            {
                "job_title": job_title,
                "name": name,
                "literacy": literacy,
                "experience": experience,
                "salary": salary,
                "workplace": workplace,
                "updated_at": updated_at,
                "scraped_at": scraped_at,
            }
        )


async def scrape(from_page: int, to_page: int):
    """
    Scrapes data from CareerViet website for a range of pages.

    Args:
        from_page (int): The starting page number.
        to_page (int): The ending page number.

    Returns:
        None
    """
    logging.info(f"Scraping data from page {from_page} to page {to_page}")
    try:
        # Create a driver instance
        driver = await create_driver_trio()

        # Iterate over the range of pages
        for page in range(from_page, to_page + 1):
            # Visit the CareerViet page for the specific page number
            driver.get(
                f"https://careerviet.vn/vi/tim-ung-vien/nganh-nghe/cntt-phan-mem/sort/date_desc/page/{page}"
            )

            # Find the candidates table on the page
            candidates_table = driver.find_element(
                By.CSS_SELECTOR, "div.table-jobs-posting table"
            )

            # Find all the rows in the table
            trs = candidates_table.find_elements(By.CSS_SELECTOR, "tbody tr")

            # If no rows found, quit the driver and return
            if not trs:
                driver.quit()
                return

            # Extract candidate information from the rows
            get_candidates(trs)

        logging.info(f"Scraped data from page {from_page} to page {to_page}")

    finally:
        # Quit the driver
        driver.quit()


def get_total_pages() -> int:
    """
    Retrieves the total number of pages from the CareerViet website.

    Returns:
        int: The total number of pages.
    """
    try:
        # Create a WebDriver instance using the Remote command executor
        driver = webdriver.Remote(
            command_executor="http://selenium:4444/wd/hub",
            options=webdriver.EdgeOptions(),
        )

        # Open the CareerViet website and navigate to the first page
        driver.get(
            "https://careerviet.vn/vi/tim-ung-vien/nganh-nghe/cntt-phan-mem/sort/date_desc/page/1"
        )

        # Get the total number of candidates from the website
        total_candidates = driver.find_element(
            By.CSS_SELECTOR, "div.right-heading strong:last-child"
        ).text

        # Convert the total number of candidates to an integer
        total_candidates = int(total_candidates.replace(",", ""))

        # Calculate the total number of pages based on the number of candidates per page (20 candidates per page)
        total_pages = (total_candidates // 20) + 1

        # Log the total number of pages
        logging.info(f"Total pages: {total_pages}")

        return total_pages
    finally:
        # Quit the WebDriver instance
        driver.quit()


async def pre_scrape():
    """
    Scrapes data from multiple pages in parallel using Trio.

    This function divides the total number of pages into chunks and starts a separate task for each chunk
    to scrape data from the corresponding pages concurrently.

    Returns:
        None
    """
    # Get the total number of pages and calculate the number of pages to scrape concurrently
    total_pages = 30
    # total_pages = get_total_pages() # Uncomment this line to get the total number of pages from the website
    max_concurrent = 8
    no_each_page = math.ceil(total_pages / max_concurrent)

    # Divide the total number of pages into chunks
    page_chunks = [
        [i, min(total_pages, i + no_each_page - 1)]  # first and last page in the chunk
        for i in range(1, total_pages + 1, no_each_page)
    ]

    # Log the number of pages and chunks
    logging.info(f"Scraping {total_pages} pages in {len(page_chunks)} chunks")

    # Start a separate task for each chunk to scrape data concurrently
    async with trio.open_nursery() as nursery:
        for chunk in page_chunks:
            nursery.start_soon(scrape, *chunk)


@task
def extract_careerviet():
    """
    Extracts candidate data by running the 'scrape' function using the Trio library.

    Returns:
        pandas.DataFrame: A DataFrame containing the extracted candidate data.
    """
    # Run the 'scrape' function using the Trio library
    trio.run(pre_scrape)

    # Return the DataFrame containing the extracted candidate data
    return candidates
