import logging
from datetime import datetime
import trio
import requests
from airflow.decorators import task
from bs4 import BeautifulSoup
from bs4.element import Tag

candidates = []
BASE_URL = "http://www.myjob.vn"


def get_page_content(page: int):
    """
    Retrieves the content of a specific page from a website.

    Args:
        page (int): The page number to retrieve.

    Returns:
        BeautifulSoup: The parsed HTML content of the page.
    """
    # Construct the URL for the specific page
    url = f"{BASE_URL}/ho-so/page-{page}.html"

    # Send a GET request to the URL and retrieve the response
    response = requests.get(url)

    # Parse the response content using BeautifulSoup
    content = BeautifulSoup(response.content, "html.parser")

    return content


def get_page_content(page: int):
    """
    Retrieves the content of a specific page from a website.

    Args:
        page (int): The page number to retrieve.

    Returns:
        BeautifulSoup: The parsed HTML content of the page.
    """
    # Construct the URL for the specific page
    url = f"{BASE_URL}/ho-so/page-{page}.html"

    # Send a GET request to the URL and retrieve the response
    response = requests.get(url)

    # Parse the response content using BeautifulSoup
    return BeautifulSoup(response.content, "html.parser")


def get_literacy(tr: Tag):
    """
    Extracts the literacy information of a candidate from a given HTML tag.

    Args:
        tr (Tag): The HTML tag containing the candidate information.

    Returns:
        str: The literacy information of the candidate.
    """
    # Extract the URL of the candidate's profile
    candidate_url = tr.select_one("div.td0 a[href]").attrs["href"]

    # Get candidate's profile URL and parse the response content using BeautifulSoup
    response = requests.get(f"{BASE_URL}{candidate_url}")
    soup = BeautifulSoup(response.content, "html.parser")

    # Extract the literacy information from the parsed content
    literacy = soup.select_one("table#dtlVLd > tr > td > div.divtdr")

    # Return the extracted literacy information
    return literacy.text.strip() if literacy else ""


async def extract_candidate_data(tr: Tag):
    """
    Extracts candidate data from a given HTML tag.

    Args:
        tr (Tag): The HTML tag containing the candidate data.

    Returns:
        dict: A dictionary containing the extracted candidate data.

    """
    # Extracting individual data fields from the HTML tag
    job_title = tr.select_one("div.td0 a[href]").text.strip()  # Extract job title
    literacy = get_literacy(tr)  # Extract literacy level
    name = tr.select_one("div.td1").text.replace(tr.select_one("div.td1 b").text, "").strip()  # Extract candidate name
    experience = tr.select_one("div.td2").text.strip()  # Extract experience
    salary = tr.select_one("div.td3").text.strip()  # Extract salary
    workplace = tr.select_one("div.td4").text.strip()  # Extract workplace
    updated_at = tr.select_one("div.td5").text.strip()  # Extract updated date

    # Constructing the candidate data dictionary
    candidates.append(
        {
            "job_title": job_title,
            "literacy": literacy,
            "name": name,
            "experience": experience,
            "salary": salary,
            "workplace": workplace,
            "updated_at": updated_at,
            "scraped_at": datetime.now().isoformat(),  # Adding the current timestamp
        }
    )


async def pre_scrape():
    """
    Extracts candidate data from the MyJobVN website.

    Returns:
        list: A list of candidate data extracted from the website.
    """
    # Initialize variables
    page = 1

    # Loop through pages
    while True:
        # Comment those two lines to scrape all pages
        if page > 30:
            break

        # Log the current page being scraped
        logging.info(f"Scraping data from page {page}")

        # Get the page content using a helper function
        soup = get_page_content(page)

        # Select all table rows containing candidate data
        trs = soup.select("table#dtlVL tr")

        # Break the loop if no more rows found
        if not trs:
            break

        async with trio.open_nursery() as nursery:
            for tr in trs:
                nursery.start_soon(extract_candidate_data, tr)

        # Log the number of candidates extracted so far
        logging.info(f"Extracted {len(candidates)} candidates")

        # Move to the next page
        page += 1

    # Return the list of extracted candidates
    return candidates


@task
def extract_myjobvn():
    trio.run(pre_scrape)
    return candidates
