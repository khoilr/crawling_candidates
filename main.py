import asyncio

import polars as pl
from selenium import webdriver
from selenium.webdriver.common.by import By
from tabulate import tabulate
from datetime import datetime

options = webdriver.EdgeOptions()
options.use_chromium = True
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

candidates_df = pl.DataFrame(
    {},
    schema={
        "job_title": str,
        "name": str,
        "literacy": str,
        "rank": str,
        "experience": str,
        "salary": str,
        "workplace": str,
        "updated_at": str,
        "scraped_at": str,
    },
)


async def scrape_data(page):
    global candidates_df
    driver = webdriver.Edge(options=options)

    driver.get(
        f"https://careerviet.vn/vi/tim-ung-vien/nganh-nghe/cntt-phan-mem/sort/date_desc/page/{page}"
    )

    candidates_table = driver.find_element(
        By.CSS_SELECTOR, "div.table-jobs-posting table"
    )
    trs = candidates_table.find_elements(By.CSS_SELECTOR, "tbody tr")

    if not trs:
        driver.quit()
        return

    candidates = []
    for tr in trs:
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
        rank = (
            candidate.find_element(By.CSS_SELECTOR, "ul.info-list li:nth-child(2)")
            .text.split(":")[1]
            .strip()
        )

        experience = tr.find_element(By.CSS_SELECTOR, "td:nth-child(2)").text.strip()
        salary = tr.find_element(By.CSS_SELECTOR, "td:nth-child(3)").text.strip()
        workplace = tr.find_element(By.CSS_SELECTOR, "td:nth-child(4)").text.strip()
        updated_at = tr.find_element(By.CSS_SELECTOR, "td:nth-child(5)").text.strip()
        scraped_at = datetime.now().isoformat()

        candidates.append(
            {
                "job_title": job_title,
                "name": name,
                "literacy": literacy,
                "rank": rank,
                "experience": experience,
                "salary": salary,
                "workplace": workplace,
                "updated_at": updated_at,
                "scraped_at": scraped_at,
            }
        )

    # Add scraped data to the candidates DataFrame
    candidates_df = pl.concat([candidates_df, pl.DataFrame(candidates)])

    driver.quit()


async def main():
    # initialize the webdriver
    driver = webdriver.Edge(options=options)
    driver.get(
        "https://careerviet.vn/vi/tim-ung-vien/nganh-nghe/cntt-phan-mem/sort/date_desc/page/1"
    )

    # get the total number of candidates
    total_candidates = driver.find_element(
        By.CSS_SELECTOR, "div.right-heading strong:last-child"
    ).text
    total_candidates = int(total_candidates.replace(",", ""))

    # each page has 20 candidates, calculate the total number of pages
    total_pages = (total_candidates // 20) + 1

    driver.quit()

    tasks = [asyncio.create_task(scrape_data(i + 1)) for i in range(total_pages)]
    await asyncio.gather(*tasks)

    candidates_df.write_csv("candidates.csv")


asyncio.run(main())
