# QODE assignment interview for Data Engineer (crawling focus)

## Pre-requisites

- [Docker](https://docs.docker.com/engine/install/)
- [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli)

### How to run

### Difficulties

- To make sure Selenium can run on your machine, I used Selenium Grid in Docker by writing `docker-compose.override.yml`. By default, Selenium Grid has a maximum sessions is one, and a timeout is 300, this is not enough for scraping efficiently and quickly, so I need to override those values by defining environment variables. Also, I need to config `networks` to work with my Airflow

- At first, I could not run concurrently in `extract` task, after some investigation, I found out that Selenium Webdriver methods are not designed to work with asyncio, they block operations that don't release control back to the event loop until they're done. So I need to run the WebDriver methods in a separate thread to prevent them from blocking the event loop. You can find the block code of the solution in the `include/tasks/careerviet.py` file.

    ```python
    driver = await trio.to_thread.run_sync(
            webdriver.Remote,
            "http://selenium:4444",
            True,
            None,
            webdriver.EdgeOptions(),
        )
    ```

### Notes

For demonstration purposes, I limited only scrape 30 first pages of each website. There is a block of code where you can get the total number of pages and remove the limit.
