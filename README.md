# Crawling Candidates from Vietnamese Jobs Platforms

## Description

The main codebase resides in the `dags` and `include` directories.

The project utilizes Docker for containerization, ensuring a consistent and reproducible environment across different platforms. It also uses the Astronomer CLI, a command-line interface that allows you to run Apache Airflow DAGs (Directed Acyclic Graphs) in an isolated environment.

The `dags` directory contains the Airflow DAGs, which define the workflows for the data extraction and processing tasks. The `include` directory contains the Python scripts that perform the actual web crawling.

## Flow

The `candidate.py` script defines a Directed Acyclic Graph (DAG) for an Apache Airflow workflow. The DAG is designed to extract, transform, and load (ETL) candidate data from two sources: CareerViet and MyJobVN.

1. **Define the DAG**: The candidate DAG is defined using the @dag decorator. It is scheduled to run daily, starting from January 1, 2023, and does not catch up on any missed runs.

2. **Initialize the database table**: The first task in the DAG, `init_db`, is an instance of the `SQLExecuteQueryOperator`. It executes a SQL query to create the candidates table in the PostgreSQL database if it doesn't already exist.

3. **Extract candidate data**: The script then calls the `extract_careerviet` and `extract_myjobvn` functions to extract candidate data from the CareerViet and MyJobVN sources, respectively. These tasks are implemented using the `trio` library, which is a Python library for async I/O and structured concurrency. The first site uses `selenium` to interact with web elements, while the second site uses `BeautifulSoup` for parsing HTML.

    > For demonstration purposes, I limited only scrape 30 first pages of each website. You can change the `MAX_PAGE` variable in the `include/tasks/careerviet.py` and `include/tasks/myjobvn.py` file to `None` to scrape all pages

4. **Transform candidate data**: The extracted data is then transformed using the `transform_careerviet` and `transform_myjobvn` functions.

5. **Load candidate data**: Finally, the `load` task is called with the transformed candidate data from both sources. This task processes the data, saves it to a CSV file, and loads it into the database.

## Data Schema

| Column       | CareerViet                                                    | MyJobVN                                                               |
| ------------ | ------------------------------------------------------------- | --------------------------------------------------------------------- |
| `name`       | Title case                                                    | Title case                                                            |
| `workplace`  | Extract first line                                            | Standardized location name                                            |
| `updated_at` | Replacing Vietnamese phrases with datetime objects            | Converted to specific datetime format                                 |
| `experience` | Replacing Vietnamese phrases with numeric values              | Extracting numeric value and converting to integer                    |
| `salary`     | Converting salary values to Vietnamese Dong (VND)             | Converting salary string to integer, considering different currencies |
| `literacy`   | Replacing Vietnamese literacy levels with English equivalents | Standardized to a predefined set of values                            |
| `source`     | Added 'CareerViet'                                            | Added 'MyJobVN'                                                       |

## Output Data Schema Description

- `name`: Names of the candidates.
- `workplace`: Workplace information of the candidates.
- `updated_at`: Date when the candidate's information was last updated.
- `experience`: Experience of the candidates in years.
- `salary`: Salary expectations of the candidates, expressed in Vietnamese Dong (VND)
- `literacy`: Literacy level of the candidates.
- `source`: Source of the data.

## How to run

1. [Install Docker](https://docs.docker.com/engine/install/)

2. [Install Astro CLI](https://docs.astronomer.io/astro/cli/install-cli)

3. Run in terminal

    ```bash
    astro dev start
    ```

    > Make sure port **8080**, **5432**, **4444** are not used by other processes.

4. Open your browser and go to `http://localhost:8080` to see the Airflow UI, the username and password are both `admin`.
   ![Screenshot](images/Sign%20In%20-%20Airflow.jpeg)

5. Click 'candidate' DAG to go to the DAG's page.
   ![Screenshot](images/DAGs%20-%20Airflow.jpeg)

6. Turn on the DAG's switch to enable the DAG. Then click 'Trigger DAG' to start the scraping process.
   ![Screenshot](images/candidate%20-%20Grid%20-%20Airflow.jpeg)

7. You can see the progress of the scraping process by clicking on the 'candidate' DAG and then clicking on the **Graph** tab.
   ![Screenshot](<images/candidate - Grid - Airflow · 2.14pm · 03-09.jpeg>)

8. After the scraping process is finished, you can see the CSV file in the `outputs` directory. You can also see the data in the PostgreSQL database with
   - Host `localhost`
   - Port `5431`
   - Username `postgres`
   - Password `postgres`
   - Database `postgres`

   ![Screenshot](<images/Screenshot 2024-03-09 at 3.24.30 PM.jpeg>)

### Difficulties

- To make sure Selenium can run on your machine, I used Selenium Grid in Docker by writing `docker-compose.override.yml`. By default, Selenium Grid has maximum sessions is one, and a timeout is 300, this is not enough for scraping efficiently and quickly, so I need to override those values by defining environment variables. Also, I need to config `networks` to work with my Airflow

- At first, I could not run concurrently in the `extract` task, after some investigation, I found out that Selenium Webdriver methods are not designed to work with async, they block operations that don't release control back to the event loop until they're done. So I use `trio` library to run the WebDriver. You can find the block code of the solution in the `include/tasks/careerviet.py` file.

    ```python
    driver = await trio.to_thread.run_sync(
            webdriver.Remote,
            "http://selenium:4444",
            True,
            None,
            webdriver.EdgeOptions(),
        )
    ```
