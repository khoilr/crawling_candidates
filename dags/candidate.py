import pandas as pd
import psycopg2
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import datetime
from psycopg2 import sql
from datetime import datetime
from include.tasks.extractors.careerviet import extract_careerviet
from include.tasks.extractors.myjobvn import extract_myjobvn
from include.tasks.transformers.careerviet import transform_careerviet
from include.tasks.transformers.myjobvn import transform_myjobvn


def loader(df: pd.DataFrame):
    """
    Load candidate data into the PostgreSQL database.

    Args:
        df (pd.DataFrame): DataFrame containing candidate data.

    Returns:
        None
    """
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        host="postgres-db",
        database="postgres",
        user="postgres",
        password="postgres",
    )
    cur = conn.cursor()

    # Iterate over each row in the DataFrame and insert into the candidates table
    for row in df.itertuples(index=False):
        query = sql.SQL(
            """
            INSERT INTO candidates (job_title, name, literacy, experience, salary, workplace, updated_at, scraped_at, source) 
            VALUES ('{}', '{}', '{}', {}, {}, '{}', '{}', '{}', '{}') 
            ON CONFLICT DO NOTHING;
            """.format(
                row.job_title,
                row.name,
                row.literacy,
                row.experience,
                row.salary,
                row.workplace,
                row.updated_at,
                row.scraped_at,
                row.source,
            )
        )
        cur.execute(query)

    # Commit the changes and close the database connection
    conn.commit()
    conn.close()
    cur.close()


@dag(schedule="@daily", start_date=datetime(2023, 1, 1), catchup=False, tags=["qode"])
def candidate():
    """
    DAG for loading and processing candidate data.

    This DAG defines tasks to extract candidate data from different sources,
    transform the data, and load it into a CSV file.

    The DAG consists of the following tasks:
    - init_db: Initializes the database table if it doesn't exist.
    - load: Loads and processes candidate data from multiple sources.

    The DAG is scheduled to run daily, starting from January 1, 2023, and
    does not catch up on any missed runs. It is tagged with "qode".

    Returns:
        None
    """

    # Initialize the database table if it doesn't exist
    SQLExecuteQueryOperator(
        task_id="init_db",
        conn_id="postgres",
        sql="""
                CREATE TABLE IF NOT EXISTS candidates (
                    id SERIAL PRIMARY KEY,
                    job_title VARCHAR(255),
                    name VARCHAR(255) UNIQUE NOT NULL,
                    literacy VARCHAR(255),
                    experience VARCHAR(255),
                    salary VARCHAR(255),
                    workplace VARCHAR(255),
                    updated_at TIMESTAMP,
                    scraped_at TIMESTAMP,
                    source VARCHAR(255)
                );
            """,
        autocommit=True,
        return_last=True,
        show_return_value_in_logs=True,
    )

    @task
    def load(*candidates_list: list[dict]):
        """
        Load and process candidate data.

        Args:
            *candidates_list (list[dict]): Variable number of candidate dictionaries.

        Returns:
            None
        """
        # Create a list of DataFrames from the candidate dictionaries
        dfs = [pd.DataFrame(candidates) for candidates in candidates_list]

        # Concatenate the DataFrames into a single DataFrame
        concatenated_df = pd.concat(dfs)

        # Save the concatenated DataFrame to a CSV file
        concatenated_df.to_csv(f"outputs/candidates_{datetime.now().isoformat()}.csv", index=False)

        # Call the loader function to load the data
        loader(concatenated_df)

    # Extract candidate data from CareerViet source
    careerviet_candidates = extract_careerviet()
    careerviet_candidates = transform_careerviet(careerviet_candidates)

    # Extract candidate data from MyJobVN source
    myjobvn_candidates = extract_myjobvn()
    myjobvn_candidates = transform_myjobvn(myjobvn_candidates)

    # Load and process candidate data from both sources
    load(careerviet_candidates, myjobvn_candidates)


candidate()
