from airflow.decorators import dag, task
from pendulum import datetime

from include.tasks.extractors.careerviet import extract_careerviet
from include.tasks.extractors.myjobvn import extract_myjobvn
from include.tasks.transformers.careerviet import transform_careerviet
from include.tasks.transformers.myjobvn import transform_myjobvn
import pandas as pd


@dag(schedule="@daily", start_date=datetime(2023, 1, 1), catchup=False, tags=["qode"])
def candidate():
    @task
    def transform(*candidates_list):
        # Convert candidates_list to pandas DataFrames
        dfs = [pd.DataFrame(candidates) for candidates in candidates_list]

        # Concatenate the DataFrames
        concatenated_df = pd.concat(dfs)

        # Save the concatenated DataFrame to a CSV file
        concatenated_df.to_csv("candidates.csv", index=False)

    careerviet_candidates = extract_careerviet()
    careerviet_candidates = transform_careerviet(careerviet_candidates)

    myjobvn_candidates = extract_myjobvn()
    myjobvn_candidates = transform_myjobvn(myjobvn_candidates)

    transform(careerviet_candidates, myjobvn_candidates)


candidate()
