import logging

import pandas as pd
from airflow.decorators import task


def transfer_name(df: pd.DataFrame):
    """
    Transfers the name column in the given DataFrame to title case.

    Args:
        df (pd.DataFrame): The DataFrame containing the name column.

    Returns:
        pd.DataFrame: The DataFrame with the name column transformed to title case.
    """
    # Transform the name column to title case
    df["name"] = df["name"].str.title()

    return df


def transfer_literacy(df: pd.DataFrame):
    """
    Transfers the literacy values in the given DataFrame to standardized values.

    Args:
        df (pd.DataFrame): The DataFrame containing the 'literacy' column.

    Returns:
        pd.DataFrame: The DataFrame with the 'literacy' column transformed to standardized values.
    """
    # Convert literacy values to uppercase
    df["literacy"] = df["literacy"].str.upper()

    # Replace literacy values with standardized values
    df["literacy"] = df["literacy"].replace(
        {
            "CỬ NHÂN": "University",
            "KỸ SƯ": "University",
            "BÁC SĨ Y KHOA": "University",
            "KHÁC": "Other",
            "": "Other",
            "TRUNG HỌC": "High School",
            "TRUNG CẤP": "Intermediate",
            "CAO ĐẲNG": "College",
            "SAU ĐẠI HỌC": "Postgraduate",
            "THẠC SĨ": "Master",
            "TIẾN SĨ": "Doctorate",
        }
    )

    return df


def transform_workplace(df: pd.DataFrame):
    """
    Transforms the 'workplace' column in the given DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame containing the 'workplace' column.

    Returns:
        pd.DataFrame: The DataFrame with the 'workplace' column transformed.

    """
    # Convert 'workplace' values to uppercase
    df["workplace"] = df["workplace"].str.upper()

    # Replace 'TP HCM' with 'Hồ Chí Minh' and title case the values
    df["workplace"] = df["workplace"].replace({"TP HCM": "Hồ Chí Minh"}).str.title()

    return df


def transform_experience(df: pd.DataFrame):
    """
    Transforms the 'experience' column in the given DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame containing the 'experience' column.

    Returns:
        pd.DataFrame: The DataFrame with the 'experience' column transformed.

    """
    # Convert 'experience' values to uppercase
    df["experience"] = df["experience"].str.upper()

    # Remove 'NĂM' from 'experience' values and keep only the number
    df["experience"] = df["experience"].str.replace(
        r"(\d+) NĂM",
        lambda match: match.group(1),
        regex=True,
    )

    # Replace empty strings with '0' in 'experience' column
    df["experience"] = df["experience"].replace("", "0")

    # Convert 'experience' values to integers
    df["experience"] = df["experience"].astype(int)

    return df


def transform_salary(df: pd.DataFrame):
    """
    Transforms the 'salary' column in the given DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame containing the 'salary' column.

    Returns:
        pd.DataFrame: The transformed DataFrame with the 'salary' column modified.

    """
    # Convert 'salary' values to uppercase
    df["salary"] = df["salary"].str.upper()

    def convert_salary(salary: str):
        """
        Converts the salary string to an integer value.

        Args:
            salary (str): The salary string to be converted.

        Returns:
            int: The converted salary value.

        """
        if "VND" in salary or "VNĐ" in salary:
            return int(salary.replace("VND", "").replace("VNĐ", "").replace(",", "").strip())
        elif "USD" in salary:
            salary = salary.replace("USD", "").replace(",", "").strip()
            return int(salary) * 23000
        else:
            return 0

    # Apply the convert_salary function to the 'salary' column
    df["salary"] = df["salary"].apply(convert_salary)

    # Log unique salary values
    logging.info(df["salary"].unique())

    return df


def transform_updated_at(df: pd.DataFrame):
    """
    Transforms the 'updated_at' column in the given DataFrame to a specific format.

    Args:
        df (pd.DataFrame): The DataFrame containing the 'updated_at' column.

    Returns:
        pd.DataFrame: The DataFrame with the 'updated_at' column transformed.

    """
    # Convert 'updated_at' column to datetime format
    df["updated_at"] = pd.to_datetime(df["updated_at"], format="%d/%m/%Y")

    # Convert 'updated_at' column to isoformat
    df["updated_at"] = df["updated_at"].dt.strftime("%Y-%m-%dT%H:%M:%S.%f")

    return df


def add_source(df: pd.DataFrame):
    """
    Add a 'source' column to the DataFrame with the value 'MyJobVN'.

    Args:
        df (pd.DataFrame): The DataFrame to add the 'source' column to.

    Returns:
        pd.DataFrame: The DataFrame with the 'source' column added.
    """
    # Add the 'source' column to the DataFrame
    df["source"] = "MyJobVN"

    return df


@task
def transform_myjobvn(candidates):
    """
    Transforms the given candidates data and saves it to a CSV file.

    Args:
        candidates (list): A list of candidate dictionaries.

    Returns:
        list: A list of candidate dictionaries in the transformed format.
    """
    # Convert candidates list to a DataFrame
    candidates_df = pd.DataFrame(candidates)

    # Remove duplicate rows from the DataFrame
    candidates_df.drop_duplicates(inplace=True)

    # Apply transformation functions to the DataFrame
    candidates_df = transfer_name(candidates_df)
    candidates_df = transfer_literacy(candidates_df)
    candidates_df = transform_workplace(candidates_df)
    candidates_df = transform_experience(candidates_df)
    candidates_df = transform_salary(candidates_df)
    candidates_df = transform_updated_at(candidates_df)
    candidates_df = add_source(candidates_df)

    # Remove duplicate rows from the DataFrame again
    candidates_df.drop_duplicates(inplace=True)

    # Save the transformed DataFrame to a CSV file
    candidates_df.to_csv("candidates_myjobvn.csv", index=False)

    # Convert the DataFrame to a list of dictionaries
    return candidates_df.to_dict(orient="records")
