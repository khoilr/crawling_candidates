from datetime import datetime, timedelta

import pandas as pd
from airflow.decorators import task


def transform_name(df: pd.DataFrame):
    """
    Transforms the 'name' column in the given DataFrame to title case.

    Args:
        df (pd.DataFrame): The DataFrame containing the 'name' column.

    Returns:
        pd.DataFrame: The DataFrame with the 'name' column transformed to title case.
    """
    # Transform the 'name' column to title case
    df["name"] = df["name"].str.title()

    return df


def transform_workplace(df: pd.DataFrame):
    """
    Transforms the 'workplace' column in the given DataFrame by extracting the first line of each entry.

    Args:
        df (pd.DataFrame): The DataFrame containing the 'workplace' column.

    Returns:
        pd.DataFrame: The transformed DataFrame with the updated 'workplace' column.
    """
    # Split the 'workplace' column by newline and extract the first line
    df["workplace"] = df["workplace"].str.split("\n").str[0]

    return df


def transform_updated_at(df: pd.DataFrame):
    """
    Transforms the 'updated_at' column in the given DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame containing the 'updated_at' column.

    Returns:
        pd.DataFrame: The DataFrame with the 'updated_at' column transformed.
    """
    # Convert all values in the 'updated_at' column to uppercase
    df["updated_at"] = df["updated_at"].str.upper()

    # Replace specific values with current date and time
    df["updated_at"] = df["updated_at"].replace(
        {
            "NGÀY HÔM NAY": datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).isoformat(),
            "NGÀY HÔM QUA": (datetime.now() - timedelta(days=1))
            .replace(hour=0, minute=0, second=0, microsecond=0)
            .isoformat(),
        },
    )

    # Replace values in the format 'x NGÀY' with dates x days ago
    df["updated_at"] = df["updated_at"].str.replace(
        r"(\d+) NGÀY",  # "days ago"
        lambda match: (datetime.now() - timedelta(days=int(match.group(1))))
        .replace(hour=0, minute=0, second=0, microsecond=0)
        .isoformat(),
        regex=True,
    )

    # Replace values in the format 'x TUẦN' with dates x weeks ago
    df["updated_at"] = df["updated_at"].str.replace(
        r"(\d+) TUẦN",  # "weeks ago"
        lambda match: (datetime.now() - timedelta(weeks=int(match.group(1))))
        .replace(hour=0, minute=0, second=0, microsecond=0)
        .isoformat(),
        regex=True,
    )

    # Replace values in the format 'x THÁNG' with dates x months ago
    df["updated_at"] = df["updated_at"].str.replace(
        r"(\d+) THÁNG",  # "months ago"
        lambda match: (datetime.now() - timedelta(months=int(match.group(1))))
        .replace(hour=0, minute=0, second=0, microsecond=0)
        .isoformat(),
        regex=True,
    )

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

    # Replace specific values in 'experience' column
    df["experience"] = df["experience"].replace(
        {
            "CHƯA CÓ KINH NGHIỆM": "0",
            "TRÊN 10 NĂM": "10",
        }
    )

    # Extract numeric values from 'experience' column
    df["experience"] = df["experience"].str.replace(
        r"(\d+) NĂM",  # "years"
        lambda match: match.group(1),
        regex=True,
    )

    # Convert 'experience' values to integer
    df["experience"] = df["experience"].astype(int)

    return df


def transform_salary(df: pd.DataFrame):
    """
    Transforms the salary column in the given DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame containing the salary column.

    Returns:
        pd.DataFrame: The DataFrame with the transformed salary column.
    """
    # Convert salary values to uppercase
    df["salary"] = df["salary"].str.upper()

    def convert_salary(salary):
        """
        Converts the salary value to VND.

        Args:
            salary (str): The salary value.

        Returns:
            int: The converted salary value in VND.
        """
        if "VND" in salary:
            # Remove unnecessary characters and convert to integer
            salary = salary.replace("TR", "").replace("VND", "").replace(",", "").strip()
            if "-" in salary:
                # If salary range, calculate average and convert to VND
                min_salary, max_salary = salary.split("-")
                return int((int(min_salary) + int(max_salary)) / 2 * 1000000)  # Convert million to VND
            else:
                # If single salary value, convert to VND
                return int(salary) * 1000000  # Convert million to VND
        elif "USD" in salary:
            # Remove unnecessary characters and convert to integer
            salary = salary.replace("USD", "").replace(",", "").strip()
            if "-" in salary:
                # If salary range, calculate average and convert to VND (assuming 1 USD = 23000 VND)
                min_salary, max_salary = salary.split("-")
                return int((int(min_salary) + int(max_salary)) / 2 * 23000)  # Convert USD to VND
            else:
                # If single salary value, convert to VND (assuming 1 USD = 23000 VND)
                return int(salary) * 23000  # Convert USD to VND

        else:
            # If salary value is not in VND or USD, return 0
            return 0

    # Apply the convert_salary function to the salary column
    df["salary"] = df["salary"].apply(convert_salary)
    return df


def transfer_literacy(df: pd.DataFrame):
    """
    Transfers the literacy values in the given DataFrame to their corresponding English equivalents.

    Args:
        df (pd.DataFrame): The DataFrame containing the 'literacy' column.

    Returns:
        pd.DataFrame: The DataFrame with the 'literacy' column updated to English equivalents.
    """
    # Convert the 'literacy' column to uppercase
    df["literacy"] = df["literacy"].str.upper()

    # Replace Vietnamese literacy values with English equivalents
    df["literacy"] = df["literacy"].replace(
        {
            "ĐẠI HỌC": "University",
            "CAO ĐẲNG": "College",
            "TRUNG CẤP": "Intermediate",
            "CHƯA TỐT NGHIỆP": "Undergraduate",
            "TRUNG HỌC": "High School",
            "SAU ĐẠI HỌC": "Postgraduate",
            "KHÁC": "Other",
        }
    )

    return df


def add_source(df: pd.DataFrame):
    """
    Adds a 'source' column to the DataFrame with the value 'CareerViet'.

    Args:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: The DataFrame with the 'source' column added.
    """
    df["source"] = "CareerViet"
    return df


@task
def transform_careerviet(candidates):
    """
    Transforms the given candidates data and saves it to a CSV file.

    Args:
        candidates (list): A list of candidate dictionaries.

    Returns:
        list: A list of transformed candidate dictionaries.

    Raises:
        None
    """
    # Convert candidates list to a DataFrame
    candidates_df = pd.DataFrame(candidates)

    # Remove duplicate rows from the DataFrame
    candidates_df.drop_duplicates(inplace=True)

    # Apply transformation functions to the DataFrame
    candidates_df = transform_name(candidates_df)
    candidates_df = transform_workplace(candidates_df)
    candidates_df = transform_updated_at(candidates_df)
    candidates_df = transform_experience(candidates_df)
    candidates_df = transform_salary(candidates_df)
    candidates_df = transfer_literacy(candidates_df)
    candidates_df = add_source(candidates_df)

    # Remove duplicate rows from the DataFrame again
    candidates_df.drop_duplicates(inplace=True)

    # Save the transformed data to a CSV file
    candidates_df.to_csv("candidates_careerviet.csv", index=False)

    # Convert the DataFrame to a list of dictionaries
    return candidates_df.to_dict(orient="records")
