from datetime import datetime, timedelta

import pandas as pd
from airflow.decorators import task


def transform_name(df: pd.DataFrame):
    df["name"] = df["name"].str.title()
    return df


def transform_workplace(df: pd.DataFrame):
    df["workplace"] = df["workplace"].str.split("\n").str[0]
    return df


def transform_updated_at(df: pd.DataFrame):
    df["updated_at"] = df["updated_at"].str.upper()
    df["updated_at"] = df["updated_at"].replace(
        {
            "NGÀY HÔM NAY": datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).isoformat(),
            "NGÀY HÔM QUA": (datetime.now() - timedelta(days=1))
            .replace(hour=0, minute=0, second=0, microsecond=0)
            .isoformat(),
        },
    )
    df["updated_at"] = df["updated_at"].str.replace(
        r"(\d+) NGÀY",  # "days ago"
        lambda match: (datetime.now() - timedelta(days=int(match.group(1))))
        .replace(hour=0, minute=0, second=0, microsecond=0)
        .isoformat(),
        regex=True,
    )
    df["updated_at"] = df["updated_at"].str.replace(
        r"(\d+) TUẦN",  # "weeks ago"
        lambda match: (datetime.now() - timedelta(weeks=int(match.group(1))))
        .replace(hour=0, minute=0, second=0, microsecond=0)
        .isoformat(),
        regex=True,
    )
    df["updated_at"] = df["updated_at"].str.replace(
        r"(\d+) THÁNG",  # "months ago"
        lambda match: (datetime.now() - timedelta(months=int(match.group(1))))
        .replace(hour=0, minute=0, second=0, microsecond=0)
        .isoformat(),
        regex=True,
    )
    return df


def transform_experience(df: pd.DataFrame):
    df["experience"] = df["experience"].str.upper()
    df["experience"] = df["experience"].replace(
        {
            "CHƯA CÓ KINH NGHIỆM": "0",
            "TRÊN 10 NĂM": "10",
        }
    )
    df["experience"] = df["experience"].str.replace(
        r"(\d+) NĂM",  # "years"
        lambda match: match.group(1),
        regex=True,
    )
    df["experience"] = df["experience"].astype(int)

    return df


def transform_salary(df: pd.DataFrame):
    df["salary"] = df["salary"].str.upper()

    def convert_salary(salary):
        if "VND" in salary:
            salary = salary.replace("TR", "").replace("VND", "").replace(",", "").strip()
            if "-" in salary:
                min_salary, max_salary = salary.split("-")
                return (int(min_salary) + int(max_salary)) / 2 * 1000000  # Convert million to VND
            else:
                return int(salary) * 1000000  # Convert million to VND
        elif "USD" in salary:
            salary = salary.replace("USD", "").replace(",", "").strip()
            if "-" in salary:
                min_salary, max_salary = salary.split("-")
                return (
                    (float(min_salary) + float(max_salary)) / 2 * 23000
                )  # Convert USD to VND (assuming 1 USD = 23000 VND)
            else:
                return float(salary) * 23000  # Convert USD to VND (assuming 1 USD = 23000 VND)

    df["salary"] = df["salary"].apply(convert_salary)
    return df


def transfer_literacy(df: pd.DataFrame):
    df["literacy"] = (
        df["literacy"]
        .str.upper()
        .replace(
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
    )

    return df


def add_source(df: pd.DataFrame):
    df["source"] = "CareerViet"
    return df


@task
def transform_careerviet(candidates):
    # convert to DataFrame
    candidates_df = pd.DataFrame(candidates)

    candidates_df.drop_duplicates(inplace=True)

    # apply transformations
    candidates_df = transform_name(candidates_df)
    candidates_df = transform_workplace(candidates_df)
    candidates_df = transform_updated_at(candidates_df)
    candidates_df = transform_experience(candidates_df)
    candidates_df = transform_salary(candidates_df)
    candidates_df = transfer_literacy(candidates_df)
    candidates_df = add_source(candidates_df)

    candidates_df.to_csv("candidates_careerviet.csv", index=False)

    # convert back to list of dictionaries
    return candidates_df.to_dict(orient="records")
