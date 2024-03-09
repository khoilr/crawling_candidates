import logging
from datetime import datetime, timedelta

import pandas as pd
from airflow.decorators import task


def transfer_name(df: pd.DataFrame):
    df["name"] = df["name"].str.title()
    return df


def transfer_literacy(df: pd.DataFrame):
    df["literacy"] = df["literacy"].str.upper()

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
    df["workplace"] = df["workplace"].str.upper()
    df["workplace"] = df["workplace"].replace({"TP HCM": "Hồ Chí Minh"}).str.title()
    return df


def transform_experience(df: pd.DataFrame):
    df["experience"] = df["experience"].str.upper()

    df["experience"] = df["experience"].str.replace(
        r"(\d+) NĂM",
        lambda match: match.group(1),
        regex=True,
    )
    df["experience"] = df["experience"].replace("", "0")

    df["experience"] = df["experience"].astype(int)
    return df


def transform_salary(df: pd.DataFrame):
    df["salary"] = df["salary"].str.upper()

    def convert_salary(salary: str):
        if "VND" in salary or "VNĐ" in salary:
            return int(salary.replace("VND", "").replace("VNĐ", "").replace(",", "").strip())
        elif "USD" in salary:
            salary = salary.replace("USD", "").replace(",", "").strip()
            return int(salary) * 23000
        else:
            return 0

    df["salary"] = df["salary"].apply(convert_salary)
    logging.info(df["salary"].unique())
    return df


def transform_updated_at(df: pd.DataFrame):
    # convert updated_at (format: 15/10/2021) to datetime iso format
    df["updated_at"] = pd.to_datetime(df["updated_at"], format="%d/%m/%Y")
    df["updated_at"] = df["updated_at"].dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
    return df


def add_source(df: pd.DataFrame):
    df["source"] = "MyJobVN"
    return df


@task
def transform_myjobvn(candidates):
    candidates_df = pd.DataFrame(candidates)

    candidates_df.drop_duplicates(inplace=True)

    candidates_df = transfer_name(candidates_df)
    candidates_df = transfer_literacy(candidates_df)
    candidates_df = transform_workplace(candidates_df)
    candidates_df = transform_experience(candidates_df)
    candidates_df = transform_salary(candidates_df)
    candidates_df = transform_updated_at(candidates_df)
    candidates_df = add_source(candidates_df)

    candidates_df.to_csv("candidates_myjobvn.csv", index=False)

    return candidates_df.to_dict(orient="records")
