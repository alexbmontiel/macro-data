import polars as pl
import json


def load_country_codes():
    with open("country_codes.json", "r") as f:
        codes = json.load(f)
    codes = set(codes.values())

    return codes


def parse_row(
    row: dict
) -> list:
    indicator_id = row["indicator"]["id"]
    indicator = row["indicator"]["value"]
    country_id = row["countryiso3code"]
    country = row["country"]["value"]
    date = row["date"]
    value = row["value"]

    parsed_row = [
        indicator_id,
        indicator,
        country_id,
        country,
        date,
        value
    ]

    return parsed_row


def filter_rows(
    rows: list,
    country_codes: set
) -> list:
    rows = [row for row in rows if row["countryiso3code"] in country_codes]

    return rows


def parse_file(
    raw_file: list
):
    country_codes = load_country_codes()
    rows = filter_rows(raw_file, country_codes)
    rows = [parse_row(row) for row in rows]
    rows = pl.DataFrame(
        rows,
        orient="row",
        schema=[
            "indicator_id",
            "indicator",
            "country_id",
            "country",
            "date",
            "value"
        ]
    )

    return rows

if __name__ == "__main__":
    with open("gdp.json", "r") as file:
        data = json.load(file)
    clean = parse_file(data)
    clean.write_parquet("gdp.parquet")
