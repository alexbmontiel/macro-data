import requests
import logging
from dataclasses import dataclass, asdict


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
BASE_URL = "https://api.worldbank.org/v2/"


@dataclass
class QueryParams:
    date: str=None
    format: str="json"
    per_page: int=100


def query_api(
    indicator: str,
    country: str="all",
    date: str=None,
    format: str="json",
    per_page: int=100
):
    params = QueryParams(
        date=date,
        format=format,
        per_page=per_page
    )
    params = {
        key: str(value)
        for key, value in asdict(params).items()
        if value is not None
    }
    query = BASE_URL + f"country/{country}/indicator/{indicator}"

    page = 1
    all_data = []

    while True:
        try:
            params["page"] = page
            response = requests.get(query, params=params)
            response.raise_for_status()

            data = response.json()

            if len(data) > 1 and data[1] is not None:
                new_data = data[1]
                all_data.extend(new_data)

                logging.info(f"Successfully retrieved page {page} of {data[0]['pages']} for {indicator}.")

                total_pages = data[0]["pages"]
                if page >= total_pages:
                    break
                else:
                    page += 1
            else:
                logging.warning("No data found in response.")
                break
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}")
            break
        except ValueError as e:
            logging.error(f"Failed to parse JSON: {e}")
            break

    return all_data


if __name__ == "__main__":
    import json
    test = query_api(indicator="NY.GDP.MKTP.CD")
    with open("gdp.json", "w") as f:
        json.dump(test, f)
