import sys
import requests
import yaml
from datetime import datetime
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from src.common.logger import get_logger
from src.common.aws import S3Util
from src.common.favicon import favicon

CONFIG_PATH = 'config.yaml'
with open(CONFIG_PATH, "r", encoding="utf-8") as file:
    config = yaml.safe_load(file)

logger = get_logger()

def get_historical_data(year, month) -> None:
    """
    Get historical data from the NYC Taxi CDN.
    """
    s3_client = S3Util()

    # S3 Location Configuration Variables
    raw = config["s3_config"]["catalogs"]["yellow_tripdata"]["raw"]
    bucket_name = config["s3_config"]["bucket_name"]

    # Additional variables for tracking the current year and month
    curr_year = datetime.now().year
    year_month_list = []

    while year <= curr_year:
        if year == curr_year and month == datetime.now().month:
            break
        month_str = "0" + str(month) if len(str(month)) == 1 else str(month)
        year_month_list.append(str(year) + "-" + month_str)
        if month == 12:
            month = 1
            year += 1
            continue
        month += 1

    for year_month in year_month_list:
        logger.info(f"{favicon['info']} Fetching data for %s", year_month)
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year_month}.parquet"
        file_year = year_month.split("-")[0]

        with requests.get(url, stream=True) as response:
            response.raise_for_status()
            s3_key = rf"{raw}/year={file_year}/yellow_tripdata_{year_month}.parquet"
            
            # Stream response.raw directly to S3
            s3_client.upload_fileobj(
                file_obj=response.raw, 
                bucket_name=bucket_name, 
                object_key=s3_key,
                # ExtraArgs={'ContentType': content_type}
            )
        
    logger.info(f"{favicon['right']} Successfully uploaded %s to S3", s3_key)


def get_current_month_data():
    """Will be run monthly once to fetch data for the latest month."""
    pass


if __name__ == "__main__":
    logger.info(f"{favicon['info']} Start fetching data from NYC CDN")
    year = config["nyc_api"]["start_year"]
    month = config["nyc_api"]["start_month"]
    refresh_type= config["nyc_api"]["refresh_type"]

    if refresh_type == "historical":
        get_historical_data(year, month)
    elif refresh_type == "current":
        get_current_month_data()
    else:
        logger.error(f"{favicon['error']} Invalid refresh_type specified in the configuration: %s", refresh_type)
        raise ValueError(f"Invalid refresh_type specified in the configuration: {refresh_type}")
