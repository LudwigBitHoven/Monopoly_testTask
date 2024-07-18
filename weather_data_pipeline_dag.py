from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime
import aiohttp
import json
import pandas as pd
from datetime import datetime
from os.path import exists


API_KEY = ""


def _download_data():
    # используем aiohttp для запроса к API
    url = f"https://api.openweathermap.org/data/2.5/weather?q=London&appid={API_KEY}"
    session = aiohttp.ClientSession()
    with session.get(url) as resp:
        result_json = resp.json()
        with open('tmp/data.json', 'w') as f:
            json.dump(result_json, f)
    session.close()
    return 1


def _process_data():
    with open('tmp/data.json', 'r') as f:
        data = json.load(f)
    result = {}
    # берем необходимые данные из json
    for temperature_key in data["main"]:
        result[temperature_key] = [data["main"][temperature_key] - 273.15]
    result["dt"] = [data["dt"]]
    # конвертируем очищенный json в датафрейм
    df = pd.DataFrame.from_dict(result)
    output_path='processed_weather_data.csv'
    df.to_csv(output_path, mode='a', header=not exists(output_path))
    return 1


def _save_data():
    # используем вшитый в pd метод to_parquet
    df = pd.read_csv("processed_weather_data.csv")
    df.to_parquet('weather.gzip', compression='gzip')
    return 1


today = datetime.today()
year, month, day = today.year, today.month, today.day
start_date = datetime(year=year, month=month, day=day)


with DAG("dag", start_date=start_date, schedule_interval="@daily", catchup=False) as dag:
	download_data = PythonOperator(
		task_id="download_data",
		python_callable=_download_data
	)

	process_data = PythonOperator(
		task_id="process_data",
		python_callable=_process_data
	)

	save_data = PythonOperator(
		task_id="save_data",
		python_callable=_save_data
	)

	download_data >> process_data >> save_data
