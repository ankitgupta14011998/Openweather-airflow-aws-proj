import json
import requests
import pandas as pd
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator

with open('/home/ec2-user/openweather_api/credentials.txt','r') as f:
    api_key=f.read()

city_name="ballia"

def kelvin_to_celsius(temp):
        return temp-273.15

def transform_load_data(task_instance):
    data=task_instance.xcom_pull(task_ids='extract_data')
    city=data['name']
    visibility=data['visibility']
    date=datetime.utcfromtimestamp(data['dt']+data['timezone'])
    sunrise = datetime.utcfromtimestamp(data['sys']['sunrise']+data['timezone'])
    sunset = datetime.utcfromtimestamp(data['sys']['sunset']+data['timezone'])
    windspeed = data['wind']['speed']
    wind_direction_in_degrees = data['wind']['deg']
    temperature=round(kelvin_to_celsius(data['main']['temp']),2)
    temp_feels_like=round(kelvin_to_celsius(data['main']['feels_like']),2)
    min_temperature=round(kelvin_to_celsius(data['main']['temp_min']),2)
    max_temperature=round(kelvin_to_celsius(data['main']['temp_max']),2)
    pressure = data['main']['pressure']
    humidity= data['main']['humidity']

    dic = {
        'city' : city,
        'visibility' : visibility,
        'date' : date,
        'sunrise' : sunrise,
        'sunset' : sunset,
        'windspeed' : windspeed,
        'wind_direction_in_degrees' : wind_direction_in_degrees,
        'temperature' : temperature,
        'temp_feels_like' : temp_feels_like,
        'min_temperature' : min_temperature,
        'max_temperature' : max_temperature,
        'pressure' : pressure,
        'humidity' : humidity
    }

    df = pd.DataFrame([dic])
    now = datetime.now()
    date_string = now.strftime('%Y%m%d%H%M%S')
    credentials = {'key':'ASIAW3MD6IE5474IITF5','secret':'hW/CRBE8X5bGtUOq5swLEZK80hjyw0QYJ/YHL2fi','token':'FwoGZXIvYXdzEAsaDGooHEIOLN5edZYrQiKCAU95O0Nz0BYWuw2LUPmNa8LsZsy7r7ZMFzZA5ODyisDl1EQxOeTD8o16aXZfLOgmWTnIqUAORwSy3AC0CIWUZeBCG//GpmSkQp7DboBM5LJjCpk05ELire1kxBwRLco1OZU6y4WjNkyrh00ryZFB9cyMKhqieTXf+FcSa0oRY6N+LvIo/52ZrQYyKPFjm63VMJ/KDkZxGK5ak8PhjccN2fAggRZajS7FTEFNM6a7AFze10A='}
    df.to_csv(f's3://openweather-api-bucket/weather_data_{city_name}_{date_string}.csv',index=False, storage_options=credentials)

default_args={
    'owner':'Airflow',
    'start_date' : datetime(2024,1,14),
    'depends_on_past' : False,
    'email' : ['ankitcool683@gmail.com'],
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retry_delay' : timedelta(minutes=2),
    'retries' : 2
}


with DAG('weather_api_dag',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False) as dag:

    is_weather_api_ready = HttpSensor(task_id='is_weather_api_ready',
                                      http_conn_id='weatherapi_conn',
                                      endpoint=f'data/2.5/weather?q={city_name}&appid={api_key}')


    extract_data = SimpleHttpOperator(task_id='extract_data',
                                      http_conn_id='weatherapi_conn',
                                      endpoint=f'data/2.5/weather?q={city_name}&appid={api_key}',
                                      method='GET',
                                      response_filter = lambda response : json.loads(response.text),
                                      log_response=True)

    transform_load_data = PythonOperator(task_id='transform_load_data',
                                         python_callable=transform_load_data)
    

    #  load_data=PythonOperator(task):
    #     #     df.to_csv("weather_data.csv",index=False)

    is_weather_api_ready >> extract_data >> transform_load_data



