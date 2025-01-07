from kafka import KafkaProducer
import requests
import sys
import time
import json
import logging
import uuid
import random
from datetime import datetime
import random

def get_random_coordinates():
    # List of coordinates for the 24 governorates of Tunisia
    locations = [
        (36.806389, 10.181667),  # Tunis
        (36.862499, 10.195556),  # Ariana
        (36.713432, 10.209552),  # Ben Arous
        (36.847278, 10.341667),  # La Manouba
        (37.274350, 9.873910),   # Bizerte
        (36.512276, 8.768223),   # Beja
        (36.446228, 8.792691),   # Jendouba
        (36.163156, 8.714555),   # Kef
        (35.821430, 10.634422),  # Sousse
        (35.671218, 10.091146),  # Monastir
        (35.506798, 11.046753),  # Mahdia
        (34.739581, 10.760026),  # Sfax
        (34.431141, 8.775656),   # Gafsa
        (33.881457, 9.560765),   # Gabes
        (33.503117, 8.775655),   # Medenine
        (33.138126, 10.268992),  # Tataouine
        (34.274433, 8.445874),   # Kasserine
        (35.167671, 8.831457),   # Kairouan
        (35.403687, 9.007866),   # Sidi Bouzid
        (35.944358, 9.798982),   # Siliana
        (35.797610, 10.125962),  # Zaghouan
        (37.342620, 9.850240),   # Nabeul
        (36.742204, 8.662648),   # Tabarka
        (33.886917, 10.098194)   # Tozeur
    ]

    # Select and return a random coordinate
    return random.choice(locations)
random_lat_lon=get_random_coordinates()
# Extract weather data
def collect_data():
    random_lat_lon = get_random_coordinates()
    api_key='793f01cfaae4d60a2baadc4829ec7cc7'
    url= f'https://api.openweathermap.org/data/2.5/weather?'\
        f'lat={random_lat_lon[0]}&lon={random_lat_lon[1]}'\
            f'&appid={api_key}&units=metric'
    response = requests.get(url)
    res=response.json()
    return res



def format_data(weather_data):
    data = {    
    "id" : weather_data['dt'],        
    "city_name": weather_data["name"],
    "wind_speed":weather_data["wind"]['speed'],
    "latitude": weather_data["coord"]['lat'],  # Take the first weather object
    "longtitude": weather_data["coord"]['lon'],
    "time_of_record":weather_data["dt"],
    "surnise_time":weather_data["sys"]["sunrise"],
    "sunset_time":weather_data["sys"]["sunset"],
    "temperature":weather_data["main"]['temp'],
    "humidity":weather_data["main"]['humidity']}   
    return(data)
# Send data to Kafka topic
def stream_data():
    producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],value_serializer=lambda v:json.dumps(v).encode('utf-8'))
    while True:
        res=collect_data()
        res=format_data(res)
        topic = 'project1'
        if res:     
            producer.send('project1', res)
            print(f"Data sent to Kafka topic '{topic}': {res}")
        time.sleep(5)
        


test=stream_data()