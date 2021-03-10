import sys
import traceback
import json
import time
import os
import configparser
from datetime import datetime, timedelta

import psycopg2
import requests

def create_connection(db_name, db_user, db_password, db_host, db_port):
  connection = None
  try:
    connection = psycopg2.connect(
      database=db_name,
      user=db_user,
      password=db_password,
      host=db_host,
      port=db_port,
    )
#    print("Connection to the PostgreSQL DB successful.\nname = ",db_name,"\nuser = ",db_user,"\npassword = ",db_password,"\nhost = ",db_host,"\nport = ",db_port)
  except:
    print("Не удалось подключиться к базе данных, изменить настройки конфигураций можно в файле \"config.txt\":\nname = "+db_name+"\nuser = "+db_user+"\npassword = "+db_password+"\nhost = "+db_host+"\nport = "+db_port)
  return connection
  
def createConfig(path):
  config = configparser.ConfigParser()
  config.add_section("Settings DB")
  config.set("Settings DB", "database", "counters")
  config.set("Settings DB", "user", "counters")
  config.set("Settings DB", "password", "counters")
  config.set("Settings DB", "host", "192.168.105.30")
  config.set("Settings DB", "port", "5432")
  config.add_section("openweathermap")
  config.set("openweathermap", "lon", "36.610279")
  config.set("openweathermap", "lat", "55.096943")
  config.set("openweathermap", "api_key", "fdb021fe937c62a0353524244ec00533")
  config.set("openweathermap", "base_url", "https://api.openweathermap.org/data/2.5/onecall/timemachine")
  config.set("openweathermap", "id_company", "1")
  config.set("openweathermap", "days_ago", "3")
  config.set("openweathermap", "counter", "3")
  with open(path, "w") as config_file:
    config.write(config_file)
  
def readConfig(path):
  if not os.path.exists(path):
    createConfig(path)
  config = configparser.ConfigParser()
  config.read(path)
  db_name = config.get("Settings DB", "database")
  db_user = config.get("Settings DB", "user")
  db_password = config.get("Settings DB", "password")
  db_host = config.get("Settings DB", "host")
  db_port = config.get("Settings DB", "port")
  return db_name,db_user,db_password,db_host,db_port  
  
def readConfig2(path):
  config = configparser.ConfigParser()
  config.read(path)
  lon = config.get("openweathermap", "lon")
  lat = config.get("openweathermap", "lat")
  api_key = config.get("openweathermap", "api_key")
  base_url = config.get("openweathermap", "base_url")
  id_company = config.get("openweathermap", "id_company")
  days_ago = config.get("openweathermap", "days_ago")
  return lon,lat,api_key,base_url,id_company,days_ago    
  
def get_weather_data():
    try:
    #    api_key = "fdb021fe937c62a0353524244ec00533"
    #    base_url = "https://api.openweathermap.org/data/2.5/onecall/timemachine"
    #    id_company = 1
     #   lon = 36.610279 #Географические координаты Обнинска (широта)
     #   lat = 55.096943 #Географические координаты Обнинска (долгота)
        
        config = readConfig2(path)
        lon = config[0]
        lat = config[1]
        api_key = config[2]
        base_url = config[3]
        id_company = config[4]
        days_ago = int(config[5])
        
        utc_today = datetime.utcnow()
        utc_yesterday = utc_today - timedelta(days = days_ago)
        dt = unix_utc_yesterday = int(utc_yesterday.timestamp())
        response = requests.get(base_url,
           params={'lat': lat, 'lon': lon, 'units': 'metric', 'dt': dt, 'appid': api_key})
        data = response.json()

        sunrise = data['current']['sunrise'] #Время восхода, Unix, UTC
        sunset = data['current']['sunset'] #Время заката, Unix, UTC
        for i in data['hourly']:
                unix_datetime = i['dt'] #Время исторических данных, Unix, UTC
                date_value = time.strftime("%Y-%m-%d %H:%M", time.localtime(unix_datetime)) #Замените time.localtime на time.gmtime для GMT даты.
                temp = '{0:+3.1f}'.format(i['temp']) #Температура. Единицы - метрическая система: Цельсий
                feels_like = '{0:+3.0f}'.format(i['feels_like'])#Температура. Это объясняет человеческое восприятие погоды. Единицы - метрическая система: Цельсий
                pressure = i['pressure'] #Атмосферное давление на уровне моря, гПа
                humidity = i['humidity'] #Влажность, %
                wind_speed = i['wind_speed'] #Скорость ветра. Скорость ветра. Единицы - метрическая система: метр / сек
                wind_deg = i['wind_deg'] #Направление ветра, градусы (метеорологические)
                wind_dir = converting_degrees_to_letters(wind_deg)
                weather = i['weather'][0]['description'] #Погодные условия
                
    #            print('unix=',unix_datetime, 'дата=',date_value, 'температура=',temp, 'давление=',pressure, 'влажность=',humidity, 
    #              'скорость ветра=',wind_speed, 'направление ветра=',wind_deg, '', wind_dir, 'погода=',weather)
                insert_air_temperature(id_company,date_value,temp,wind_dir,wind_deg,wind_speed,humidity,pressure)  

    except Exception as e:
        print("Exception (weather):", e)
        pass

def converting_degrees_to_letters(wind_deg):
    return {
    0 <= wind_deg < 23: 'N',
    23 <= wind_deg < 68: 'NE',
    68 <= wind_deg < 113: 'E',
    113 <= wind_deg < 158: 'SE',
    158 <= wind_deg < 203: 'S',
    203 <= wind_deg < 248: 'SW',
    248 <= wind_deg < 293: 'W',
    293 <= wind_deg < 338: 'NW',
    338 <= wind_deg <= 360: 'N'
    }[True]

def insert_air_temperature(id_company, date_value, temperature_indication, wind_direction, wind_direction_grad, wind_speed, humidity , pressure):
  sql_query = """ INSERT INTO cnt.air_temperature(
            id_company, date_value, temperature_indication, wind_direction, 
            wind_direction_grad, wind_speed, humidity , pressure)
    VALUES ( %s, %s, %s, %s, %s, %s, %s, %s);"""
  cur = conn.cursor()
  cur.execute(sql_query, (id_company, date_value, temperature_indication, wind_direction, wind_direction_grad, wind_speed, humidity , pressure))
  conn.commit()
  cur.close()
  #print('добавлено в БД: ', id_company, date_value, temperature_indication, wind_direction, wind_direction_grad, wind_speed, humidity , pressure)

if __name__ == "__main__":
  path = "config.txt"
  config = readConfig(path)
  conn = create_connection(config[0],config[1],config[2],config[3],config[4])
  get_weather_data()
  conn.close()