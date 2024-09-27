import aiohttp
import asyncio
from typing import Dict, Optional, Tuple, List
import asyncio
import aiofiles
from openpyxl import Workbook, load_workbook
from datetime import datetime

from utils import (
    convert_degrees_to_direction, 
    convert_weather_code_to_string,
    run_task_periodically
    )  
from settings import LOGGER
from db import create_and_add_info, get_all_weather_info

async def get_data() -> Optional[str]:
    try:
        URL: str = (
            'https://api.open-meteo.com/v1/forecast?'
            'latitude=55.6878&'
            'longitude=37.3684&'
            'current=temperature_2m,precipitation,weather_code,'
            'pressure_msl,surface_pressure,wind_speed_10m,'
            'wind_direction_10m&'
            'wind_speed_unit=ms&'
            'timezone=Europe%2FMoscow'
        )
        LOGGER.info(f'CURRENT URL: {URL} (info)')
        async with aiohttp.ClientSession() as session:
            async with session.get(URL) as response:
                LOGGER.info(f'Status Code: {response.status} (info)')
                if response.status == 200:
                    LOGGER.info('Content-type:' 
                    f'{response.headers['content-type']}')
                    data: str = await response.json()
                    LOGGER.info(f'Data from API:\n{data} (info)')
                    return data
                elif response.status >= 500:
                    LOGGER.warning(
                        f'Server Error occurred while parsing data.'
                        f'Status code: {response.status}')
                    return None
                else:
                    LOGGER.warning(
                    'Error occurred while parsing data.'
                    f'Status code: {response.status}')
                    return None
    except Exception as e:
        LOGGER.error(
            f'Error occurred while getting data from API:'
            f'\n{e} (error)')
        return None
        
    
async def parse_data():
    try:
        json_data: str = await get_data()
        if json_data is not None:
            temperature: float = json_data["current"]["temperature_2m"]
            windspeed: float = json_data["current"]["wind_speed_10m"]
            direction: str = await convert_degrees_to_direction(
                    json_data["current"]["wind_direction_10m"]
                )
            pressure_above_sea: float = (
                json_data["current"]["pressure_msl"] * 0.75006375541921
            )
            surface_pressure: float = (
                json_data["current"]["surface_pressure"] * 0.75006375541921
            )
            precipitation: float = json_data["current"]["precipitation"]
            current_weather: str = await convert_weather_code_to_string(
                json_data["current"]["weather_code"]
            )
            await create_and_add_info(temperature=temperature, 
                direction=direction,
                pressure_above_sea=pressure_above_sea, 
                surface_pressure=surface_pressure,
                precipation=precipitation,current_weather=current_weather
            )

            weather_info_list = await get_all_weather_info()
            if weather_info_list:
                for object in weather_info_list:
                    for table_name, value in vars(object).items():
                        print(f"{table_name}: {value} (info)")
    except Exception as e:
        LOGGER.error('Error occurred while serializing data from API:'
        f'\n{e} (error)')


async def export_data_to_xlsx():
    wb = Workbook()
    ws = wb.active
    now = datetime.now()
    
    ws.title = f"Weather-Data {now.strftime('%Y-%m-%d %H-%M-%S')}"  

    weather_info_list = await get_all_weather_info()
    last_ten_entries = weather_info_list[-10:]  
    
    attributes_list = [[value for value in vars(entry).values()] 
    for entry in last_ten_entries]

    ws.append(['id', 'Температура', 'Направление ветра', 
               'Давление в ммрт над уровнем моря',
               'Давление в ммрт на поверхности',
               'Осадки (в мм)', 'Погода', 'Дата'])

    for entry_attributes in attributes_list:
        ws.append([
            entry_attributes[6],  
            entry_attributes[3],  
            entry_attributes[1],  
            entry_attributes[2],  
            entry_attributes[7],  
            entry_attributes[4],  
            entry_attributes[8], 
            entry_attributes[5].strftime("%Y-%m-%d %H-%M-%S"),
        ])
    
    wb.save(f"{now.strftime('%Y-%m-%d %H-%M-%S')}.xlsx")

async def menu():
    while True:
        try:
            option = int(input('Введите 1, если хотите импортировать последние 10 записей из БД в файл,\n'
                               'Введите 2, если хотите выйти из программы: '))
            if option != 1 and option != 2:  
                print("Вы ввели не ту цифру.")
                continue  
            if option == 1:
                await export_data_to_xlsx()  
                print("Данные были сохранены в файл в текущей директории.")
                LOGGER.info("Data was saved to .xlsx file (info)")
            elif option == 2:
                print("Выход из программы.")
                break  
        except ValueError:
            print("Вы ввели не число. Повторите попытку.")
            LOGGER.error("User input is not a number (error)")
        except Exception as e:
            print("Произошла ошибка. Попробуйте ещё раз.")
            LOGGER.error("Error occurred while user was inputting number: %s", e)


async def main():
    await menu()
    asyncio.create_task(run_task_periodically(300, parse_data))
    await asyncio.sleep(3600)  # Получение данных с API будет происходить
                               # в течении одного часа


if __name__ == "__main__":
    asyncio.run(main())