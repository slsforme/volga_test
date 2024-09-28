import aiohttp
import asyncio
from typing import Optional
from openpyxl import Workbook
from datetime import datetime
import aioconsole
from utils import (
    convert_degrees_to_direction,
    convert_weather_code_to_string,
)  
from settings import LOGGER
from db import create_and_add_info, get_all_weather_info

event = asyncio.Event()
menu_running = False  


async def get_data() -> Optional[str]:
    """
    Получает данные с API и возвращает результат в
    зависимости от статуса запроса.

    :return: Если запрос успешен (status code 200),
             возвращает данные в формате JSON в виде строки.
             Если код статуса >= 500 или происходит ошибка,
             возвращает None.
    :rtype: Optional[str]
    """
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
        async with aiohttp.ClientSession() as session:
            async with session.get(URL) as response:
                if response.status == 200:
                    data: str = await response.json()
                    return data
                elif response.status >= 500:
                    return None
                else:
                    return None
    except Exception as e:
        LOGGER.error(f"Error occurred while fetching data: {e}")
        return None


async def parse_data() -> None:
    """
    Получает и обрабатывает данные из API, сохраняет их в БД. 
    При успешном завершении вызывает корутину 
    `menu()` и устанавливает событие `event`.

    :param: Нет параметров.
    :return: Нет возвращаемого значения.
    """
    global menu_running  
    while True:
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
                    precipation=precipitation, 
                    current_weather=current_weather
                )
                LOGGER.info(f"Data was parsed and saved to DB:"
                " (info)")

                if not menu_running:
                    menu_running = True
                    asyncio.create_task(menu())  

        except Exception as e:
            LOGGER.error(f"Error occurred in parse_data: {e}")
        finally:
            event.set()
            await asyncio.sleep(60)


async def export_data_to_xlsx() -> None:
    """
    Экспортирует данные из БД в .xlsx файл. 
    Название файла содержит дату и время создания.

    :param: Нет параметров.
    :return: Нет возвращаемого значения.
    """
    try:
        wb = Workbook()
        ws = wb.active
        now = datetime.now()
        
        ws.title = f"Weather-Data {now.strftime('%Y-%m-%d %H-%M-%S')}"

        weather_info_list = await get_all_weather_info()

        if not weather_info_list:
            LOGGER.warning("No weather data found.")
        
        keys = ['id', 'temperature', 'current_weather', 'direction', 
                'precipation', 'pressure_above_sea',
                'surface_pressure', 'current_date']

        parsed_data = [dict(zip(keys, entry)) for entry in weather_info_list]
        
        last_ten_entries = parsed_data[-10:]

        ws.append(keys)

        for entry in last_ten_entries:
            ws.append([
                entry['id'],
                entry['temperature'],
                entry['current_weather'],
                entry['direction'],
                entry['precipation'],
                entry['pressure_above_sea'],
                entry['surface_pressure'],
                entry['current_date'].strftime("%Y-%m-%d %H-%M-%S")
            ])
        
        wb.save(f"{now.strftime('%Y-%m-%d %H-%M-%S')}.xlsx")
    except Exception as e:
        LOGGER.error("Error occurred while saving data to .xlsx file: "
                     f"{e}")


async def menu() -> None:
    """
    Реализует меню для выбора пользователем действий: 
    экспорт данных в файл или выход из программы.
    Ожидает срабатывания события `event` для отображения меню.

    :param: Нет параметров.
    :return: Нет возвращаемого значения.
    """
    global menu_running  
    while True:
        try:
            await event.wait()
            option = await aioconsole.ainput(
                '(Каждые 60 секунд парсятся данные с API)\nВведите 1,'
                ' если хотите'
                ' импортировать последние 10 записей из БД в файл,\n'
                'Введите 2, если хотите выйти из программы: '
            )
            if option == '1':
                await export_data_to_xlsx()
                print("Данные были сохранены в файл в текущей директории.")
                LOGGER.info("Data was saved to .xlsx file (info)")
            elif option == '2':
                print("Выход из программы.")
                menu_running = False  
                exit()
            else:
                print("Вы ввели не ту цифру.")
            event.clear()
        except Exception as e:
            LOGGER.error(f"Error occurred while fetching data: {e}")


async def main() -> None:
    """
    Основная функция, которая запускает фоновую задачу `parse_data` и 
    ожидает срабатывания события 
    (что означает завершение первой итерации парсинга данных).

    :param: Нет параметров.
    :return: Нет возвращаемого значения.
    """
    asyncio.create_task(parse_data())  
    await asyncio.Event().wait()  


if __name__ == "__main__":
    """
    Запуск основной корутины `main`.
    """
    asyncio.run(main())
