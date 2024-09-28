import asyncio
from sqlalchemy import (
    Column, Integer, Float, String, DateTime, create_engine, MetaData, Table,
    inspect
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime
from typing import Optional, List
from sqlalchemy.future import select
from settings import LOGGER

Base = declarative_base()

DATABASE_URL = "sqlite+aiosqlite:///weather.db"  
engine = create_async_engine(DATABASE_URL, echo=True)

class WeatherInfo(Base):
    """
    Реализация класса таблицы в БД с информацией о погоде.
    """
    __tablename__ = "weather_info"

    id = Column(Integer, primary_key=True)
    temperature = Column(Float)
    direction = Column(String)
    pressure_above_sea = Column(Float)
    surface_pressure = Column(Float)
    precipation = Column(Float)
    current_weather = Column(String)
    current_date = Column(DateTime)

async def create_and_add_info(temperature: float, direction: str,
                            pressure_above_sea: float, surface_pressure: 
                            float, precipation: float, current_weather: str):
    """
    Проверяет наличие таблицы `weather_info` в базе данных и, если её нет,
     создаёт её. Затем добавляет новую запись о погоде в базу данных.

    :param temperature: Температура (в градусах Цельсия).
    :type temperature: float
    :param direction: Направление ветра.
    :type direction: str
    :param pressure_above_sea: Давление на уровне моря (в мм рт. ст.).
    :type pressure_above_sea: float
    :param surface_pressure: Давление на поверхности (в мм рт. ст.).
    :type surface_pressure: float
    :param precipation: Количество осадков (в мм).
    :type precipation: float
    :param current_weather: Описание текущих погодных условий.
    :type current_weather: str
    :return: None
    """
    try:
        async with engine.begin() as conn:
            def check_table_exists(connection):
                return inspect(connection).has_table(
                    WeatherInfo.__tablename__
                    )
            
            table_exists = await conn.run_sync(check_table_exists)
            
            if not table_exists:
                await conn.run_sync(Base.metadata.create_all)
            
            await add_weather_info(temperature, direction, pressure_above_sea,
                                surface_pressure, precipation,
                                current_weather)

    except Exception as e:
        LOGGER.error("Error occurred while getting data from DB: "
        f"{e} (error)")
        
async def add_weather_info(temperature, direction, pressure_above_sea, 
                            surface_pressure, precipation, current_weather):
    """
    Добавляет новую запись о погоде в таблицу `weather_info` 
    в базе данных.

    :param temperature: Температура (в градусах Цельсия).
    :type temperature: float
    :param direction: Направление ветра.
    :type direction: str
    :param pressure_above_sea: Давление на уровне моря (в мм рт. ст.).
    :type pressure_above_sea: float
    :param surface_pressure: Давление на поверхности (в мм рт. ст.).
    :type surface_pressure: float
    :param precipation: Количество осадков (в мм).
    :type precipation: float
    :param current_weather: Описание текущих погодных условий.
    :type current_weather: str
    :return: None
    """
    try:
        async_session = sessionmaker(engine, class_=AsyncSession,
            expire_on_commit=False)
        async with async_session() as session:
            new_weather_entry = WeatherInfo(
                temperature=temperature,
                direction=direction,
                pressure_above_sea=pressure_above_sea,
                surface_pressure=surface_pressure,
                precipation=precipation,
                current_weather=current_weather,
                current_date=datetime.now()  
            )
            session.add(new_weather_entry)
            await session.commit()
    except Exception as e:
        LOGGER.error("Error occurred while getting data from DB: "
        f"{e} (error)")

async def get_all_weather_info() -> Optional[List[WeatherInfo]]:
    """
    Извлекает все записи из таблицы `weather_info` в базе данных.

    :return: Список всех записей о погоде из базы данных. 
        Если возникает ошибка или нет записей, возвращается None.
    :rtype: Optional[List[WeatherInfo]]
    """
    try:
        async_session = sessionmaker(engine, class_=AsyncSession, 
        expire_on_commit=False)
        async with async_session() as session:
            result = await session.execute(
                select(
                    WeatherInfo.id, 
                    WeatherInfo.temperature, 
                    WeatherInfo.current_weather, 
                    WeatherInfo.direction, 
                    WeatherInfo.precipation,
                    WeatherInfo.pressure_above_sea,
                    WeatherInfo.surface_pressure, 
                    WeatherInfo.current_date)
            )
            weather_info_list = result.all()  
            return weather_info_list
    except Exception as e:
        LOGGER.error("Error occurred while getting data from DB: "
        f"{e} (error)")


