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
    try:
        async with engine.begin() as conn:
            def check_table_exists(connection):
                return inspect(connection).has_table(
                    WeatherInfo.__tablename__
                    )
            
            table_exists = await conn.run_sync(check_table_exists)
            
            if not table_exists:
                await conn.run_sync(Base.metadata.create_all)
                LOGGER.info("Таблица 'weather_info' создана.")
            
            await add_weather_info(temperature, direction, pressure_above_sea, 
                                surface_pressure, precipation, current_weather)
            LOGGER.info("Новые данные были добавлены в WeatherInfo")

    except Exception as e:
        LOGGER.error("Error occurred while working with table WeatherInfo: "
                     f"{e}")

async def add_weather_info(temperature, direction, pressure_above_sea, 
                            surface_pressure, precipation, current_weather):
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
            LOGGER.info("Данные о погоде успешно добавлены.")
    except Exception as e:
        LOGGER.error("Error occurred while adding info to table WeatherInfo:"
        f" {e}")


async def get_all_weather_info() -> Optional[List[WeatherInfo]]:
    try:
        async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with async_session() as session:
            result = await session.execute(select(WeatherInfo))  
            weather_info_list: List[WeatherInfo] = result.scalars().all()  
            return weather_info_list
    except Exception as e:
        LOGGER.error(f"Error occurred while fetching data from table WeatherInfo: {e}")
        return None

