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
import logging

logging.basicConfig()
logging.getLogger('sqlalchemy').setLevel(logging.ERROR)

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
            
            await add_weather_info(temperature, direction, pressure_above_sea, 
                                surface_pressure, precipation, current_weather)

    except Exception as e:
        pass 
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
    except Exception as e:
        pass

async def get_all_weather_info() -> Optional[List[WeatherInfo]]:
    try:
        async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
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
        return None


