from typing import Tuple, Dict, Coroutine
import asyncio


async def convert_degrees_to_direction(degrees):
    """
    Преобразует направление ветра в градусах
    в строковое обозначение направления.

    :param degrees: Направление ветра в градусах.
    :type degrees: float
    :return: Строковое обозначение направления,
        соответствующее градусам 
    (например, "С" для северного направления).
    :rtype: str
    """
    DIRECTIONS: Tuple = (
        "С",  # Север
        "СВ", # Северо-восток
        "В",  # Восток
        "ЮВ", # Юго-восток
        "Ю",  # Юг
        "ЮЗ", # Юго-запад
        "З",  # Запад
        "СЗ"  # Северо-запад
    )
    index: int = round(degrees / 45) % 8
    return DIRECTIONS[index]

async def convert_weather_code_to_string(code: int) -> str:
    """
    Преобразует код погоды в текстовое описание погодных условий.

    :param code: Код погоды.
    :type code: int
    :return: Текстовое описание погодных условий .
    :rtype: Optional[str]
    """
    WEATHER_STATUSES: Dict = {
        0: "Чистое небо",
        (1, 2, 3): "В основном ясно, частично облачно, и облачно",
        (45, 48): "Туман и оседающий иней",
        (51, 53, 55): "Дождь: легкой, умеренной и сильной интенсивности",
        (56, 57): "Замерзающий дождь: легкой и сильной интенсивности",
        (61, 63, 65): "Дождь: легкой, умеренной и сильной интенсивности",
        (66, 67): "Замерзающий дождь: легкой и сильной интенсивности",
        (71, 73, 75): "Снегопад: легкой, умеренной и сильной интенсивности",
        77: "Снежные крупы",
        (80, 81, 82): "Дождевые дожди: легкие, умеренные и сильные",
        (85, 86): "Снеговые дожди: легкие и сильные",
        95: "Гроза: легкая или умеренная",
        (96, 99): "Гроза с легким и сильным градом"
    }

    for keys, description in WEATHER_STATUSES.items():
        if isinstance(keys, tuple):
            if code in keys:
                return description
        elif code == keys:
            return description
    
    return None


