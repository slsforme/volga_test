import loguru

"""
В данном модуле создаётся логгер при помощи сторонней библиотеки 
    loguru и устанавливается в определённый режим.
"""

LOGGER = loguru.logger
LOGGER.add("debug.log", format="{time} {level} {message}",
    level="DEBUG", rotation="1 GB", compression='zip')