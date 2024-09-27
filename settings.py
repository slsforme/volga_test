import loguru

LOGGER = loguru.logger
LOGGER.add("debug.log", format="{time} {level} {message}",
    level="DEBUG", rotation="1 GB", compression='zip')