from pathlib import Path
import os
from pydantic_settings import BaseSettings
from pydantic.functional_validators import field_validator
from utils.log.json_stdout_logger import logger

__all__ = ["env_config"]


class Config(BaseSettings):
    """
    this class represent the project configs
    we have to declare the variables same as our .env files
    the point is if we have these tools or config locally we do not need to fill out them in the .env file
    in the config class wee look for the .env file if that found the priority of .env file is upper
    so if we fill same variable in this file and .env the .env will load
    """

    DEBUG: bool = os.getenv('DEBUG', False)
    BROKER_URL: str = os.getenv('BROKER_URL')
    BROKER_TYPE: str = os.getenv('BROKER_TYPE')

    @field_validator('*', mode='before')
    @classmethod
    def strip_environemt(cls, v):
        if isinstance(v, str):
            v = v.strip()
        return v

    class Config:
        case_sensitive = False
        BASE_DIR = Path(__file__).resolve().parent.parent.parent
        env_file = (str(BASE_DIR) + "/.env").replace("//", "/")
        logger.info(env_file)
        env_file_encoding = "utf-8"
        extra = 'allow'


env_config = Config()
