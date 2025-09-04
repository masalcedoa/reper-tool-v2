from pydantic_settings import BaseSettings
class Settings(BaseSettings):
    DB_HOST:str="localhost"; DB_PORT:int=5432; DB_USER:str="postgres"; DB_PASSWORD:str="postgres"; DB_NAME:str="frauddb"
    REDIS_URL:str="redis://localhost:6379/0"
    S3_ENDPOINT:str|None=None; S3_ACCESS_KEY:str|None=None; S3_SECRET_KEY:str|None=None; S3_REGION:str|None="us-east-1"; S3_BUCKET:str|None=None; S3_USE_SSL:bool=False
    API_HOST:str="0.0.0.0"; API_PORT:int=8000
    class Config: env_file=".env"
settings=Settings()
