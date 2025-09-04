from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from .config.settings import settings
def get_engine()->Engine:
    url=f"postgresql+psycopg://{settings.DB_USER}:{settings.DB_PASSWORD}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
    return create_engine(url, pool_pre_ping=True, pool_size=10, max_overflow=20)
def init_db():
    import glob
    eng=get_engine()
    with eng.begin() as con:
        for p in sorted(glob.glob('app/sql/*.sql')):
            sql=open(p,'r',encoding='utf-8').read(); con.exec_driver_sql(sql)
    return eng
