from altonomy.ace import config
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

db_string = f"mysql+pymysql://{config.XALPHA_DB_USER}:{config.XALPHA_DB_PASSWORD}@{config.XALPHA_DB_HOST}/{config.XALPHA_DB_DATABASE}"
engine = create_engine(db_string, pool_pre_ping=True, pool_size=100, pool_recycle=3600, max_overflow=50)
XAplphaSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
