from altonomy.ace import config
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

db_string = f"mysql+pymysql://{config.DB_USER}:{config.DB_PASSWORD}@{config.DB_HOST_LOCAL}/{config.DB_DATABASE}"
engine = create_engine(db_string, pool_pre_ping=True, pool_size=100, pool_recycle=3600, max_overflow=50)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
