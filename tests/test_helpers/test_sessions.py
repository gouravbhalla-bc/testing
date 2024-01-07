from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# def _fk_pragma_on_connect(dbapi_con, con_record):
#     dbapi_con.execute("pragma foreign_keys=ON")


# # TODO Should add settings file to handle env variables?
# # https://fastapi.tiangolo.com/advanced/settings/#settings-and-testing
# test_db_string = "sqlite:///./Altonomy_test.db"
# test_engine = create_engine(test_db_string, connect_args={"check_same_thread": False})
# event.listen(test_engine, "connect", _fk_pragma_on_connect)

# TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=test_engine)

# For mysql as test db
from altonomy.ace import config

username = config.DB_USER
password = config.DB_PASSWORD
hostname = config.DB_HOST
db = config.DB_DATABASE

test_db_string = f"mysql+pymysql://{username}:{password}@{hostname}/{db}_test"

test_engine = create_engine(test_db_string, pool_pre_ping=True)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=test_engine)
