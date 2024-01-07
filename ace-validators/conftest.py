
import json
import pytest
import requests

from altonomy.ace.db.base_class import Base
from altonomy.ace.db.deps import get_db
from altonomy.ace.main import app
from fastapi.testclient import TestClient
from sqlalchemy.orm.session import close_all_sessions
from sqlalchemy_utils import create_database
from sqlalchemy_utils import database_exists
from tests.test_helpers.init_test_db import init_db
from tests.test_helpers.test_sessions import TestingSessionLocal
from tests.test_helpers.test_sessions import test_db_string
from tests.test_helpers.test_sessions import test_engine
from typing import Generator


# Make sure test db empty before setting up
# @hookspec(historic=True)
def pytest_configure(config):
    print("Setting up test db")
    if not database_exists(test_db_string):
        create_database(test_db_string)
    init_db()


# #Make sure test db empty before tearing down
def pytest_unconfigure(config):
    print("Tearing downp test db")
    # Drop influx db

    close_all_sessions()
    Base.metadata.drop_all(bind=test_engine)


def pytest_itemcollected(item):
    par = item.parent.obj
    node = item.obj
    pref = par.__doc__.strip() if par.__doc__ else par.__class__.__name__
    suf = node.__doc__.strip() if node.__doc__ else node.__name__
    if pref or suf:
        item._nodeid = ' '.join((pref, suf))


@pytest.fixture(scope="session", autouse=True)
def client() -> Generator:
    """Setting up app"""

    def override_get_db():
        try:
            db = TestingSessionLocal()
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as c:
        yield c


@pytest.fixture(scope="session")
def db() -> Generator:

    yield TestingSessionLocal()


@pytest.fixture(scope="session")
def alt_auth_token() -> Generator:

    alt_auth_token = {
        "alt-auth-token":  "mock-key"
    }

    return alt_auth_token
