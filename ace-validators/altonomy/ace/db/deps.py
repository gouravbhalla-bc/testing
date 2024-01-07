from typing import Generator
from altonomy.ace.db.sessions import SessionLocal


def get_db() -> Generator:
    try:
        db = SessionLocal()
        yield db
    finally:
        db.close()
