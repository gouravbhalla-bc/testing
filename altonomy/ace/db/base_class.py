import re

from typing import Any

from sqlalchemy.ext.declarative import as_declarative
from sqlalchemy.ext.declarative import declared_attr


def camel2snake(name):
    return name[0].lower() + re.sub(r'(?!^)[A-Z]', lambda x: '_' + x.group(0).lower(), name[1:])


@as_declarative()
class Base:
    id: Any
    __name__: str

    @declared_attr
    def __tablename__(cls) -> str:
        return camel2snake(cls.__name__)
