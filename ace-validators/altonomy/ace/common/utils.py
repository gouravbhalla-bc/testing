import json
from datetime import datetime
from decimal import Decimal
import pytz
from altonomy.ace.db.base_class import Base


def serialize_dict(data: dict):
    _data = {}
    for k in data:
        v = data[k]
        if isinstance(v, datetime):
            v = v.timestamp()
        _data.update({
            k: v
        })
    return _data


def row_to_dict(row: Base):
    return {
        col.name: getattr(row, col.name)
        for col in row.__table__.columns
    }


def row_to_json(row: Base):
    d = {}

    for k, v in row_to_dict(row).items():
        v_type = type(v)
        if v_type == datetime:
            v = v.replace(tzinfo=pytz.UTC).timestamp()
        elif v_type == Decimal:
            v = float(v)
        d[k] = v

    return json.dumps(d)


def empty_str_to_none(v: str):
    if v is not None and len(v) > 0:
        return v
    return None


def convert_to_float(v: str):
    if v is not None:
        return float(v)
    return None


def round_2f(v: float):
    return "{:.2f}".format(v)


def round_6f(v: float):
    return "{:.6f}".format(v)
