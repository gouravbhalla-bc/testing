import decimal
import random
import string

from altonomy.ace.db.base_class import Base
from datetime import datetime
from datetime import timedelta
from dateutil import tz
from tests.test_helpers.test_sessions import TestingSessionLocal
from uuid import UUID


def clear_model(model, db):
    db.query(model).delete()


def clear_db(db):
    meta = Base.metadata
    for table in reversed(meta.sorted_tables):
        db.execute(table.delete())
    db.commit()


def round_6sf(x):
    # Round number to 6sf to mimic mysql rounding of floats
    round_6sf_x = float("%.6g" % x)
    return round_6sf_x


def compare_dict_key_arr(A, B, skip_keys):
    equal = True
    for i in range(0, len(skip_keys)):
        skip_keys[i] = skip_keys[i].lower()
    if isinstance(A, dict) and isinstance(B, dict):
        for key in A:
            if key.lower() in skip_keys:
                continue
            a_value = A[key]
            if key not in B:
                raise Exception(f"{key} not in {str(B)}")
                equal = False
                break
            b_value = B[key]
            equal = compare_dict_key_arr(a_value, b_value, skip_keys)
        for key in B:
            if key.lower() in skip_keys:
                continue
            if key not in A:
                raise Exception(f"{key} not in {str(A)}")
                equal = False
                break
            a_value = A[key]
            b_value = B[key]
            equal = compare_dict_key_arr(a_value, b_value, skip_keys)
    elif isinstance(A, list) and isinstance(B, list):
        length = len(A)
        if length != len(B):
            raise Exception(
                f"{str(A)} length of {length}does not eq {str(B)} length of {len(B)}"
            )
            equal = False
        else:
            for index in range(0, length):
                equal = compare_dict_key_arr(A[index], B[index], skip_keys)
                if not equal:
                    raise Exception(f"{str(A[index])} not eq {str(B[index])}")
                    break
    elif not isinstance(A, dict) and not isinstance(B, dict):
        equal = A == B
        if not equal:
            raise Exception(f"{str(A)} not equal to {str(B)}")
        return A == B
    else:
        raise Exception(f"Invalid type {A} and {B}")
        return False
    return equal


def compare_dict(A, B, skip_key):
    equal = True
    if isinstance(A, dict) and isinstance(B, dict):
        for key in A:
            if skip_key.lower() in key.lower():
                continue
            a_value = A[key]
            if key not in B:
                raise Exception(f"{key} not in {str(B)}")
                equal = False
                break
            b_value = B[key]
            equal = compare_dict(a_value, b_value, skip_key)
        for key in B:
            if skip_key.lower() in key.lower():
                continue
            if key not in A:
                raise Exception(f"{key} not in {str(A)}")
                equal = False
                break
            a_value = A[key]
            b_value = B[key]
            equal = compare_dict(a_value, b_value, skip_key)
    elif isinstance(A, list) and isinstance(B, list):
        length = len(A)
        if length != len(B):
            raise Exception(
                f"{str(A)} length of {length}does not eq {str(B)} length of {len(B)}"
            )
            equal = False
        else:
            for index in range(0, length):
                equal = compare_dict(A[index], B[index], skip_key)
                if not equal:
                    raise Exception(f"{str(A[index])} not eq {str(B[index])}")
                    break
    elif not isinstance(A, dict) and not isinstance(B, dict):
        equal = A == B
        if not equal:
            raise Exception(f"{str(A)} not equal to {str(B)}")
        return A == B
    else:
        raise Exception(f"Invalid type {A} and {B}")
        return False
    return equal


def is_uuid(uuid_to_test):
    try:
        uuid_obj = UUID(uuid_to_test, version=4)
    except ValueError:
        return False

    return str(uuid_obj) == uuid_to_test


def random_string(length):
    letters = string.ascii_letters
    return "".join(random.choice(letters) for i in range(length))


def random_bool():
    return bool(random.getrandbits(1))


def random_int():
    return random.randint(1, 2147483647)


def random_decimal():
    return round(random.random() + random_int() % 1000, 6)


def random_time_future():
    return datetime.now() + timedelta(seconds=random_int())


def random_time_past():
    return (datetime.now() - timedelta(days=random_int() % 10)).astimezone(
        tz.gettz("UTC")
    )


def sanitize_decimal_value(hash):
    for key in hash:
        value = hash[key]
        if isinstance(value, decimal.Decimal):
            hash[key] = float(value)
    return hash


def random_dict():
    return {
        random_string(3): random_string(3)
    }


def get_test_db():
    return TestingSessionLocal()


def mock_funcion(mocker, path, return_value):
    function_mocker = mocker.patch(path)
    function_mocker.return_value = return_value


def mock_get_jwt_payload(mocker, path, return_value):
    jwt_mocker = mocker.patch(f"{path}.get_jwt_payload")
    jwt_mocker.return_value = return_value
