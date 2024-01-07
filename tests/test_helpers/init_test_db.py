import altonomy.ace.daos  # noqa
import tests.test_helpers.stub_models  # noqa

from altonomy.ace.db.base_class import Base
from tests.test_helpers.test_sessions import test_engine


def init_db() -> None:
    Base.metadata.create_all(bind=test_engine)
