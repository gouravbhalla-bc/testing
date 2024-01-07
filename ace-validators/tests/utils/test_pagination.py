import math
import pytest

from altonomy.ace.common.pagination import PaginateUtil
from altonomy.ace.daos import base_dao
from tests.test_helpers.stub_models import Base
from tests.test_helpers.utils import clear_model
from tests.test_helpers.utils import random_int


@pytest.mark.usefixtures("db")
class TestPagination:
    """Pagination"""

    def compare_page(self, page_a, page_b):
        return (
            page_a.page == page_b.page
            and page_a.per_page == page_b.per_page
            and page_a.total == page_b.total
            and page_a.items == page_b.items
            and page_a.pages == page_b.pages
            and page_a.prev_num == page_b.prev_num
            and page_a.has_prev == page_b.has_prev
            and page_a.has_next == page_b.has_next
            and page_a.next_num == page_b.next_num
        )

    def test_get_pages_total_size_100(self, db) -> None:
        """should give correct pages when totoal size = 100, page size = 40"""
        clear_model(Base, db)
        curr_base_dao = base_dao.BaseDao(db, Base)
        total_size = random_int() % 1000
        page_size = random_int() % 100
        while page_size == 0:
            page_size = random_int() % 100
        for x in range(0, total_size):
            curr_base_dao.create(Base())
        query = db.query(Base)
        paginate_util = PaginateUtil(query)
        page = paginate_util.paginate(page=1, per_page=page_size)
        assert page.pages == math.ceil(total_size / page_size)

    def test_get_pages_total_size_0(self, db) -> None:
        """should return 0 for pages when total size is 0"""
        clear_model(Base, db)
        curr_base_dao = base_dao.BaseDao(db, Base)
        for x in range(0, 0):
            curr_base_dao.create(Base())
        query = db.query(Base)
        paginate_util = PaginateUtil(query)
        page = paginate_util.paginate(page=1, per_page=40)

        assert page.pages == 0

    def test_get_pages_page_size_0(self, db) -> None:
        """should return 0 for pages when page size is 0"""
        clear_model(Base, db)
        curr_base_dao = base_dao.BaseDao(db, Base)
        total_size = 10
        page_size = 0
        for x in range(0, total_size):
            curr_base_dao.create(Base())
        query = db.query(Base)
        paginate_util = PaginateUtil(query)
        page = paginate_util.paginate(page=1, per_page=page_size)
        assert page.pages == 0

    def test_has_prev_happy_path(self, db) -> None:
        """should return prev page present"""
        clear_model(Base, db)
        curr_base_dao = base_dao.BaseDao(db, Base)
        for x in range(0, 100):
            curr_base_dao.create(Base())
        query = db.query(Base)
        paginate_util = PaginateUtil(query)
        page = paginate_util.paginate(page=2, per_page=40)
        assert page.has_prev

    def test_has_prev_sad_path(self, db) -> None:
        """should return prev page not present"""
        clear_model(Base, db)
        curr_base_dao = base_dao.BaseDao(db, Base)
        for x in range(0, 100):
            curr_base_dao.create(Base())
        query = db.query(Base)
        paginate_util = PaginateUtil(query)
        page = paginate_util.paginate(page=1, per_page=40)
        assert not page.has_prev

    def test_prev_num_happy_path(self, db) -> None:
        """should return prev page num"""
        clear_model(Base, db)
        curr_base_dao = base_dao.BaseDao(db, Base)
        for x in range(0, 100):
            curr_base_dao.create(Base())
        query = db.query(Base)
        paginate_util = PaginateUtil(query)
        page = paginate_util.paginate(page=2, per_page=40)
        assert page.prev_num == 1

    def test_prev_num_sad_path(self, db) -> None:
        """should not return prev page num when it does not have previous page"""
        clear_model(Base, db)
        curr_base_dao = base_dao.BaseDao(db, Base)
        for x in range(0, 100):
            curr_base_dao.create(Base())
        query = db.query(Base)
        paginate_util = PaginateUtil(query)
        page = paginate_util.paginate(page=1, per_page=40)
        assert page.prev_num is None

    def test_prev_happy_path(self, db) -> None:
        """should give correct prev page"""
        clear_model(Base, db)
        curr_base_dao = base_dao.BaseDao(db, Base)
        for x in range(0, 100):
            curr_base_dao.create(Base())
        query = db.query(Base)
        paginate_util = PaginateUtil(query)
        page = paginate_util.paginate(page=2, per_page=40)
        assert self.compare_page(
            page.prev(), paginate_util.paginate(page=1, per_page=40)
        )

    def test_prev_sad_path(self, db) -> None:
        """should give correct prev page"""
        clear_model(Base, db)
        curr_base_dao = base_dao.BaseDao(db, Base)
        for x in range(0, 100):
            curr_base_dao.create(Base())
        query = db.query(Base)
        paginate_util = PaginateUtil(query)
        page = paginate_util.paginate(page=1, per_page=40)

        try:
            page.prev()
            assert False, "should throw error when index out of bounds"
        except ValueError as e:
            assert str(e) == "Page number should be greater than or equal to 1"

    def test_has_next_happy_path(self, db) -> None:
        """should return next page present"""
        clear_model(Base, db)
        curr_base_dao = base_dao.BaseDao(db, Base)
        for x in range(0, 100):
            curr_base_dao.create(Base())
        query = db.query(Base)
        paginate_util = PaginateUtil(query)
        page = paginate_util.paginate(page=2, per_page=40)
        assert page.has_next

    def test_has_next_sad_path(self, db) -> None:
        """should return next page not present"""
        clear_model(Base, db)
        curr_base_dao = base_dao.BaseDao(db, Base)
        for x in range(0, 100):
            curr_base_dao.create(Base())
        query = db.query(Base)
        paginate_util = PaginateUtil(query)
        page = paginate_util.paginate(page=3, per_page=40)
        assert not page.has_next

    def test_next_num_happy_path(self, db) -> None:
        """should return next page num"""
        clear_model(Base, db)
        curr_base_dao = base_dao.BaseDao(db, Base)
        for x in range(0, 100):
            curr_base_dao.create(Base())
        query = db.query(Base)
        paginate_util = PaginateUtil(query)
        page = paginate_util.paginate(page=2, per_page=40)
        assert page.next_num == 3

    def test_next_num_sad_path(self, db) -> None:
        """should not return next page num when it does not have next page"""
        clear_model(Base, db)
        curr_base_dao = base_dao.BaseDao(db, Base)
        for x in range(0, 100):
            curr_base_dao.create(Base())
        query = db.query(Base)
        paginate_util = PaginateUtil(query)
        page = paginate_util.paginate(page=3, per_page=40)
        assert page.next_num is None

    def test_next_happy_path(self, db) -> None:
        """should give correct next page"""
        clear_model(Base, db)
        curr_base_dao = base_dao.BaseDao(db, Base)
        for x in range(0, 100):
            curr_base_dao.create(Base())
        query = db.query(Base)
        paginate_util = PaginateUtil(query)
        page = paginate_util.paginate(page=2, per_page=40)
        assert self.compare_page(
            page.next(), paginate_util.paginate(page=3, per_page=40)
        )

    def test_prev_next_path(self, db) -> None:
        """should give correct next page"""
        clear_model(Base, db)
        curr_base_dao = base_dao.BaseDao(db, Base)
        for x in range(0, 100):
            curr_base_dao.create(Base())
        query = db.query(Base)
        paginate_util = PaginateUtil(query)
        page = paginate_util.paginate(page=3, per_page=40)

        try:
            page.next()
            assert False, "should throw error when index out of bounds"
        except Exception as e:
            assert str(e) == "Not found"

    def test_iter_pages(self, db) -> None:
        """should give valid iterator"""
        clear_model(Base, db)
        curr_base_dao = base_dao.BaseDao(db, Base)
        for x in range(0, 100):
            curr_base_dao.create(Base())
        query = db.query(Base)
        paginate_util = PaginateUtil(query)
        page = paginate_util.paginate(page=3, per_page=40)
        page_iter = page.iter_pages()

        for correct_page_num in range(1, 4):
            iter_value = next(page_iter)
            assert iter_value == correct_page_num

        try:
            next(page_iter)
            assert False, "should stop iteration when reached max page"
        except StopIteration:
            assert True, "successful handling of max page reached"

    def test_paginate_handle_empty_page(self, db) -> None:
        """should handle empty page for paginate"""
        clear_model(Base, db)
        curr_base_dao = base_dao.BaseDao(db, Base)
        for x in range(0, 100):
            curr_base_dao.create(Base())
        query = db.query(Base)
        paginate_util = PaginateUtil(query)
        page = paginate_util.paginate(per_page=40)

        assert page.page == 1

    def test_paginate_handle_empty_per_page(self, db) -> None:
        """should handle empty page for paginate"""
        clear_model(Base, db)
        curr_base_dao = base_dao.BaseDao(db, Base)
        for x in range(0, 100):
            curr_base_dao.create(Base())
        query = db.query(Base)
        paginate_util = PaginateUtil(query)
        page = paginate_util.paginate(page=1)

        assert page.per_page == 20

    def test_paginate_sad_path_invalid_per_page(self, db) -> None:
        """should handle empty per_page for paginate"""
        clear_model(Base, db)
        curr_base_dao = base_dao.BaseDao(db, Base)
        for x in range(0, 100):
            curr_base_dao.create(Base())
        query = db.query(Base)
        paginate_util = PaginateUtil(query)

        try:
            paginate_util.paginate(page=3, per_page=-100)
            assert False, "should hanlde invalid per_page"
        except ValueError as e:
            assert str(e) == "Per Page should be greater than or equal to 1"

    def test_paginate_sad_path_invalid_page(self, db) -> None:
        """should handle empty per_page for paginate"""
        clear_model(Base, db)
        curr_base_dao = base_dao.BaseDao(db, Base)
        for x in range(0, 100):
            curr_base_dao.create(Base())
        query = db.query(Base)
        paginate_util = PaginateUtil(query)

        try:
            paginate_util.paginate(page=0, per_page=40)
            assert False, "should hanlde invalid page"
        except ValueError as e:
            assert str(e) == "Page number should be greater than or equal to 1"

        try:
            paginate_util.paginate(page=-100, per_page=40)
            assert False, "should hanlde invalid page"
        except ValueError as e:
            assert str(e) == "Page number should be greater than or equal to 1"
