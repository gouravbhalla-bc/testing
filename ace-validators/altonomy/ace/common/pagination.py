from math import ceil


class Pagination(object):

    def __init__(self, paginate_util, page, per_page, total, items):
        self.paginate_util = paginate_util
        self.page = page
        self.per_page = per_page
        self.total = total
        self.items = items

    @property
    def pages(self):
        """The total number of pages"""
        if self.per_page == 0:
            pages = 0
        else:
            pages = int(ceil(self.total / float(self.per_page)))
        return pages

    def prev(self):
        assert self.paginate_util.query is not None
        return self.paginate_util.paginate(self.page - 1, self.per_page)

    @property
    def prev_num(self):
        """Number of the previous page."""
        if not self.has_prev:
            return None
        return self.page - 1

    @property
    def has_prev(self):
        """True if a previous page exists"""
        return self.page > 1

    def next(self):
        assert self.paginate_util.query is not None
        return self.paginate_util.paginate(self.page + 1, self.per_page)

    @property
    def has_next(self):
        """True if a next page exists."""
        return self.page < self.pages

    @property
    def next_num(self):
        """Number of the next page"""
        if not self.has_next:
            return None
        return self.page + 1

    def iter_pages(self, left_edge=2, left_current=2,
                   right_current=5, right_edge=2):
        last = 0
        for num in range(1, self.pages + 1):
            if num <= left_edge or (num > self.page - left_current - 1 and num < self.page + right_current) or num > self.pages - right_edge:
                if last + 1 != num:
                    yield None
                yield num
                last = num


class PaginateUtil(object):
    def __init__(self, query):
        self.query = query

    def paginate(self, page=None, per_page=None, order=None):
        if page is None:
            page = 1
        if per_page is None:
            per_page = 20
        if page < 1:
            raise ValueError("Page number should be greater than or equal to 1")
        if per_page < 0:
            raise ValueError("Per Page should be greater than or equal to 1")

        items = self.query.order_by(order).limit(per_page).offset((page - 1) * per_page).all()
        if not items and page != 1:
            raise Exception("Not found")

        total = self.query.order_by(order).count()
        return Pagination(self, page, per_page, total, items)
