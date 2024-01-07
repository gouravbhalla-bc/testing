from datetime import datetime

import pytest
from altonomy.ace.accounting_core.utils import DealContextRule
from altonomy.ace.models import SystemFeed


class BaseTestXalpha:
    '''
    This set of tests should check the data correctness of the generated feeds

    Usage: Create a test class and inherit from this class, overriding the feeds fixture
    '''

    @pytest.fixture(scope="class")
    def feeds(self):
        return []

    @pytest.fixture(scope="class")
    def context(self):
        new_feeds = []
        current_feeds = []
        FeedType = SystemFeed
        return DealContextRule(new_feeds, current_feeds, FeedType)

    def test_portfolio_only_one(self, feeds):
        """portfolio value for all feeds is the same"""
        if len(feeds) == 0:
            return

        portfolios = set(feed.portfolio for feed in feeds)
        assert len(portfolios) == 1

    def test_portfolio_value(self, feeds):
        """portfolio value is not None"""
        assert all(feed.portfolio is not None for feed in feeds)

    def test_entity(self, feeds):
        """entity value is not None"""
        assert all(feed.entity is not None for feed in feeds)

    def test_comp_code_unique(self, feeds):
        """comp_code value for all feeds are unique"""
        comp_codes = [feed.comp_code for feed in feeds]
        assert len(comp_codes) == len(set(comp_codes))

    def test_feed_type(self, feeds):
        """feed_type is either Cash or PV"""
        feed_types = ("Cash", "PV")
        assert all(feed.feed_type in feed_types for feed in feeds)

    def test_asset(self, feeds):
        """asset is not None"""
        assert all(feed.asset is not None for feed in feeds)

    def test_record_type(self, feeds):
        """record_type is either CREATE or DELETE"""
        record_types = ("CREATE", "DELETE")
        assert all(feed.record_type in record_types for feed in feeds)

    def test_value_date(self, feeds):
        """value_date is a date"""
        assert all(type(feed.value_date) is datetime for feed in feeds)
