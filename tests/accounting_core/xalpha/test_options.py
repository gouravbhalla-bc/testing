import pytest

from altonomy.ace.accounting_core.xalpha import options


class TestXalphaRuleOptions:

    @pytest.fixture
    @pytest.mark.xfail(reason="TODO")
    def feeds(self, context):
        deal = {}
        out_feeds = options(context, deal)
        assert len(out_feeds) > 0

    @pytest.mark.xfail(reason="TODO")
    def test_success(self, feeds):
        assert len(feeds) > 0
