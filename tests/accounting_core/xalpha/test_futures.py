import pytest

from altonomy.ace.accounting_core.xalpha import futures


class TestXalphaRuleFutures:

    @pytest.fixture
    @pytest.mark.xfail(reason="TODO")
    def feeds(self, context):
        deal = {}
        out_feeds = futures(context, deal)
        assert len(out_feeds) > 0

    @pytest.mark.xfail(reason="TODO")
    def test_success(self, feeds):
        assert len(feeds) > 0
