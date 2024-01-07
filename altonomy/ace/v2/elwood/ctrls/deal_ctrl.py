from datetime import datetime
from typing import Dict, List
from altonomy.ace.common.utils import row_to_dict
from altonomy.ace.v2.elwood.daos.deal_dao import DealDao
from altonomy.ace.v2.elwood.models.deal import Deal
from sqlalchemy.orm import Session
from altonomy.ace.v2.elwood.models.elwood_deal import ElwoodDeal


class DealCtrl(object):

    def __init__(self, db: Session):
        self.dao = DealDao(db, Deal)

    def get_deal_by_portfolio_time(
        self,
        portfolios: List[int],
        from_date: datetime,
        to_date: datetime,
    ) -> List[Deal]:
        return self.dao.get_deal_by_portfolio_time(
            portfolios,
            from_date,
            to_date,
        )

    def get_last_deal(self, deal_id: int, creation_date: datetime) -> Deal:
        return self.dao.get_last_deal(deal_id, creation_date)

    def get_deal_by_version(self, deal_id: int) -> Deal:
        return self.dao.get_deal_by_version(deal_id)

    def is_valid_deal(
        self,
        current_deal: ElwoodDeal
    ) -> bool:
        """Discard Child deals
        If value Date is None we will ignore the deal as well
        Ignore pending deals"""
        if (current_deal.deal_type != 'Execution' or
                current_deal.master_deal_id is not None or
                current_deal.value_date is None):
            return False
        return True

    def convert_deal_to_elwood_deal(self, deal: Deal) -> ElwoodDeal:
        deal = row_to_dict(deal)
        deal_type_data = deal["deal_type_data"]
        deal.update(deal_type_data)
        del deal["deal_type_data"]

        columns = dir(ElwoodDeal)

        self.update_deal_type_date_value(deal)
        del_col = [column for column in deal.keys() if column not in columns]

        for column in del_col:
            del deal[column]

        elwood_deal = ElwoodDeal(**deal)
        elwood_deal.parent_id = elwood_deal.id
        elwood_deal.id = None
        return elwood_deal

    def update_deal_type_date_value(self, deal: Dict):
        for column in self.deal_type_date_column():
            # convert timestamp to date
            if column in deal.keys() and deal[column] is not None:
                deal[column] = datetime.utcfromtimestamp(deal[column])

    def deal_type_date_column(self) -> List:
        return ['incoming_settled_date', 'client_settled_date', 'fee_settled_date']

    def get_last_deal_dict(
        self,
        last_deal: List[Deal]
    ) -> Dict:
        deal_dict = {}
        if last_deal is not None and len(last_deal) != 0:
            for deal in last_deal:
                deal_dict[deal.export] = deal
        return deal_dict
