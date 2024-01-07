from datetime import datetime
from typing import List, Optional

from altonomy.ace.daos.base_dao import BaseDao
from altonomy.ace.v2.elwood.models.deal import Deal


class DealDao(BaseDao[Deal]):
    def get_deal_by_portfolio_time(
        self,
        portfolios: List[int],
        from_date: datetime,
        to_date: datetime,
    ) -> List[Deal]:
        q = (
            self.db.query(self.model)
            .filter(
                self.model.portfolio_number.in_(portfolios),
                self.model.valid_from >= from_date,
                self.model.valid_from < to_date
            )
            .order_by(
                self.model.valid_from,
            )
        )

        page_size = 1000
        pages = int(q.count() / page_size) + 1

        for page in range(pages):
            result = q.limit(page_size).offset(page * page_size).all()
            print("result", len(result))
            yield result

    def get_last_deal(
        self,
        deal_id: int,
        creation_data: datetime
    ) -> Optional[Deal]:
        return (
            self.db.query(self.model)
            .filter(
                self.model.deal_id == deal_id,
                self.model.valid_from <= creation_data
            )
            .order_by(
                self.model.valid_from.desc(),
            )
        ).first()

    def get_deal_by_version(
        self,
        deal_id: int
    ) -> Optional[Deal]:
        return (
            self.db.query(self.model)
            .filter(
                self.model.deal_id == deal_id,
                self.model.version == 1
            )
            .order_by(
                self.model.valid_from.desc(),
            )
        ).first()
