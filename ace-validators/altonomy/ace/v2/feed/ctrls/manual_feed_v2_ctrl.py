import csv
from typing import Any, Dict, List, Tuple
from datetime import datetime

from pydantic.error_wrappers import ValidationError
from altonomy.ace.common.utils import empty_str_to_none
from altonomy.ace.endpoints.schemas import ManualFeedV2Input
from altonomy.ace.enums import RecordType, SystemSource

from altonomy.ace.v2.feed.daos import ManualFeedV2Dao
from sqlalchemy.orm import Session

from altonomy.ace.v2.feed.models.manual_feed_v2 import ManualFeedV2


class ManualFeedV2Ctrl(object):

    def __init__(self, db: Session):
        self.dao = ManualFeedV2Dao(db, ManualFeedV2)

    def add_from_csv(self, data: str) -> Tuple[Dict[str, Any], int, bool]:
        '''
        Convert csv string in to manual feeds.
        The entire contents is parsed for validation.
        If any fail, we return the failed lines, 0, false.
        Returns a tuple (failed lines, added count, success)
        '''
        invalid_lines = {}
        reader = csv.DictReader(data.splitlines(), dialect=csv.Dialect.delimiter)
        row_counter = 0
        valid: List[ManualFeedV2Input] = []
        for row in reader:
            item, errors = self._validate_csv_line(row)
            if errors is not None:
                invalid_lines["row:" + str(row_counter)] = errors
            else:
                valid.append(item)
            row_counter = row_counter + 1

        if len(invalid_lines.keys()) > 0:
            return invalid_lines, 0, False
        date_fmt = "%Y-%m-%d %H:%M:%S"
        now_string = datetime.strftime(datetime.utcnow(), date_fmt)

        for i in valid:
            new_f = ManualFeedV2(
                system_source=SystemSource.MANUAL,
                version=1,
                record_type=RecordType.CREATE,
                feed_type=i.feed_type,
                portfolio=i.portfolio,
                transfer_type=i.transfer_type,
                contract=empty_str_to_none(i.contract),
                product=i.product,
                coa_code=i.coa_code,
                comp_code=empty_str_to_none(i.comp_code),
                asset=i.asset,
                amount=i.amount,
                asset_price=0,
                counterparty_ref=i.counterparty_ref,
                counterparty_name=i.counterparty_name,
                account=empty_str_to_none(i.account),
                entity=i.entity,
                value_date=datetime.strftime(i.value_date, date_fmt),
                trade_date=datetime.strftime(i.trade_date, date_fmt),
                effective_date_start=now_string,
                effective_date_end=None
            )
            self.dao.create(new_f)
        return None, len(valid), True

    def _validate_csv_line(self, row: Dict) -> Tuple[ManualFeedV2Input, List[Dict[str, Any]]]:
        try:
            f = ManualFeedV2Input(**row)
            return f, None
        except ValidationError as e:
            return None, e.errors()
