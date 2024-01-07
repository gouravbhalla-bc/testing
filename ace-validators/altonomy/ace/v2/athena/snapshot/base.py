from datetime import datetime
from typing import Generic, List, Optional, TypeVar
from itertools import chain

Item = TypeVar('Item')
Snapshot = TypeVar('Snapshot')


class Snapshot(Generic[Snapshot, Item]):

    def __init__(self, db, item_dao, snapshot_dao, manual_item_dao=None) -> None:
        self.db = db
        self.item_dao = item_dao
        self.snapshot_dao = snapshot_dao
        self.reset()
        self.reset_snapshot()
        self.manual_item_dao = manual_item_dao

    def read_cached_snapshot(self, cached_snapshot: Snapshot) -> None:
        raise NotImplementedError()

    def get_snapshot_filters(self):
        raise NotImplementedError()

    def process_item(self, item: Item) -> None:
        raise NotImplementedError()

    def get_item_filters(self):
        raise NotImplementedError()

    def get_manual_item_filters(self):
        raise NotImplementedError()

    def get_value(self) -> dict:
        raise NotImplementedError()

    def create_snapshot(self) -> Snapshot:
        raise NotImplementedError()

    def reset(self) -> None:
        raise NotImplementedError()

    def is_equal_snapshot(self, snapshot: Snapshot) -> bool:
        raise NotImplementedError()

    def pre_load(self, trade_date: datetime, effective_date: datetime) -> None:
        pass

    def post_load(self, trade_date: datetime, effective_date: datetime) -> None:
        pass

    def get_previous_version_snapshot(
        self,
        trade_date: datetime,
    ) -> Optional[Snapshot]:
        return self.snapshot_dao.get_previous_version_snapshot(self.get_snapshot_filters(), trade_date)

    def get_cached_snapshot(
        self,
        trade_date: datetime,
        effective_date: datetime,
    ) -> Optional[Snapshot]:
        return self.snapshot_dao.get_cached_snapshot(self.get_snapshot_filters(), trade_date, effective_date)

    def get_previous_cached_snapshot(
        self,
        trade_date: datetime,
        effective_date: datetime,
    ) -> Optional[Snapshot]:
        return self.snapshot_dao.get_previous_cached_snapshot(self.get_snapshot_filters(), trade_date, effective_date)

    def get_items(
        self,
        trade_date_start: datetime,
        trade_date_end: datetime,
        effective_date: datetime,
    ) -> List[Item]:
        items = self.item_dao.get_filtered_at_trade_date_at_effective_date(
            self.get_item_filters(),
            trade_date_start,
            trade_date_end,
            effective_date,
        )
        if self.manual_item_dao is not None:
            # concat here.
            manual_items = self.manual_item_dao.get_filtered_at_trade_date_at_effective_date(
                self.get_manual_item_filters(),
                trade_date_start,
                trade_date_end,
                effective_date,
            )
            return chain(items, manual_items)
        return items

    def reset_snapshot(self) -> None:
        self.trade_date = None
        self.effective_date = None
        self.cached_snapshot = None
        self.system_load_start_date = datetime.utcnow()

    def save(self) -> bool:
        prev_snapshot = self.get_previous_version_snapshot(self.trade_date)

        if (
            prev_snapshot is not None and
            self.trade_date == prev_snapshot.trade_date and
            self.is_equal_snapshot(prev_snapshot)
        ):
            return False

        version = 1
        if prev_snapshot is not None:
            version = prev_snapshot.version + 1
            prev_snapshot.effective_date_end = self.effective_date
            self.snapshot_dao.update(prev_snapshot)

        snapshot: Snapshot = self.create_snapshot()
        snapshot.ref_snapshot = self.cached_snapshot.id if self.cached_snapshot is not None else None
        snapshot.version = version
        snapshot.system_load_start_date = self.system_load_start_date
        snapshot.trade_date = self.trade_date
        snapshot.effective_date_start = self.effective_date
        snapshot.effective_date_end = None

        self.snapshot_dao.create(snapshot)
        return True

    def process_items(
        self,
        items: List[Item],
    ) -> None:
        for item in items:
            self.process_item(item)

    def load_items(
        self,
        trade_date_start: datetime,
        trade_date_end: datetime,
        effective_date: datetime,
    ) -> None:
        self.trade_date = trade_date_end
        self.effective_date = effective_date

        self.pre_load(self.trade_date, self.effective_date)

        items = self.get_items(trade_date_start, trade_date_end, effective_date)
        self.process_items(items)

        self.post_load(self.trade_date, self.effective_date)

    def load_from_cached(self, cached_snapshot: Snapshot) -> None:
        self.cached_snapshot = cached_snapshot
        if self.cached_snapshot is None:
            self.reset()
            self.reset_snapshot()
        else:
            self.read_cached_snapshot(self.cached_snapshot)

    def load(
        self,
        trade_date: datetime,
        effective_date: datetime,
    ) -> None:
        self.system_load_start_date = datetime.utcnow()

        cached_snapshot = self.get_cached_snapshot(trade_date, effective_date)
        self.load_from_cached(cached_snapshot)

        trade_date_start = datetime.min if self.cached_snapshot is None else self.cached_snapshot.trade_date
        self.load_items(trade_date_start, trade_date, effective_date)

    def load_2(
        self,
        trade_date: datetime,
        effective_date: datetime,
    ) -> None:
        self.system_load_start_date = datetime.utcnow()

        cached_snapshot = self.get_previous_cached_snapshot(trade_date, effective_date)
        self.load_from_cached(cached_snapshot)

        trade_date_start = datetime.min if self.cached_snapshot is None else self.cached_snapshot.trade_date
        self.load_items(trade_date_start, trade_date, effective_date)
