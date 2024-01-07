from datetime import datetime
from typing import Optional
from pydantic import BaseModel, validator

from altonomy.ace.enums import FeedType, TransferType


# WorkflowTask
class WorkflowTaskReturnData(BaseModel):
    id: int
    execution_id: int
    workflow_name: str
    task_name: str
    description: str
    maker_id: int
    checker_id: int
    stage: int
    total_step: int
    data: dict
    extra: dict
    status: str
    task_type: str
    create_time: float
    update_time: float = None


class ManualFeedV2Input(BaseModel):
    feed_type: FeedType
    portfolio: str
    transfer_type: TransferType
    product: str
    contract: Optional[str] = None
    asset: str
    coa_code: int = -1
    comp_code: Optional[str] = None
    amount: float
    counterparty_ref: str
    counterparty_name: str
    account: Optional[str] = None
    entity: str
    value_date: datetime
    trade_date: datetime

    @validator("value_date", "trade_date")
    def value_date_must_be_in_past(cls, v):
        if v > datetime.utcnow():
            raise ValueError("must be in the past (UTC)")
        return v

    @validator("contract")
    def contract_defined_for_futures(cls, v, values):
        if values.get("product") == "Futures":
            if v is None or len(v) == 0:
                raise ValueError("contract must be defined for futures")
        return v

    @validator("asset")
    def asset_should_be_in_contract(cls, v, values):
        if (
            values.get("product") == "Futures"
            and values.get("contract") is not None
            and v not in values.get("contract")
        ):
            raise ValueError("contract asset mismatch")
        return v


class Balance(BaseModel):
    account_id: int
    account_name: str
    exchange_name: str
    asset: str
    available: float
    frozen: float
    margin_balance: float
    total: float
    usd_price: float
    usd_available: float
    usd_frozen: float
    usd_margin_balance: float
    usd_total: float
    portfolio_number: str = None
    portfolio_name: str = None
    business_line: str = None
    activity: str = None
    sub_activity: str = None
    strategy: str = None
    function: str = None
