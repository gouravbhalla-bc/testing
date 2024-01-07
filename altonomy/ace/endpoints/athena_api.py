from datetime import datetime
from typing import Any, Optional

from altonomy.ace.ctrls.system_feed_ctrl import SystemFeedCtrl
from altonomy.ace.db import deps
from altonomy.ace.in_mem_calculation.cal_manager import CalManager
from fastapi import APIRouter, Depends, Header
from sqlalchemy.orm import Session

router = APIRouter()

_permanent_cal_manager = CalManager()


@router.get("/position/current")
def position_current(
    alt_auth_token: str = Header(None),
    db: Session = Depends(deps.get_db),
    portfolio: str = "",
    portfolio_separate: str = "YES",
) -> Any:
    portfolio = portfolio.split(",") if len(portfolio) > 0 else []
    portfolio_separate = portfolio_separate == "YES"

    cal_manager = _permanent_cal_manager.clone(portfolios=portfolio)

    system_feed_ctrl = SystemFeedCtrl(db)
    trades_today = system_feed_ctrl.get_trade_view_today(portfolio)
    cal_manager.process_trades(trades_today)

    positions = cal_manager.get_position_by_portfolio(portfolio)
    if not portfolio_separate:
        positions = cal_manager.merge_positions(positions)
    return positions


@router.get("/summary/current")
def summary_current(
    alt_auth_token: str = Header(None),
    db: Session = Depends(deps.get_db),
    portfolio: str = "",
    portfolio_separate: str = "YES",
    pinned_assets: Optional[str] = "",
) -> Any:
    portfolios = portfolio.split(",") if len(portfolio) > 0 else []
    portfolio_separate = portfolio_separate == "YES"

    cal_manager = _permanent_cal_manager.clone(portfolios=portfolios)

    system_feed_ctrl = SystemFeedCtrl(db)
    feeds_today = system_feed_ctrl.get_feeds_today(portfolios)
    cal_manager.process_feeds(feeds_today)

    summaries = cal_manager.get_summary_by_portfolio(portfolios)
    if not portfolio_separate:
        summaries = cal_manager.merge_summaries(summaries)
    summaries = system_feed_ctrl.with_ticker_price(summaries, "Asset")

    for summary in summaries:
        summary.update({
            "Position (USD)": summary.get("Position", 0) * summary.get("Last Price", 0)
        })

    pinned_assets = pinned_assets.split(",")
    summaries = [s for s in summaries if s.get("Asset") in pinned_assets or s.get("Position") != 0]

    summaries.sort(key=lambda s: s.get("Position (USD)"), reverse=True)
    summaries.sort(key=lambda s: s.get("Asset") in pinned_assets, reverse=True)

    summaries.append({
        "Portfolio": "Sum",
        "Position (USD)": sum(s.get("Position (USD)", 0) for s in summaries),
    })

    return summaries


@router.get("/settlement/current")
def settlement_current(
    alt_auth_token: str = Header(None),
    db: Session = Depends(deps.get_db),
    portfolio: str = "",
    portfolio_separate: str = "YES",
) -> Any:
    portfolios = portfolio.split(",") if len(portfolio) > 0 else []
    portfolio_separate = portfolio_separate == "YES"

    cal_manager = CalManager()

    system_feed_ctrl = SystemFeedCtrl(db)
    feeds_today = system_feed_ctrl.get_feeds_today(portfolios)
    cal_manager.process_feeds(feeds_today)

    settlements = cal_manager.get_settlement_by_portfolio(portfolios)
    if not portfolio_separate:
        settlements = cal_manager.merge_settlements(settlements)
    return settlements


@router.get("/account_balance/current")
def account_balance_current(
    alt_auth_token: str = Header(None),
    db: Session = Depends(deps.get_db),
    portfolio: str = "",
    portfolio_separate: str = "YES",
) -> Any:
    portfolios = portfolio.split(",") if len(portfolio) > 0 else []
    portfolio_separate = portfolio_separate == "YES"

    cal_manager = _permanent_cal_manager.clone(portfolios=portfolios)

    system_feed_ctrl = SystemFeedCtrl(db)
    feeds_today = system_feed_ctrl.get_feeds_today(portfolios)
    cal_manager.process_feeds(feeds_today)

    balance = cal_manager.get_account_balance_by_portfolio(portfolios)
    if not portfolio_separate:
        balance = cal_manager.merge_account_balance(balance)
    return balance


@router.get("/trades")
def trades(
    product: str,
    from_date: datetime,
    to_date: datetime,
    portfolios: str = "",
    alt_auth_token: str = Header(None),
    db: Session = Depends(deps.get_db),
) -> Any:
    portfolios = portfolios.split(",") if len(portfolios) > 0 else []

    system_feed_ctrl = SystemFeedCtrl(db)
    trades = system_feed_ctrl.get_trades_by_product_portfolio_time(portfolios, product, from_date, to_date)

    formatted_trades = []
    for trade in trades:
        formatted_trade = {
            "Trade Date": trade["trade_date"],
            "Base Asset": trade["base_asset"],
            "Quote Asset": trade["quote_asset"],
            "Quantity": trade["base_amount"],
            "Price": trade["quote_amount"] / trade["base_amount"] if trade["base_amount"] != 0 else 0,
            "Portfolio": trade["portfolio"],
            "Counterparty": trade["counterparty_name"],
            "Account": trade["account"] if trade["account"] is not None else "",
            "Completed": "YES" if trade["feed_type"] == "Cash" else "NO",
            "Fee": trade["fee_amount"],
            "Fee Asset": trade["fee_asset"],
            "Deal Ref": trade["deal_ref"],
        }

        if product != "Execution":
            del formatted_trade["Completed"]

        formatted_trades.append(formatted_trade)

    formatted_trades.sort(key=lambda t: t["Trade Date"], reverse=True)
    return formatted_trades


@router.get("/trades/list")
def trades_list(
    from_date: datetime,
    to_date: datetime,
    asset: str,
    portfolio: str = "",
    alt_auth_token: str = Header(None),
    db: Session = Depends(deps.get_db),
) -> Any:
    from_date = from_date.replace(tzinfo=None)
    to_date = to_date.replace(tzinfo=None)
    feeds_from_date = datetime.min

    system_feed_ctrl = SystemFeedCtrl(db)
    feeds = system_feed_ctrl.get_feeds_by_portfolio_asset_entity_time(portfolio, asset, feeds_from_date, to_date)

    snapshot = {}

    feeds_pnl = _permanent_cal_manager.enrich_feeds_with_pnl(snapshot, feeds)

    return [
        {
            "Trade Date": feed["trade_date"].strftime("%Y-%m-%d %H:%M:%S"),
            "Deal Ref": feed["deal_ref"],
            "Deal Type": feed["product"],
            "Counterparty Ref": feed["counterparty_ref"],
            "Counterparty": feed["counterparty_name"],
            "Entity": feed["entity"],
            "Direction":
                ("Sell" if feed["amount"] < 0 else "Buy")
                if feed["transfer_type"] == "trade" else
                ("Out" if feed["amount"] < 0 else "In"),
            "Asset": feed["asset"],
            "Asset Amount": feed["amount"],
            "USD Rate": feed["trade_price"],
            "Sum Amount (USD)": feed["amount_usdt"],
            "Quantity": feed["quantity"],
            "Last WA": feed["weighted_average_price"],
            "PnL (USD)": feed["pnl"],
            "Sum PnL (USD)": feed["sum_pnl"],
        }
        for feed in feeds_pnl
        if feed["trade_date"] > from_date and feed["trade_date"] <= to_date
    ]


@router.get("/transfers")
def transfers(
    from_date: datetime,
    to_date: datetime,
    portfolios: str = "",
    alt_auth_token: str = Header(None),
    db: Session = Depends(deps.get_db),
) -> Any:
    portfolios = portfolios.split(",") if len(portfolios) > 0 else []

    system_feed_ctrl = SystemFeedCtrl(db)
    transfers = system_feed_ctrl.get_feeds_transfer_by_portfolio_time(portfolios, from_date, to_date)

    formatted_transfers = []
    for transfer in transfers:
        formatted_transfer = {
            "Transfer Date": transfer["trade_date"],
            "Asset": transfer["asset"],
            "Quantity": transfer["amount"],
            "Portfolio": transfer["portfolio"],
            "Counterparty": transfer["counterparty_name"],
            "Account": transfer["account"] if transfer["account"] is not None else "",
            "Entity": transfer["entity"],
            "Deal Ref": transfer["deal_ref"],
        }

        formatted_transfers.append(formatted_transfer)

    formatted_transfers.sort(key=lambda t: t["Transfer Date"], reverse=True)
    return formatted_transfers


@router.get("/reset")
def reset(
    alt_auth_token: str = Header(None),
    db: Session = Depends(deps.get_db)
) -> Any:
    system_feed_ctrl = SystemFeedCtrl(db)
    _permanent_cal_manager.clear()

    trades_past = system_feed_ctrl.get_trade_view_past()
    _permanent_cal_manager.process_trades(trades_past)

    feeds_past = system_feed_ctrl.get_feeds_past()
    _permanent_cal_manager.process_feeds(feeds_past)

    return True
