from datetime import datetime, timedelta
import json
from typing import Any, Optional
from decimal import Decimal
from fastapi.exceptions import HTTPException

from starlette.responses import Response

from altonomy.ace.common.utils import row_to_dict
from altonomy.ace.db import deps
from altonomy.ace.enums import Product
from altonomy.ace.v2.athena.snapshot.utils import (
    bulk_load_position_by_portfolio, bulk_load_settlement_by_portfolio, bulk_load_summary_v2_by_portfolio,
    bulk_load_summary_by_portfolio)
from altonomy.ace.v2.feed.ctrls import FeedV2Ctrl
from altonomy.ace.v2.feed.ctrls.manual_feed_v2_ctrl import ManualFeedV2Ctrl
from altonomy.ace.v2.trade.ctrls import TradeV2Ctrl
from fastapi import APIRouter, Depends, Header, UploadFile, File
from sqlalchemy.orm import Session
from altonomy.ace.enums import DealType
from altonomy.ace.common import api_utils
from altonomy.core import client
from altonomy.loggers.log_utils import get_simple_logger
from altonomy.ace.endpoints.schemas import Balance
from altonomy.ace.v2.ems.ems_ctrl import EMSCtrl
from altonomy.ace.external.optimus_client import OptimusClient
from typing import List
from altonomy.ace import config
from altonomy.ace.common.utils import round_2f, round_6f
import math
from redis import StrictRedis

oc = OptimusClient()
router = APIRouter()
c = client()
redis_client = c.redis
log = get_simple_logger()

trade_redis = None
try:
    trade_redis = StrictRedis(host=config.REDIS_HOST_T, port=config.REDIS_PORT, password=config.REDIS_PASS,
                              decode_responses=True)
    trade_redis.ping()
except BaseException:
    pass


@router.get("/snapshot/summary")
async def summary_current(
        alt_auth_token: str = Header(None),
        db: Session = Depends(deps.get_db),
        portfolio: str = "",
        portfolio_separate: str = "YES",
        trade_date: Optional[datetime] = None,
        effective_date: Optional[datetime] = None,
        pinned_assets: Optional[str] = "",
) -> Any:
    portfolios = portfolio.split(",") if len(portfolio) > 0 else []
    portfolio_separate = portfolio_separate == "YES"

    trade_date = datetime.utcnow() if trade_date is None else trade_date
    effective_date = datetime.utcnow() if effective_date is None else effective_date

    snapshots = []
    for snapshot in bulk_load_summary_by_portfolio(db, portfolios, trade_date, effective_date):
        snapshots.append(snapshot.get_value())

    if not portfolio_separate:
        new_snapshots = {}

        for snapshot in snapshots:
            key = snapshot["asset"]

            if key not in new_snapshots:
                new_snapshots[key] = {
                    "asset": snapshot["asset"],
                    "position": 0,
                    "last_price": snapshot.get("last_price", 0),
                    "change": snapshot.get("change", 0),
                }

            new_snapshot = new_snapshots[key]
            new_snapshot.update({
                "position": new_snapshot["position"] + snapshot["position"],
            })

        snapshots = list(new_snapshots.values())

    for snapshot in snapshots:
        snapshot.update({
            "position_usd": snapshot.get("position", 0) * snapshot.get("last_price", 0),
            "position_usd_abs": abs(snapshot.get("position", 0) * snapshot.get("last_price", 0)),
        })

    pinned_assets = pinned_assets.split(",")
    snapshots = [s for s in snapshots if s.get("asset") in pinned_assets or s.get("position") != 0]

    snapshots.sort(key=lambda s: s.get("position_usd", 0), reverse=True)
    snapshots.sort(key=lambda s: s.get("asset", "") in pinned_assets, reverse=True)

    snapshots.append({
        "asset": "all",
        "portfolio": "all",
        "position_usd": sum(s.get("position_usd", 0) for s in snapshots),
    })

    return snapshots


@router.get("/snapshot/summary-v2")
async def summary_v2_current(
        alt_auth_token: str = Header(None),
        db: Session = Depends(deps.get_db),
        portfolio: str = "",
        portfolio_separate: str = "YES",
        contract_separate: str = "YES",
        product_separate: str = "YES",
        trade_date: Optional[datetime] = None,
        effective_date: Optional[datetime] = None,
        pinned_assets: Optional[str] = "",
) -> Any:
    portfolios = portfolio.split(",") if len(portfolio) > 0 else []
    portfolio_separate = portfolio_separate == "YES"
    contract_separate = contract_separate == "YES"
    product_separate = product_separate == "YES"

    trade_date = datetime.utcnow() if trade_date is None else trade_date
    effective_date = datetime.utcnow() if effective_date is None else effective_date

    snapshots = []
    for snapshot in bulk_load_summary_v2_by_portfolio(db, portfolios, trade_date, effective_date):
        snapshots.append(snapshot.get_value())

    new_snapshots = {}
    for snapshot in snapshots:
        # Remove options from snapshots
        if snapshot['product'] == 'Options':
            continue
        keys = [snapshot['asset'] or ""]
        var_dict = {}
        if portfolio_separate:
            keys.append(snapshot['portfolio'] or "")
            var_dict.update({"portfolio": snapshot['portfolio']})
        if product_separate:
            keys.append(snapshot['product'] or "")
            var_dict.update({"product": snapshot['product']})
        if contract_separate:
            keys.append(snapshot['contract'] or "")
            var_dict.update({"contract": snapshot['contract']})

        key = '-'.join(keys)
        if key not in new_snapshots:
            new_snapshots[key] = {
                "asset": snapshot["asset"],
                "position": 0,
                "last_price": snapshot.get("last_price", 0),
                "change": snapshot.get("change", 0),
            }
            new_snapshots[key].update(var_dict)
        new_snapshot = new_snapshots[key]
        new_snapshot.update({
            "position": new_snapshot["position"] + snapshot["position"],
        })
    snapshots = list(new_snapshots.values())

    for snapshot in snapshots:
        snapshot.update({
            "position_usd": snapshot.get("position", 0) * snapshot.get("last_price", 0),
            "position_usd_abs": abs(snapshot.get("position", 0) * snapshot.get("last_price", 0)),
        })

    pinned_assets = pinned_assets.split(",")
    snapshots = [s for s in snapshots if s.get("asset") in pinned_assets or s.get("position") != 0]

    snapshots.sort(key=lambda s: s.get("position_usd", 0), reverse=True)
    snapshots.sort(key=lambda s: s.get("asset", "") in pinned_assets, reverse=True)

    snapshots.append({
        "asset": "all",
        "portfolio": "all",
        "contract": "all",
        "product": "all",
        "position_usd": sum(s.get("position_usd", 0) for s in snapshots),
    })

    return snapshots


def get_expiry_from_contract(contract: str) -> str:
    contract_part = contract.split("-")
    return datetime.strptime(contract_part[1], "%d%b%y") + timedelta(
        hours=8)  # add 8h to caliberate with general expiry time


def is_expire(contract: str) -> bool:
    if contract is not None:
        try:
            expiry_date = get_expiry_from_contract(contract)
            # date without time
            today = datetime.utcnow()
            if expiry_date < today:
                return True
        except (IndexError, ValueError):
            pass
    return False


def get_delta_and_price(contract: str, last_price: Decimal):
    if contract is not None and "(N)" in contract:
        if not is_expire(contract):
            try:
                contract = contract.replace("(N)", "")
                if config.USE_OPTION_PRICER_V2_DATA and "/" not in contract:
                    data = redis_client.hgetall(f"options:v2:rfqpricer:rfqdata:{contract}")
                    delta = Decimal(data.get("delta"))
                    last_price = Decimal(data.get("price_usd"))
                    data["raw"] = json.loads(data["raw"])
                    trading_param = data
                else:
                    data = redis_client.hgetall(f"options:rfqpricer:rfqdata:{contract}")
                    raw = json.loads(data["raw"])
                    trading_param = raw["for_client"] if "for_client" in raw else {}
                    delta, last_price = _get_delta_and_price_by_quote_asset(data, trading_param)
                return delta, last_price, trading_param
            except Exception as e:
                log.error(
                    f"[get_option_instrument_cached_rfq_data contract = {contract}]failed_to_get_option_instrument_rfq_data_from_cache|e={e}")
                return 0, None, {}
        else:
            return 0, None, {}
    else:
        return 1, last_price, {}


def _get_delta_and_price_by_quote_asset(data: dict, for_client_data: dict):
    quote_asset = for_client_data.get("quote", "USD")
    delta, last_price = None, None
    if quote_asset == "USD":
        net_delta = json.loads(data["net_delta"])
        net_dollor_bid = Decimal(json.loads(data["net_dollar_bid"]))
        net_dollor_offer = Decimal(json.loads(data["net_dollar_offer"]))
        delta = Decimal(net_delta[0])
        last_price = (net_dollor_bid + net_dollor_offer) / 2
    else:
        net_delta = for_client_data.get("delta_in_quote", 0)
        net_native_bid = Decimal(for_client_data.get("net_native_bid", 0))
        net_native_offer = Decimal(for_client_data.get("net_native_offer", 0))
        delta = Decimal(net_delta[0])
        last_price = (net_native_bid + net_native_offer) / 2
    return delta, last_price


def update_trading_param(snapshot: dict, param: dict, position: Decimal, contract: str):
    if not param:
        return

    if config.USE_OPTION_PRICER_V2_DATA and "/" not in contract:
        snapshot.update({
            "gamma": round_6f(Decimal(param["gamma"])) if "gamma" in param else "",
            "theta": round_2f(Decimal(param["theta"])) if "theta" in param else "",
            "vega": round_2f(Decimal(param["vega"])) if "vega" in param else "",
            "rho": round_2f(Decimal(param["rho"])) if "rho" in param else "",
            "net_gamma": round_6f(Decimal(param["gamma"]) * Decimal(snapshot.get("spot_price", 0)) * Decimal(
                math.copysign(100, position)) + Decimal(snapshot.get("net_gamma", 0))) if "gamma" in param else "",
            "net_theta": round_2f(Decimal(param["theta"]) * position + Decimal(
                snapshot.get("net_theta", 0))) if "theta" in param else "",
            "net_vega": round_2f(
                Decimal(Decimal(param["vega"])) * position + Decimal(snapshot.get("net_vega", 0))) if "vega" in param else "",
            "net_rho": round_2f(
                Decimal(Decimal(param["rho"])) * position + Decimal(snapshot.get("net_rho", 0))) if "rho" in param else "",
        })
    else:
        quote_asset = param.get("quote", "USD")
        if quote_asset == "USD":
            snapshot.update({
                "gamma": round_6f(param["gamma"][0]) if "gamma" in param else "",
                "theta": round_2f(param["theta"][0]) if "theta" in param else "",
                "vega": round_2f(param["vega"][0]) if "vega" in param else "",
                "rho": round_2f(param["rho"][0]) if "rho" in param else "",
            })
            # Calculate derived greeks
            # position = Decimal(snapshot.get("position", 0))
            snapshot.update({
                "net_gamma": round_6f(Decimal(param["gamma"][0]) * Decimal(snapshot.get("spot_price", 0)) * Decimal(
                    math.copysign(100, position)) + Decimal(snapshot.get("net_gamma", 0))) if "gamma" in param else "",
                "net_theta": round_2f(Decimal(param["theta"][0]) * position + Decimal(
                    snapshot.get("net_theta", 0))) if "theta" in param else "",
                "net_vega": round_2f(
                    Decimal(param["vega"][0]) * position + Decimal(snapshot.get("net_vega", 0))) if "vega" in param else "",
                "net_rho": round_2f(
                    Decimal(param["rho"][0]) * position + Decimal(snapshot.get("net_rho", 0))) if "rho" in param else "",
            })
        else:
            snapshot.update({
                "gamma": round_6f(param["gamma_in_quote"][0]) if "gamma_in_quote" in param else "",
                "theta": round_2f(param["theta_in_quote"][0]) if "theta_in_quote" in param else "",
                "vega": round_2f(param["vega_in_quote"][0]) if "vega_in_quote" in param else "",
                "rho": round_2f(param["rho_in_quote"][0]) if "rho_in_quote" in param else "",
            })
            # Calculate derived greeks
            # position = Decimal(snapshot.get("position", 0))
            snapshot.update({
                "net_gamma": round_6f(Decimal(param["gamma_in_quote"][0]) * Decimal(
                    math.copysign(100, position))) if "gamma_in_quote" in param else "",
                "net_theta": round_2f(Decimal(param["theta_in_quote"][0]) * position) if "theta_in_quote" in param else "",
                "net_vega": round_2f(Decimal(param["vega_in_quote"][0]) * position) if "vega_in_quote" in param else "",
                "net_rho": round_2f(Decimal(param["rho_in_quote"][0]) * position) if "rho_in_quote" in param else "",
            })


@router.get("/snapshot/option-summary-v2")
async def option_summary_v2_current(
        alt_auth_token: str = Header(None),
        db: Session = Depends(deps.get_db),
        portfolio: str = "",
        portfolio_separate: str = "YES",
        contract_separate: str = "YES",
        product_separate: str = "YES",
        spot_price_enable: str = "YES",
        trade_date: Optional[datetime] = None,
        effective_date: Optional[datetime] = None,
        pinned_assets: Optional[str] = "",
        products: Optional[str] = "",
        detailed_summary: bool = "YES"
) -> Any:
    portfolios = portfolio.split(",") if len(portfolio) > 0 else []
    portfolio_separate = portfolio_separate == "YES"
    contract_separate = contract_separate == "YES"
    product_separate = product_separate == "YES"
    spot_price_enable = spot_price_enable == "YES"
    detailed_summary = detailed_summary == "YES"

    products = products.split(",") if len(products) > 0 else []
    products = [product.strip() for product in products]

    trade_date = datetime.utcnow() if trade_date is None else trade_date
    effective_date = datetime.utcnow() if effective_date is None else effective_date

    snapshots = []
    for snapshot in bulk_load_summary_v2_by_portfolio(db, portfolios, trade_date, effective_date):
        # Change Options Settlements to FX Spot
        new_snapshot = snapshot.get_value()

        if new_snapshot["product"] == DealType.OPTIONS and "(N)" not in new_snapshot["contract"]:
            new_snapshot["product"] = DealType.FX_SPOT
            new_snapshot["contract"] = ""
        snapshots.append(new_snapshot)

    new_snapshots = {}
    for snapshot in snapshots:
        if len(products) != 0 and snapshot['product'] not in products:
            continue
        keys = [snapshot['asset'] or ""]
        var_dict = {}
        if portfolio_separate:
            keys.append(snapshot['portfolio'] or "")
            var_dict.update({"portfolio": snapshot['portfolio']})
        if product_separate:
            keys.append(snapshot['product'] or "")
            var_dict.update({"product": snapshot['product']})
        if contract_separate:
            contract = snapshot['contract']
            if snapshot['product'] == 'Options' and contract is not None:
                if "(N)" in contract:
                    keys.append(contract)
                    var_dict.update({"contract": contract.replace("(N)", "")})
                else:
                    keys.append("")
                    var_dict.update({"contract": ""})
            else:
                keys.append(contract or "")
                var_dict.update({"contract": contract if contract is not None else ""})

        key = '-'.join(keys)
        contract = snapshot['contract']
        # Option Notional contract
        if contract is not None and "(N)" in contract:
            # Remove expiry Notional form Athena
            if is_expire(contract):
                continue
            key = key + "(N)"

        if key not in new_snapshots:
            new_snapshots[key] = {
                "asset": snapshot["asset"],
                "position": 0,
                "last_price": snapshot.get("last_price", 0),
                "spot_price": snapshot.get("last_price", 0),
            }
            new_snapshots[key].update(var_dict)

        # Need delta calculation
        delta, last_price, trading_param = get_delta_and_price(snapshot["contract"], snapshot.get("last_price", 0))
        if last_price is not None:
            new_snapshots[key].update({"last_price": last_price})
        new_snapshot = new_snapshots[key]
        new_snapshots[key].update({
            "position": new_snapshot["position"] + snapshot["position"],
        })
        if detailed_summary:
            if contract_separate:
                update_trading_param(new_snapshot, trading_param, snapshot["position"], snapshot["contract"])
            new_snapshot.update({
                "delta": delta,
            })
            new_snapshot.update({
                "change": snapshot.get("change", 0),
                "position_usd": snapshot.get("position", 0) * new_snapshot.get("last_price", 0) + new_snapshot.get(
                    "position_usd", 0),
                "delta_net": Decimal(snapshot["position"]) * Decimal(new_snapshot.get("delta", 0)) + new_snapshot.get(
                    "delta_net", 0),
            })

    for key in new_snapshots:
        # Remove Delta and last_price if contract is not active
        if not contract_separate:
            if "delta" in new_snapshots[key]:
                del new_snapshots[key]["delta"]
            if "(N)" in key:
                del new_snapshots[key]["last_price"]

        # Remove delta information for stables/fiat
        if new_snapshots[key]['asset'] in config.FIAT_STABLES_LIST:
            if "delta" in new_snapshots[key]:
                del new_snapshots[key]["delta"]
            del new_snapshots[key]["delta_net"]
    snapshots = list(new_snapshots.values())

    for snapshot in snapshots:
        if detailed_summary and snapshot['asset'] not in config.FIAT_STABLES_LIST:
            snapshot.update({
                "delta_net_usd": snapshot.get("delta_net", 0) * snapshot.get("spot_price", 0),
            })
        if not spot_price_enable:
            del snapshot["spot_price"]

    pinned_assets = pinned_assets.split(",") if len(pinned_assets) > 0 else []
    pinned_assets = [pinned_asset.strip() for pinned_asset in pinned_assets]

    if len(pinned_assets) != 0:
        snapshots = [s for s in snapshots if s.get("asset") in pinned_assets]

    snapshots.sort(key=lambda s: s.get("asset", "") in pinned_assets, reverse=True)

    snapshots.append({
        "asset": "all",
        "portfolio": "all",
        "product": "all",
    })
    if detailed_summary:
        snapshots.append({
            "position_usd": sum(s.get("position_usd", 0) for s in snapshots),
            "delta_net_usd": sum(s.get("delta_net_usd", 0) for s in snapshots),
        })
    return snapshots


@router.get("/snapshot/option-summary-v3")
async def option_summary_v3_current(
        alt_auth_token: str = Header(None),
        db: Session = Depends(deps.get_db),
        portfolio: str = "",
        portfolio_separate: str = "NO",
        contract_separate: str = "YES",
        product_separate: str = "YES",
        expiry_separate: str = "YES",
        spot_price_enable: str = "NO",
        trade_date: Optional[datetime] = None,
        effective_date: Optional[datetime] = None,
        pinned_assets: Optional[str] = "",
        products: Optional[str] = "",
        detailed_summary: str = "YES",
        group_cash_flow: str = "YES"
) -> Any:
    portfolios = portfolio.split(",") if len(portfolio) > 0 else []
    portfolio_separate = portfolio_separate == "YES"
    contract_separate = contract_separate == "YES"
    product_separate = product_separate == "YES"
    spot_price_enable = spot_price_enable == "YES"
    detailed_summary = detailed_summary == "YES"
    expiry_separate = expiry_separate == "YES"
    group_cash_flow = group_cash_flow == "YES"
    products = products.split(",") if len(products) > 0 else []
    products = [product.strip() for product in products]
    trade_date = datetime.utcnow() if trade_date is None else trade_date
    effective_date = datetime.utcnow() if effective_date is None else effective_date
    snapshots = []
    print(datetime.now())
    for snapshot in bulk_load_summary_v2_by_portfolio(db, portfolios, trade_date, effective_date):
        # Change Options Settlements to FX Spot
        new_snapshot = snapshot.get_value()

        if new_snapshot["product"] == DealType.OPTIONS and "(N)" not in new_snapshot["contract"]:
            new_snapshot["product"] = DealType.FX_SPOT
            new_snapshot["contract"] = ""
        if group_cash_flow and new_snapshot["product"] == DealType.CASHFLOW:
            new_snapshot["product"] = DealType.FX_SPOT
        if new_snapshot["asset"] in config.FIAT_STABLES_LIST:
            new_snapshot["product"] = DealType.FX_SPOT
            new_snapshot["contract"] = ""
        # Remove Long and Short from contract of Futures
        if new_snapshot["product"] == DealType.FUTURES and new_snapshot["contract"] is not None:
            new_snapshot["contract"] = new_snapshot["contract"].replace("Long", "").replace("Short", "").strip()
            if is_expire(new_snapshot["contract"]):
                new_snapshot["contract"] = ""
                new_snapshot["product"] = DealType.FX_SPOT
        snapshots.append(new_snapshot)
    print(datetime.now())
    new_snapshots = {}
    for snapshot in snapshots:
        if len(products) != 0 and snapshot['product'] not in products:
            continue
        keys = [snapshot['asset'] or ""]
        var_dict = {}
        if portfolio_separate:
            keys.append(snapshot['portfolio'] or "")
            var_dict.update({"portfolio": snapshot['portfolio']})
        if product_separate:
            keys.append(snapshot['product'] or "")
            var_dict.update({"product": snapshot['product']})
        if expiry_separate:
            var_dict.update({"expiry": ""})
            if snapshot['product'] == 'Options':
                contract = snapshot['contract']
                expiry = get_expiry_from_contract(contract).strftime('%m/%d/%Y')
                if "(N)" in contract and expiry is not None:  # Open Option
                    keys.append(expiry)
                    var_dict.update({"expiry": expiry})
        if contract_separate:
            contract = snapshot['contract']
            if snapshot['product'] == 'Options' and contract is not None:
                if "(N)" in contract:
                    keys.append(contract)
                    var_dict.update({"contract": contract.replace("(N)", "")})
                else:
                    keys.append("")
                    var_dict.update({"contract": ""})
            else:
                keys.append(contract or "")
                var_dict.update({"contract": contract if contract is not None else ""})

        key = '-'.join(keys)
        contract = snapshot['contract']

        # Option Notional contract
        if contract is not None and "(N)" in contract:
            # Remove expiry Notional form Athena
            if is_expire(contract):
                continue
            if contract_separate or expiry_separate:
                key = key + "(N)"

        if key not in new_snapshots:
            new_snapshots[key] = {
                "asset": snapshot["asset"],
                "position": 0,
                "last_price": snapshot.get("last_price", 0),
                "spot_price": snapshot.get("last_price", 0),
            }
            new_snapshots[key].update(var_dict)

        # Need delta calculation
        delta, last_price, trading_param = get_delta_and_price(snapshot["contract"], snapshot.get("last_price", 0))
        if last_price is not None:
            new_snapshots[key].update({"last_price": last_price})
        new_snapshot = new_snapshots[key]
        new_snapshots[key].update({
            "position": new_snapshot["position"] + snapshot["position"],
        })

        if detailed_summary:
            # if contract_separate:
            update_trading_param(new_snapshot, trading_param, snapshot["position"], snapshot["contract"])
            new_snapshot.update({
                "delta": delta,
            })
            new_snapshot.update({
                "change": snapshot.get("change", 0),
                "position_usd": snapshot.get("position", 0) * new_snapshot.get("last_price", 0) + new_snapshot.get(
                    "position_usd", 0),
                "net_delta": Decimal(snapshot["position"]) * Decimal(
                    new_snapshot.get("delta", 0)) + new_snapshot.get("net_delta", 0),
            })

    for key in new_snapshots:
        # Remove Delta and last_price if contract is not active
        if not contract_separate:
            if "delta" in new_snapshots[key]:
                del new_snapshots[key]["delta"]
            if "(N)" in key:
                del new_snapshots[key]["last_price"]

        # Remove delta information for stables/fiat
        if new_snapshots[key]['asset'] in config.FIAT_STABLES_LIST:
            if "delta" in new_snapshots[key]:
                del new_snapshots[key]["delta"]
            del new_snapshots[key]["net_delta"]
    snapshots = list(new_snapshots.values())
    for snapshot in snapshots:
        if detailed_summary and snapshot['asset'] not in config.FIAT_STABLES_LIST:
            snapshot.update({
                "net_delta_usd": snapshot.get("net_delta", 0) * snapshot.get("spot_price", 0),
            })
        # CONVERT TO 8 DECIMAL PLACES
        last_price = snapshot.get("last_price", 0)
        last_price = f"{round(last_price):,.2f}" if last_price >= 1000 else f"{round(last_price, 8):.8f}"
        snapshot.update({
            "last_price": last_price.rstrip('0').rstrip('.') if '.' in last_price else last_price,
        })
    for snapshot in snapshots:
        if not spot_price_enable:
            del snapshot["spot_price"]
        if not contract_separate:
            if "gamma" in snapshot:
                del snapshot["gamma"]
            if "vega" in snapshot:
                del snapshot["vega"]
            if "theta" in snapshot:
                del snapshot["theta"]
            if "rho" in snapshot:
                del snapshot["rho"]
    pinned_assets = pinned_assets.split(",") if len(pinned_assets) > 0 else []
    pinned_assets = [pinned_asset.strip() for pinned_asset in pinned_assets]
    if len(pinned_assets) != 0:
        snapshots = [s for s in snapshots if s.get("asset") in pinned_assets]

    snapshots.sort(key=lambda s: s.get("asset", "") in pinned_assets, reverse=True)

    if detailed_summary:
        populate_summary(snapshots)
    return snapshots


def populate_summary(snapshots):
    """
    populate_summary will add a last row that sums up all the previous rows for fields that requires summation
    """
    position_usd_sum = 0
    net_delta_usd_sum = 0
    net_gamma_sum = 0
    net_vega_sum = 0
    net_theta_sum = 0
    net_rho_sum = 0
    for s in snapshots:
        position_usd_sum += s.get("position_usd", 0)
        net_delta_usd_sum += s.get("net_delta_usd", 0)

        net_gamma = s.get("net_gamma", 0)
        if net_gamma is not None and net_gamma != "":
            net_gamma_sum += Decimal(net_gamma)
        net_vega = s.get("net_vega", 0)
        if net_vega is not None and net_vega != "":
            net_vega_sum += Decimal(net_vega)
        net_theta = s.get("net_theta", 0)
        if net_theta is not None and net_theta != "":
            net_theta_sum += Decimal(net_theta)
        net_rho = s.get("net_rho", 0)
        if net_rho is not None and net_rho != "":
            net_rho_sum += Decimal(net_rho)

    snapshots.append({
        "asset": "all",
        "portfolio": "all",
        "product": "all",
        "position_usd": position_usd_sum,
        "net_delta_usd": net_delta_usd_sum,
        "net_gamma": net_gamma_sum,
        "net_vega": net_vega_sum,
        "net_theta": net_theta_sum,
        "net_rho": net_rho_sum,
    })

    for s in snapshots:
        s.update({
            "position_usd": "{:,}".format(round(float(s.get("position_usd", 0)))),
            "net_delta_usd": "{:,}".format(round(float(s.get("net_delta_usd", 0)))) + " $",
            "net_gamma": "{:,}".format(round(float(s.get("net_gamma", 0)))) + " $",
            "net_vega": "{:,}".format(round(float(s.get("net_vega", 0)))) + " $",
            "net_theta": "{:,}".format(round(float(s.get("net_theta", 0)))) + " $",
            "net_rho": "{:,}".format(round(float(s.get("net_rho", 0)))) + " $",
        })


@router.get("/snapshot/position_v2")
async def position_v2(
        alt_auth_token: str = Header(None),
        db: Session = Depends(deps.get_db),
        portfolio: str = "",
        trade_date: Optional[datetime] = None,
        effective_date: Optional[datetime] = None,
        pinned_assets: Optional[str] = "",
) -> Any:
    portfolios = portfolio.split(",") if len(portfolio) > 0 else []

    trade_date = datetime.utcnow() if trade_date is None else trade_date
    effective_date = datetime.utcnow() if effective_date is None else effective_date

    snapshots = []
    for snapshot in bulk_load_summary_v2_by_portfolio(db, portfolios, trade_date, effective_date):
        # Change Options Settlements to FX Spot
        new_snapshot = snapshot.get_value()

        if new_snapshot["product"] == DealType.OPTIONS and "(N)" not in new_snapshot["contract"]:
            new_snapshot["product"] = DealType.FX_SPOT
            new_snapshot["contract"] = ""
        snapshots.append(new_snapshot)

    new_snapshots = {}
    for snapshot in snapshots:
        keys = [snapshot['product'] or ""]
        keys.append(snapshot['asset'] or "")
        key = '-'.join(keys)
        contract = snapshot['contract']

        # Options
        if contract is not None and "(N)" in contract:
            # Remove expiry Notional form Athena
            if is_expire(contract):
                continue
            key = key + "(N)"

        if key not in new_snapshots:
            new_snapshots[key] = {
                "asset": snapshot["asset"],
                "product": snapshot['product'],
                "position": 0,
            }
        new_snapshot = new_snapshots[key]
        new_snapshot.update({
            "position": new_snapshot["position"] + snapshot["position"],
        })

    snapshots = list(new_snapshots.values())
    new_snapshots = {}
    for snapshot in snapshots:
        if snapshot["asset"] not in new_snapshots:
            new_snapshots[snapshot["asset"]] = {
                "asset": snapshot["asset"],
                snapshot["product"]: snapshot["position"]
            }
        else:
            new_snapshots[snapshot["asset"]].update({
                snapshot["product"]: snapshot["position"]
            })
    snapshots = list(new_snapshots.values())
    pinned_assets = pinned_assets.split(",") if len(pinned_assets) > 0 else []
    pinned_assets = [pinned_asset.strip() for pinned_asset in pinned_assets]

    if len(pinned_assets) != 0:
        snapshots = [s for s in snapshots if s.get("asset") in pinned_assets]

    snapshots.sort(key=lambda s: s.get("asset", "") in pinned_assets, reverse=True)

    snapshots.append({
        "asset": "all",
        "product": "all",
    })

    return snapshots


@router.get("/snapshot/settlement")
async def settlement_current(
        alt_auth_token: str = Header(None),
        db: Session = Depends(deps.get_db),
        portfolio: str = "",
        portfolio_separate: str = "YES",
        trade_date: Optional[datetime] = None,
        effective_date: Optional[datetime] = None,
) -> Any:
    portfolios = portfolio.split(",") if len(portfolio) > 0 else []
    portfolio_separate = portfolio_separate == "YES"

    trade_date = datetime.utcnow() if trade_date is None else trade_date
    effective_date = datetime.utcnow() if effective_date is None else effective_date

    snapshots = []
    for snapshot in bulk_load_settlement_by_portfolio(db, portfolios, trade_date, effective_date):
        value = snapshot.get_value()
        if value.get("position", 0) != 0:
            snapshots.append(value)

    cols = ["portfolio", "counterparty_ref", "counterparty_name", "net_exposure"]

    new_snapshots = {}
    for snapshot in snapshots:
        portfolio = snapshot.get("portfolio")
        counterparty_ref = snapshot.get("counterparty_ref")
        counterparty_name = snapshot.get("counterparty_name")
        asset = snapshot.get("asset", "")
        position = snapshot.get("position", 0)

        if asset not in cols:
            cols.append(asset)

        key = (portfolio, counterparty_ref, counterparty_name)
        if key not in new_snapshots:
            new_snapshots[key] = {
                "portfolio": portfolio,
                "counterparty_ref": counterparty_ref,
                "counterparty_name": counterparty_name,
                "net_exposure": 0,
            }

        new_snapshot = new_snapshots[key]
        new_snapshot.update({
            asset: position,
            "net_exposure": new_snapshot.get("net_exposure", 0) + snapshot.get("net_exposure", 0)
        })

    snapshots = [
        {col: s.get(col, 0) for col in cols}
        for s in new_snapshots.values()
    ]

    if not portfolio_separate:
        new_snapshots = {}
        str_cols = ["portfolio", "counterparty_ref", "counterparty_name"]

        for snapshot in snapshots:
            key = snapshot["counterparty_ref"]

            if key not in new_snapshots:
                new_snapshots[key] = {
                    "counterparty_ref": snapshot["counterparty_ref"],
                    "counterparty_name": snapshot["counterparty_name"],
                }

            new_settlement = new_snapshots[key]
            new_settlement.update({
                c: new_settlement.get(c, 0) + snapshot.get(c, 0)
                for c in snapshot
                if c not in str_cols
            })

        snapshots = list(new_snapshots.values())

    return snapshots


@router.get("/snapshot/position")
async def position_current(
        alt_auth_token: str = Header(None),
        db: Session = Depends(deps.get_db),
        portfolio: str = "",
        portfolio_separate: str = "YES",
        trade_date: Optional[datetime] = None,
        effective_date: Optional[datetime] = None,
) -> Any:
    portfolios = portfolio.split(",") if len(portfolio) > 0 else []
    portfolio_separate = portfolio_separate == "YES"

    trade_date = datetime.utcnow() if trade_date is None else trade_date
    effective_date = datetime.utcnow() if effective_date is None else effective_date

    snapshots = []
    for snapshot in bulk_load_position_by_portfolio(db, portfolios, trade_date, effective_date):
        snapshots.append(snapshot.get_value())

    for snapshot in snapshots:
        base = snapshot.get("base_asset")
        quote = snapshot.get("quote_asset")
        snapshot.update({
            "base_asset": base if base is not None else "",
            "quote_asset": quote if quote is not None else "",
        })

    if not portfolio_separate:
        new_snapshots = {}

        for snapshot in snapshots:
            key = (snapshot["base_asset"], snapshot["quote_asset"])

            if key not in new_snapshots:
                new_snapshots[key] = {
                    "base_asset": snapshot["base_asset"],
                    "quote_asset": snapshot["quote_asset"],
                    "net_position": 0,
                    "realized_pnl": 0,
                    "realized_pnl_usd": 0,
                    "market_price": snapshot["market_price"],
                    "unrealized_pnl": 0,
                    "unrealized_pnl_usd": 0,
                    "total_buy_quantity": 0,
                    "total_sell_quantity": 0,
                    "total_pnl": 0,
                    "total_pnl_usd": 0,
                }

            new_snapshot = new_snapshots[key]
            new_snapshots[key].update({
                "net_position": new_snapshot["net_position"] + snapshot["net_position"],
                "realized_pnl": new_snapshot["realized_pnl"] + snapshot["realized_pnl"],
                "realized_pnl_usd": new_snapshot["realized_pnl_usd"] + snapshot["realized_pnl_usd"],
                "unrealized_pnl": new_snapshot["unrealized_pnl"] + snapshot["unrealized_pnl"],
                "unrealized_pnl_usd": new_snapshot["unrealized_pnl_usd"] + snapshot["unrealized_pnl_usd"],
                "total_buy_quantity": new_snapshot["total_buy_quantity"] + snapshot["total_buy_quantity"],
                "total_sell_quantity": new_snapshot["total_sell_quantity"] + snapshot["total_sell_quantity"],
                "total_pnl": new_snapshot["total_pnl"] + snapshot["total_pnl"],
                "total_pnl_usd": new_snapshot["total_pnl_usd"] + snapshot["total_pnl_usd"],
            })

        snapshots = list(new_snapshots.values())

    snapshots = [
        s
        for s in snapshots
        if s.get("total_buy_quantity", 0) != 0 or
        s.get("total_sell_quantity", 0) != 0
    ]

    return snapshots


@router.get("/trades")
async def trades(
        product: str,
        from_date: datetime,
        to_date: datetime,
        portfolios: str = "",
        alt_auth_token: str = Header(None),
        db: Session = Depends(deps.get_db),
) -> Any:
    portfolios = portfolios.split(",") if len(portfolios) > 0 else []

    trade_ctrl = TradeV2Ctrl(db)
    trades = trade_ctrl.get_trades_by_product_portfolio_time(portfolios, product, from_date, to_date)

    formatted_trades = []
    for trade in trades:
        trade = row_to_dict(trade)
        trade.update({
            "price": trade["quote_amount"] / trade["base_amount"] if trade["base_amount"] != 0 else 0,
            "account": trade["account"] if trade["account"] is not None else "",
        })

        if product == Product.EXECUTION:
            trade.update({
                "completed": "YES" if trade["feed_type"] == "Cash" else "NO",
            })

        formatted_trades.append(trade)

    formatted_trades.sort(key=lambda t: t["trade_date"], reverse=True)
    return formatted_trades


@router.get("/transfers")
async def transfers(
        from_date: datetime,
        to_date: datetime,
        portfolios: str = "",
        alt_auth_token: str = Header(None),
        db: Session = Depends(deps.get_db),
) -> Any:
    portfolios = portfolios.split(",") if len(portfolios) > 0 else []

    feed_ctrl = FeedV2Ctrl(db)
    transfers = feed_ctrl.get_feeds_transfer_by_portfolio_time(portfolios, from_date, to_date)

    formatted_transfers = []
    for transfer in transfers:
        formatted_transfer = row_to_dict(transfer)
        formatted_transfer.update({
            "account": formatted_transfer["account"] if formatted_transfer["account"] is not None else "",
        })
        formatted_transfers.append(formatted_transfer)

    formatted_transfers.sort(key=lambda t: t["trade_date"], reverse=True)
    return formatted_transfers


@router.get("/position")
async def position(
        from_date: datetime,
        asset: str,
        products: str = "FX Spot,Futures",
        portfolios: str = "",
        db: Session = Depends(deps.get_db),
) -> Any:
    products = [item.strip() for item in products.split(',')]
    portfolios = [item.strip() for item in portfolios.split(',')]

    feed_ctrl = FeedV2Ctrl(db)
    positions = feed_ctrl.get_feeds_position_by_asset_product_portfolio_time(
        asset, from_date, products, portfolios)

    formatted_positions = []
    for position in positions:
        formatted_positions.append({
            "asset": position[0],
            "product": position[1],
            "amount":
                position[2]
                if position[2] is not None else 0.0,
        })
    return formatted_positions


@router.post("/manual-feeds")
async def create_upload_file(
        response: Response,
        alt_auth_token: str = Header(None),
        db: Session = Depends(deps.get_db),
        f: UploadFile = File(default=None)
):
    err, result = api_utils.get_jwt_payload(alt_auth_token, ["ace_feed_create"])
    if err is not None:
        raise HTTPException(status_code=api_utils.get_status_code(err), detail=err)
    contents = await f.read()
    contents = contents.decode('utf-8')
    manual_feed_ctrl = ManualFeedV2Ctrl(db)
    errors, added, is_success = manual_feed_ctrl.add_from_csv(contents)
    if is_success:
        return {"msg": f"Added {str(added)}"}
    else:
        response.status_code = 422
        return {"msg": "Failed", "errors": errors}


@router.get("/balances", response_model=List[Balance])
async def get_balances(
        account_ids: str,
        group_by: str = None,
        alt_auth_token: str = Header(None),
) -> Any:
    ec = EMSCtrl(optimus_client=oc)
    account_ids = account_ids.split(",")
    group_by = group_by.split(",") if group_by is not None else []
    return ec.get_formated_account_balances(account_ids, group_by)


@router.get("/counterparty-exposure")
async def counterparty_exposure(
        db: Session = Depends(deps.get_db),
        deribit_exposure: str = "NO",
        pinned_assets: Optional[str] = "",
        portfolio: str = ""
) -> Any:
    deribit_exposure = deribit_exposure == "YES"
    pinned_assets = pinned_assets.split(",") if len(pinned_assets) > 0 else []
    pinned_assets = [pinned_asset.strip() for pinned_asset in pinned_assets]
    portfolios = portfolio.split(",") if len(portfolio) > 0 else []

    feed_v2_ctrl = FeedV2Ctrl(db, client_redis=redis_client, trade_redis=trade_redis, logger=log)
    if not deribit_exposure:
        return feed_v2_ctrl.get_counterparty_exposure(portfolios, pinned_assets, [])
    return feed_v2_ctrl.get_deribit_exposure(portfolios, pinned_assets)


@router.get("/options-pnl")
async def options_pnl(
        db: Session = Depends(deps.get_db),
        pinned_assets: Optional[str] = "",
        portfolio: str = ""
) -> Any:
    pinned_assets = pinned_assets.split(",") if len(pinned_assets) > 0 else []
    pinned_assets = [pinned_asset.strip() for pinned_asset in pinned_assets]
    portfolios = portfolio.split(",") if len(portfolio) > 0 else []

    feed_v2_ctrl = FeedV2Ctrl(db, client_redis=redis_client, trade_redis=trade_redis, logger=log)
    return feed_v2_ctrl.get_open_options_pnl(portfolios, pinned_assets)


@router.get("/snapshot/delta-summary")
async def delta_summary(
        db: Session = Depends(deps.get_db),
        portfolio: str = "",
        trade_date: Optional[datetime] = None,
        effective_date: Optional[datetime] = None,
        group_cash_flow: str = "YES",
        pinned_assets: Optional[str] = ""
) -> Any:
    portfolios = portfolio.split(",") if len(portfolio) > 0 else []
    trade_date = datetime.utcnow() if trade_date is None else trade_date
    effective_date = datetime.utcnow() if effective_date is None else effective_date
    group_cash_flow = group_cash_flow == "YES"
    pinned_assets = pinned_assets.split(",") if len(pinned_assets) > 0 else []
    pinned_assets = set([pinned_asset.strip() for pinned_asset in pinned_assets])

    asset_map = {}
    for snapshot in bulk_load_summary_v2_by_portfolio(db, portfolios, trade_date, effective_date):
        snapshot_dict = snapshot.get_value()

        if snapshot_dict['product'] == DealType.EXECUTION or snapshot_dict['asset'] == '':
            continue
        if len(pinned_assets) and snapshot_dict['asset'] not in pinned_assets:
            continue
        if snapshot_dict["asset"] in config.FIAT_STABLES_LIST:
            continue

        # Change Options Settlements to FX Spot
        if snapshot_dict["product"] == DealType.OPTIONS and snapshot_dict["contract"][-3:] != "(N)":
            snapshot_dict["product"] = DealType.FX_SPOT
            snapshot_dict["contract"] = ""

        # # Remove Long and Short from contract of Futures
        if snapshot_dict["product"] == DealType.FUTURES and snapshot_dict["contract"] is not None:
            snapshot_dict["contract"] = snapshot_dict["contract"].replace("Long", "").replace("Short", "").strip()
            if is_expire(snapshot_dict["contract"]):
                snapshot_dict["contract"] = ""
                snapshot_dict["product"] = DealType.FX_SPOT

        # Treat cash flow as FX Spot if group_cash_flow
        if snapshot_dict["product"] == DealType.CASHFLOW and group_cash_flow:
            snapshot_dict["product"] = DealType.FX_SPOT

        product = snapshot_dict["product"]
        asset = snapshot_dict["asset"]

        if asset not in asset_map:
            asset_map[asset] = {}
        if product not in asset_map[asset]:
            asset_map[asset][product] = 0

        if product == DealType.OPTIONS:
            delta, _, _ = get_delta_and_price(snapshot_dict['contract'], snapshot_dict['last_price'])
            asset_map[asset][product] += snapshot_dict['position'] * delta * snapshot_dict['last_price']
        else:
            asset_map[asset][product] += snapshot_dict['position'] * snapshot_dict['last_price']
    result = []
    grand_total_row = {
        'asset': 'Grand Total',
        'Futures': 0,
        'Options': 0,
        'FX Spot': 0,
        'Cash Flow': 0,
        'Grand Total': 0
    }
    for key in sorted(asset_map.keys()):
        row = {
            'asset': key,
            'Futures': 0,
            'Options': 0,
            'FX Spot': 0,
        }
        if not group_cash_flow:
            row['Cash Flow'] = 0
        row['Grand Total'] = 0

        has_position = False
        for product, delta_exposure_usd in asset_map[key].items():
            if float(delta_exposure_usd) == 0:
                continue
            has_position = True
            row[product] = delta_exposure_usd
            row['Grand Total'] += delta_exposure_usd
            grand_total_row[product] += delta_exposure_usd
            grand_total_row['Grand Total'] += delta_exposure_usd
        if has_position:
            result.append(row)
    result.append(grand_total_row)
    return result
