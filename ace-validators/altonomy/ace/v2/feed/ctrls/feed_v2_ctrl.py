import json
from typing import List, Tuple
from datetime import datetime

from decimal import Decimal

from altonomy.ace.v2.feed.daos import FeedV2Dao
from altonomy.ace.models import FeedV2
from sqlalchemy.orm import Session
from redis import Redis
from logging import Logger

from altonomy.ace.v2.ticker.ctrls import TickerCtrl
from altonomy.ace import config


class FeedV2Ctrl(object):

    def __init__(self, db: Session, client_redis: Redis = None, trade_redis: Redis = None, logger: Logger = None):
        self.dao = FeedV2Dao(db, FeedV2)
        self.client_redis = client_redis
        self.logger = logger
        self.ticker_ctrl = TickerCtrl(db, trade_redis)

    def get_all_counterparties(self, portfolio: int, asset: str) -> List[Tuple[str, str]]:
        return self.dao.get_all_counterparties(portfolio, asset)

    def get_feeds_transfer_by_portfolio_time(
        self,
        portfolios: List[int],
        from_date: datetime,
        to_date: datetime,
    ) -> List[FeedV2]:
        return self.dao.get_feeds_transfer_by_portfolio_time(
            portfolios,
            from_date,
            to_date,
        )

    def get_feeds_position_by_asset_product_portfolio_time(
        self,
        asset: str,
        from_date: datetime,
        products: List[int],
        portfolios: List[int],
    ) -> List[Tuple[str, str, float]]:
        return self.dao.get_feeds_position_by_asset_product_portfolio_time(
            asset,
            from_date,
            products,
            portfolios,
        )

    def get_counterparty_exposure(
            self,
            portfolios: List[str],
            assets: List[str],
            counterparties: List[str],
    ) -> List[dict]:
        result = []
        option_feeds = self.dao.get_options_feed(portfolios, assets, counterparties)
        tickers = self.ticker_ctrl.get_tickers_cached('USDT')
        contract_delta_price_map = {}
        grand_total_row = {
            'counterparty_name': 'Grand Total',
            'asset': '',
            'instrument': '',
            'notional': 0,
            'market_value': 0,
            'market_value_usd': 0,
            'delta_exposure': 0,
            'delta_exposure_usd': 0,
        }
        for option_feed in option_feeds:
            counterparty_name, contract, amount = option_feed
            contract = contract[:-3]
            asset = contract.split('-')[0]
            exposure = {
                'counterparty_name': counterparty_name,
                'asset': asset,
                'instrument': contract,
                'notional': amount,
            }
            spot_price = tickers.get(asset.upper(), 0)
            if contract not in contract_delta_price_map:
                contract_delta_price_map[contract] = self.get_delta_and_price(contract)
            delta, last_price, last_price_usd = contract_delta_price_map[contract]
            exposure['market_value'] = exposure['notional'] * last_price
            exposure['market_value_usd'] = exposure['notional'] * last_price_usd
            exposure['delta_exposure'] = exposure['notional'] * delta
            exposure['delta_exposure_usd'] = exposure['delta_exposure'] * spot_price
            result.append(exposure)

            grand_total_row['notional'] += exposure['notional']
            grand_total_row['market_value'] += exposure['market_value']
            grand_total_row['market_value_usd'] += exposure['market_value_usd']
            grand_total_row['delta_exposure'] += exposure['delta_exposure']
            grand_total_row['delta_exposure_usd'] += exposure['delta_exposure_usd']
        result.append(grand_total_row)
        return result

    def get_deribit_exposure(
            self,
            portfolios: List[str],
            assets: List[str],
    ) -> List[dict]:
        exposures = self.get_counterparty_exposure(portfolios, assets, ['Deribit'])
        result = []
        notional = 0
        market_value = 0
        market_value_usd = 0
        delta_exposure = 0
        delta_exposure_usd = 0
        for idx in range(len(exposures)):
            if idx > 0 and exposures[idx]['asset'] != exposures[idx - 1]['asset']:
                result.append({
                    'counterparty_name': '',
                    'asset': f'{exposures[idx - 1]["asset"]} Total',
                    'instrument': '',
                    'notional': notional,
                    'market_value': market_value,
                    'market_value_usd': market_value_usd,
                    'delta_exposure': delta_exposure,
                    'delta_exposure_usd': delta_exposure_usd
                })
                notional = 0
                market_value = 0
                market_value_usd = 0
                delta_exposure = 0
                delta_exposure_usd = 0
            notional += exposures[idx]['notional']
            market_value += exposures[idx]['market_value']
            market_value_usd += exposures[idx]['market_value_usd']
            delta_exposure += exposures[idx]['delta_exposure']
            delta_exposure_usd += exposures[idx]['delta_exposure_usd']
            result.append(exposures[idx])
        result[-1]['asset'] = 'Grand Total'
        return result

    def get_open_options_pnl(
            self,
            portfolios: List[str],
            assets: List[str],
    ):
        open_option_notional_list = self.dao.get_options_feed(portfolios, assets, [])
        result_dicts = {
            f"{open_option_notional[0]}-{open_option_notional[1][:-3]}":
                {
                    "counter_party": open_option_notional[0],
                    "contract": open_option_notional[1][:-3],
                    "notional": open_option_notional[2],
                    "premium_paid_usd": Decimal(0),
                    "current_market_value_usd": self.get_delta_and_price(open_option_notional[1][:-3])[2] *
                    open_option_notional[2],
                    "total_pnl_usd": Decimal(0)
                }
            for open_option_notional in open_option_notional_list
        }
        open_option_paid_premium_list = self.dao.get_open_option_paid_premium_list(portfolios, assets)
        for open_option_paid_premium in open_option_paid_premium_list:
            key = f"{open_option_paid_premium.counterparty_name}-{open_option_paid_premium.contract}"
            if key in result_dicts:
                if open_option_paid_premium.asset in config.FIAT_STABLES_LIST:
                    paid_premium_usd = open_option_paid_premium.amount
                else:
                    trade_date = open_option_paid_premium.trade_date
                    # reuse same slots for lru cache for trade_date that falls within the same hour
                    # since ticker feed updates only every hour on the 11th minute
                    trade_date = trade_date.replace(minute=12, second=0, microsecond=0)
                    paid_premium_usd = self.ticker_ctrl.get_ticker_by_date_cached(open_option_paid_premium.asset,
                                                                                  "USDT", trade_date)
                result_dicts[key]["premium_paid_usd"] += paid_premium_usd

        result = []

        curr_counterparty = ""
        curr_notional = 0
        curr_premium_paid_usd = 0
        curr_current_market_value_usd = 0
        curr_total_pnl_usd = 0

        for result_dict in result_dicts.values():
            if result_dict["notional"] == 0:
                continue

            if curr_counterparty != "" and curr_counterparty != result_dict["counter_party"]:
                result.append({
                    "counter_party": f"{curr_counterparty} - Grand Total",
                    "contract": "Grand Total",
                    "notional": curr_notional,
                    "premium_paid_usd": curr_premium_paid_usd,
                    "current_market_value_usd": curr_current_market_value_usd,
                    "total_pnl_usd": curr_total_pnl_usd
                })
                curr_counterparty = result_dict["counter_party"]
                curr_notional = 0
                curr_premium_paid_usd = 0
                curr_current_market_value_usd = 0
                curr_total_pnl_usd = 0
            elif curr_counterparty == "":
                curr_counterparty = result_dict["counter_party"]

            result_dict["total_pnl_usd"] = result_dict["current_market_value_usd"] + result_dict["premium_paid_usd"]
            result.append(result_dict)
            curr_notional += result_dict["notional"]
            curr_premium_paid_usd += result_dict["premium_paid_usd"]
            curr_current_market_value_usd += result_dict["current_market_value_usd"]
            curr_total_pnl_usd += result_dict["total_pnl_usd"]
        else:
            result.append({
                "counter_party": f"{curr_counterparty} - Grand Total",
                "contract": "Grand Total",
                "notional": curr_notional,
                "premium_paid_usd": curr_premium_paid_usd,
                "current_market_value_usd": curr_current_market_value_usd,
                "total_pnl_usd": curr_total_pnl_usd
            })

        return result

    def get_delta_and_price(self, contract: str):
        try:
            data = self.client_redis.hmget(f"options:rfqpricer:rfqdata:{contract}",
                                           ['net_delta', 'net_native_bid', 'net_native_offer', 'net_dollar_bid',
                                            'net_dollar_offer'])
            net_delta = json.loads(data[0])
            net_native_bid = Decimal(json.loads(data[1]))
            net_native_offer = Decimal(json.loads(data[2]))
            net_dollar_bid = Decimal(json.loads(data[3]))
            net_dollar_offer = Decimal(json.loads(data[4]))
            native_price = (net_native_bid + net_native_offer) / 2
            native_price_dollar = (net_dollar_bid + net_dollar_offer) / 2
            return Decimal(net_delta[0]), native_price, native_price_dollar
        except Exception as e:
            self.logger.error(f"[get_delta_and_price] err[{e}]")
            return 0, 0, 0
