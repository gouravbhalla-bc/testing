import time
import json

from operator import itemgetter
from altonomy.ace import config
from redis import Redis, StrictRedis
from altonomy.ace.external.optimus_client import OptimusClient
from typing import List
from logging import Logger, getLogger


class EMSCtrl(object):

    def __init__(self, for_sync: bool = False, optimus_client: OptimusClient = None):
        self.redis: Redis = None
        self.oms_redis: Redis = None
        self.ems_redis: Redis = None
        self.optimus_client: OptimusClient = optimus_client
        self.logger: Logger = None
        self.ticker_exchanges = ["Binance", "Coinbase", "Kraken", "Huobi", "MXC", "Gateiov4", "Bitmax"]
        self.stable_coins = ("USD", "USDT", "USDC", "BUSD", "DAI")
        if for_sync:
            self.start_sync_dependencies()
        else:
            self.start_redis()

    def start_sync_dependencies(self):
        oms_host = config.REDIS_HOST
        ems_host = config.REDIS_HOST_T
        port = config.REDIS_PORT
        password = config.REDIS_PASS
        self.oms_redis = self.__get_redis_client(oms_host, port, password, True)
        self.ems_redis = self.__get_redis_client(ems_host, port, password, True)
        self.optimus_client = OptimusClient()
        self.logger = getLogger()

    def start_redis(self, redis_host=None, redis_port=None, redis_password=None, decode_responses=True):
        host = redis_host if redis_host else config.REDIS_HOST_T
        port = redis_port if redis_port else config.REDIS_PORT
        password = redis_password if redis_password else config.REDIS_PASS
        self.redis = self.__get_redis_client(host, port, password, decode_responses)

    def __get_redis_client(self, host, port, password, decode_responses):
        redis = StrictRedis(host=host, port=port, password=password, decode_responses=decode_responses)
        try:
            redis.ping()
            return redis
        except BaseException:
            return None

    def __get_portfolio_map(self):
        portfolio_map = {}
        _, portfolios = self.optimus_client.get_portfolios()
        for portfolio in portfolios:
            portfolio_map.update({
                portfolio["portfolio_number"]: portfolio
            })
        return portfolio_map

    def __get_acc_portfolio_map(self):
        account_map = {}
        portfolio_map = self.__get_portfolio_map()
        _, accounts = self.optimus_client.get_accounts()
        for account in accounts:
            portfolio_number = account.get("portfolio_number", None)
            portfolio = portfolio_map.get(portfolio_number, {})
            portfolio_name = portfolio.get("portfolio_name", None)
            business_line = portfolio.get("business_line", None)
            activity = portfolio.get("activity", None)
            sub_activity = portfolio.get("sub_activity", None)
            strategy = portfolio.get("strategy", None)
            function = portfolio.get("function", None)
            exchange = account.get("exchange", None)
            if portfolio_number is not None:
                account_map.update({
                    account["nitro_account_id"]: {
                        "portfolio_number": portfolio_number,
                        "portfolio_name": portfolio_name,
                        "business_line": business_line,
                        "activity": activity,
                        "sub_activity": sub_activity,
                        "strategy": strategy,
                        "function": function,
                        "exchange": exchange,
                    }
                })
        return account_map

    def __get_config_key(self, account_id: str):
        return f"config:Broker:{account_id}"

    def __get_balance_key(self, account_id: str):
        return f"balance:Broker:{account_id}"

    def __get_ticker_key(self, exchange_name: str):
        return f"ticker:{exchange_name}"

    def get_tickers(self, exchanges: List[str]):
        pipeline = self.redis.pipeline()
        for exchange in exchanges:
            pipeline.hgetall(self.__get_ticker_key(exchange))
        results = pipeline.execute()
        ticker_map = {}
        for result in results:
            if isinstance(result, dict):
                ts = float(result.get("timestamp", 0))
                raw = result.get("raw", "{}")
                try:
                    raw = json.loads(raw)
                    if time.time() - ts <= 300:
                        for pair in raw:
                            price = raw[pair].get("price", 0)
                            if pair not in ticker_map and price > 0:
                                ticker_map.update({
                                    pair: price,
                                })
                except BaseException:
                    pass
        return ticker_map

    def get_usd_price(self, asset: str, ticker_map: dict, exchange: str):
        # convert for futures instrument
        _asset = asset.strip()
        if " " in _asset:
            _asset = _asset.split("USD")[0]
            # Note: requested by MO, don't show contract value until MO figured out how we value them.
            return 0
        # check existing stables
        for coin in self.stable_coins:
            if exchange.lower() == 'binance' and coin == 'USD':
                pair = f"{_asset}USDT"
            else:
                pair = f"{_asset}{coin}"
            if pair in ticker_map:
                return ticker_map[pair]
        if _asset in self.stable_coins:
            return 1
        return 0

    def get_account_balances(self, account_ids: List[str]):
        ticker_map = self.get_tickers(self.ticker_exchanges)
        pipeline = self.redis.pipeline()
        for acc_id in account_ids:
            pipeline.hgetall(self.__get_config_key(acc_id))
        results = pipeline.execute()

        acc_config_map = {}
        for idx, acc_id in enumerate(account_ids):
            acc_config_map.update({
                acc_id: results[idx]
            })

        for acc_id in account_ids:
            pipeline.hgetall(self.__get_balance_key(acc_id))
        results = pipeline.execute()
        acc_portfolio_map = self.__get_acc_portfolio_map()
        resp = []
        for idx, acc_id in enumerate(account_ids):
            acc_config = acc_config_map.get(acc_id, {})
            account_name = acc_config.get("name", "")
            exchange_name = account_name.split("@")[-1]
            result = results[idx]
            if isinstance(result, dict):
                ts = float(result.get("timestamp", 0))
                raw = result.get("raw", "{}")
                try:
                    raw = json.loads(raw)
                    # balances = []
                    if time.time() - ts <= 300:
                        for k in raw:
                            usd_price = self.get_usd_price(k, ticker_map, exchange_name)
                            available = raw.get(k, {}).get("available", 0)
                            frozen = raw.get(k, {}).get("frozen", 0)
                            margin_balance = raw.get(k, {}).get("equity", 0)
                            total = available + frozen + margin_balance if margin_balance == 0 else margin_balance
                            if total > 0:
                                acc_portfolio = acc_portfolio_map.get(str(acc_id), {})
                                resp.append({
                                    "account_id": acc_id,
                                    "account_name": account_name,
                                    "exchange_name": acc_portfolio.get("exchange", exchange_name),
                                    "asset": k,
                                    "available": available,
                                    "frozen": frozen,
                                    "margin_balance": margin_balance,
                                    "total": total,
                                    "usd_price": usd_price,
                                    "usd_available": available * usd_price,
                                    "usd_frozen": frozen * usd_price,
                                    "usd_margin_balance": margin_balance * usd_price,
                                    "usd_total": total * usd_price,
                                    "portfolio_number": acc_portfolio.get("portfolio_number", None),
                                    "portfolio_name": acc_portfolio.get("portfolio_name", None),
                                    "business_line": acc_portfolio.get("business_line", None),
                                    "activity": acc_portfolio.get("activity", None),
                                    "sub_activity": acc_portfolio.get("sub_activity", None),
                                    "strategy": acc_portfolio.get("strategy", None),
                                    "function": acc_portfolio.get("function", None),
                                    "principal": total * usd_price if acc_portfolio.get("portfolio_number", None) in ['8000', '8836', '8838'] else 0,
                                    "agency": total * usd_price if acc_portfolio.get("portfolio_number", None) in ['8002', '8839', '8837'] else 0,
                                })
                except BaseException:
                    pass
        return resp

    def __get_group_key(self, keys: List[str], data: dict):
        key_vs = []
        for key in keys:
            val = data.get(key, "")
            val = "" if val is None else val
            key_vs.append(val)
        return ":".join(key_vs)

    def __group_balance(self, keys: List[str], acc_balances: List[dict]):
        balance_map = {}
        for acc_b in acc_balances:
            key = self.__get_group_key(keys, acc_b)
            available = acc_b["available"]
            frozen = acc_b["frozen"]
            margin_balance = acc_b["margin_balance"]
            total = acc_b["total"]
            usd_price = acc_b["usd_price"]
            usd_available = acc_b["usd_available"]
            usd_frozen = acc_b["usd_frozen"]
            usd_margin_balance = acc_b["usd_margin_balance"]
            usd_total = acc_b["usd_total"]
            principal = acc_b["principal"]
            agency = acc_b["agency"]
            if key not in balance_map:
                balance_map[key] = {
                    "available": 0,
                    "frozen": 0,
                    "margin_balance": 0,
                    "total": 0,
                    "usd_price": usd_price,
                    "usd_available": 0,
                    "usd_frozen": 0,
                    "usd_margin_balance": 0,
                    "usd_total": 0,
                    "principal": 0,
                    "agency": 0,
                }
            balance_map[key]["available"] += available
            balance_map[key]["frozen"] += frozen
            balance_map[key]["margin_balance"] += margin_balance
            balance_map[key]["total"] += total
            balance_map[key]["usd_available"] += usd_available
            balance_map[key]["usd_frozen"] += usd_frozen
            balance_map[key]["usd_margin_balance"] += usd_margin_balance
            balance_map[key]["usd_total"] += usd_total
            balance_map[key]["principal"] += principal
            balance_map[key]["agency"] += agency
        return balance_map

    def __prepare_overrides(self, keys: List[str], key_vals: List[any]):
        overrides = {}
        for i in range(len(keys)):
            overrides.update({
                keys[i]: key_vals[i],
            })
        if "asset" not in keys:
            overrides.update({
                "available": 0,
                "frozen": 0,
                "margin_balance": 0,
                "total": 0,
                "usd_price": 0,
            })
        return overrides

    def __format_balance_output(self, keys: List[str], balance_map: dict):
        resp = []
        total_obj = {
            "account_id": 0,
            "account_name": "Total",
            "exchange_name": "Total",
            "asset": "Total",
            "available": 0,
            "frozen": 0,
            "margin_balance": 0,
            "total": 0,
            "usd_price": 0,
            "usd_available": 0,
            "usd_frozen": 0,
            "usd_margin_balance": 0,
            "usd_total": 0,
            "portfolio_number": "all",
            "portfolio_name": "all",
            "business_line": "all",
            "activity": "all",
            "sub_activity": "all",
            "strategy": "all",
            "function": "all",
            "principal": 0,
            "agency": 0,
        }
        for key in balance_map:
            key_vals = key.split(":")
            overrides = self.__prepare_overrides(keys, key_vals)
            usd_available = overrides.get("usd_available", balance_map[key]["usd_available"])
            usd_frozen = overrides.get("usd_frozen", balance_map[key]["usd_frozen"])
            usd_margin_balance = overrides.get("usd_margin_balance", balance_map[key]["usd_margin_balance"])
            usd_total = overrides.get("usd_total", balance_map[key]["usd_total"])
            principal = overrides.get("principal", balance_map[key]["principal"])
            agency = overrides.get("agency", balance_map[key]["agency"])
            resp.append({
                "account_id": 0,
                "account_name": overrides.get("account_name", "all"),
                "exchange_name": overrides.get("exchange_name", "all"),
                "asset": overrides.get("asset", "all"),
                "available": overrides.get("available", balance_map[key]["available"]),
                "frozen": overrides.get("frozen", balance_map[key]["frozen"]),
                "margin_balance": overrides.get("margin_balance", balance_map[key]["margin_balance"]),
                "total": overrides.get("total", balance_map[key]["total"]),
                "usd_price": overrides.get("usd_price", balance_map[key]["usd_price"]),
                "usd_available": usd_available,
                "usd_frozen": usd_frozen,
                "usd_margin_balance": usd_margin_balance,
                "usd_total": usd_total,
                "portfolio_number": overrides.get("portfolio_number", "all"),
                "portfolio_name": overrides.get("portfolio_name", "all"),
                "business_line": overrides.get("business_line", "all"),
                "activity": overrides.get("activity", "all"),
                "sub_activity": overrides.get("sub_activity", "all"),
                "strategy": overrides.get("strategy", "all"),
                "function": overrides.get("function", "all"),
                "principal": principal,
                "agency": agency,

            })
            total_obj["usd_available"] += usd_available
            total_obj["usd_frozen"] += usd_frozen
            total_obj["usd_margin_balance"] += usd_margin_balance
            total_obj["usd_total"] += usd_total
            total_obj["principal"] += principal
            total_obj["agency"] += agency
        sorted_resp = sorted(resp, key=itemgetter('function', 'usd_total'), reverse=True)
        sorted_resp.append(total_obj)
        return sorted_resp

    def get_formated_account_balances(self, account_ids: List[str], group_by: List[str] = []):
        resp = []
        acc_balances = self.get_account_balances(account_ids)
        if "portfolio" in group_by and "exchange" in group_by and "asset" in group_by:
            keys = ["portfolio_number", "portfolio_name", "business_line", "activity", "sub_activity", "strategy", "function", "exchange_name", "asset"]
        elif "portfolio" in group_by and "exchange" in group_by:
            keys = ["portfolio_number", "portfolio_name", "business_line", "activity", "sub_activity", "strategy", "function", "exchange_name"]
        elif "portfolio" in group_by and "asset" in group_by:
            keys = ["portfolio_number", "portfolio_name", "business_line", "activity", "sub_activity", "strategy", "function", "asset"]
        elif "portfolio" in group_by:
            keys = ["portfolio_number", "portfolio_name", "business_line", "activity", "sub_activity", "strategy", "function"]
        elif "activity" in group_by and "exchange" in group_by and "asset" in group_by:
            keys = ["activity", "exchange_name", "asset"]
        elif "activity" in group_by and "exchange" in group_by:
            keys = ["activity", "exchange_name"]
        elif "activity" in group_by and "asset" in group_by:
            keys = ["activity", "asset"]
        elif "activity" in group_by:
            keys = ["activity"]
        elif "subactivity" in group_by and "exchange" in group_by and "asset" in group_by:
            keys = ["sub_activity", "exchange_name", "asset"]
        elif "subactivity" in group_by and "exchange" in group_by:
            keys = ["sub_activity", "exchange_name"]
        elif "subactivity" in group_by and "asset" in group_by:
            keys = ["sub_activity", "asset"]
        elif "subactivity" in group_by:
            keys = ["sub_activity"]
        elif "exchange" in group_by and "asset" in group_by:
            keys = ["exchange_name", "asset"]
        elif "exchange" in group_by and "function" in group_by:
            keys = ["function", "exchange_name"]
        elif "exchange" in group_by:
            keys = ["exchange_name"]
        elif "asset" in group_by:
            keys = ["asset"]
        elif "account" in group_by:
            keys = ["account_name"]
        elif "all" in group_by:
            keys = ["all"]
        else:
            return acc_balances
        balance_map = self.__group_balance(keys, acc_balances)
        resp = self.__format_balance_output(keys, balance_map)
        return resp

    def sync_balance(self, pipe_batch_size):
        """Sync all balance data from EMS redis in tokyo to OMS redis in Singapore
        keys: balance:Broker:*
        """
        err, account_list = self.optimus_client.get_valid_nitro_account_list()
        if err is not None:
            raise Exception(
                f"get_valid_account_product_list error:{err}")

        ems_pipe = self.ems_redis.pipeline(transaction=False)
        oms_pipe = self.oms_redis.pipeline(transaction=False)
        key_list = []
        for account in account_list:
            key = self.__get_balance_key(account["nitro_account_id"])
            key_list.append(key)
            ems_pipe.hgetall(key)
            if len(key_list) % pipe_batch_size == 0:
                self.__sync_pipeline(ems_pipe, key_list, oms_pipe)
                key_list = []
                oms_pipe = self.oms_redis.pipeline(transaction=False)
                ems_pipe = self.ems_redis.pipeline(transaction=False)
        if key_list:
            self.__sync_pipeline(ems_pipe, key_list, oms_pipe)
        return

    def __sync_pipeline(self, ems_pipe, key_list, oms_pipe):
        results = ems_pipe.execute()
        for i in range(len(results)):
            if results[i]:
                key = key_list[i]
                oms_pipe.hmset(key, results[i])
                # Put expiry in case account becomes invalid
                oms_pipe.expire(key, 86400)
                self.logger.debug(f'syncing key: {key} data: {results[i]}')
        oms_pipe.execute()
