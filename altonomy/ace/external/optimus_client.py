import json
import time
import copy
import requests
from redis import Redis, StrictRedis
from altonomy.ace import config


_static_optimus_data = {}
REDIS_CACHE_KEY = "st:cache:ace"


class OptimusClient(object):

    def __init__(self, lite: bool = False):
        self.endpoint = config.OPTIMUS_EP
        self.headers = {}
        self.redis: Redis = self.__get_redis_client(config.REDIS_HOST, config.REDIS_PORT, config.REDIS_PASS, True)
        if not lite:
            self.__authenticate()
        self.payloads = [{
            "key": "account_list",
            "path": self.endpoint + "/account_api/account/detail/list",
            "data_keys": [
                "account_id",
                "main_account_id",
                "account_product_id",
                "product",
                "account_source",
                "exchange",
                "account_name",
                "addresses",
                "nitro_account_id",
                "exchange_indicator",
                "function",
                "acc_exchange",
                "internal_status",
                "portfolio_name",
                "portfolio_number",
                "portfolio_entity",
            ],
        }, {
            "key": "portfolio_list",
            "path": self.endpoint + "/portfolio_api/portfolio/list",
            "data_keys": [
                "portfolio_id",
                "portfolio_type",
                "portfolio_number",
                "portfolio_name",
                "business_line",
                "activity",
                "sub_activity",
                "strategy",
                "function",
            ],
        }]

    def __authenticate(self, username: str = config.OPTIMUS_USERNAME, password: str = config.OPTIMUS_PASSWORD):
        err, result = self.__login(username, password)
        if err is None:
            self.headers = {
                "Alt-Auth-Token": result.get("jwt_token", None)
            }
        else:
            raise Exception("authenticate failed")

    def __login(self, username: str, password: str):
        r = requests.post(
            self.endpoint + "/auth_api/auth/login",
            data=json.dumps({
                "username": username,
                "password": password
            }),
            timeout=10
        )
        if r.status_code == 200:
            result = r.json()
            err = None
        else:
            err, result = f"HTTP Error: {r.status_code} {r.text}", None
        return err, result

    def get_valid_nitro_account_list(self):
        try:
            response = requests.get(f'{self.endpoint}/account_api/account_product/list', headers=self.headers)
            if response.status_code == 200:
                response_json = response.json()
                account_list = []
                err = None
                for account in response_json:
                    try:
                        if int(account['nitro_account_id']) > 0:
                            account_list.append(account)
                    except BaseException:
                        pass
            else:
                err, account_list = f"HTTP Error: {response.status_code} {response.text}", None
            return err, account_list
        except BaseException as e:
            return str(e), None

    def __get_redis_client(self, host, port, password, decode_responses):
        redis = StrictRedis(host=host, port=port, password=password, decode_responses=decode_responses)
        try:
            redis.ping()
            return redis
        except Exception:
            return None

    def store_in_mem(self, key: str, data: any):
        try:
            self.store_in_mem_redis(key, data)
        except BaseException:
            # print("revert to store in local cache", e)
            _static_optimus_data.update({key: {"data": data, "timestamp": time.time()}})

    def check_in_mem(self, key: str, expire_in: int):
        try:
            return self.check_in_mem_redis(key, expire_in)
        except BaseException:
            # print("revert to check from local cache", e)
            payload = _static_optimus_data.get(key, {})
            if payload == {}:
                return "data not found", None
            timestamp = payload.get("timestamp", 0)
            now = time.time()
            if now - timestamp > expire_in:
                return "expired", None
            data = payload.get("data", None)
            return None, data

    def store_in_mem_redis(self, key: str, data: any):
        r = self.redis
        _optimus_data = r.hgetall(REDIS_CACHE_KEY)
        _optimus_data.update(
            {key: json.dumps({"data": data, "timestamp": time.time()})}
        )
        r.hmset(REDIS_CACHE_KEY, _optimus_data)
        r.expire(name=REDIS_CACHE_KEY, time=3600)

    def check_in_mem_redis(self, key: str, expire_in: int):
        r = self.redis
        _optimus_data = r.hgetall(REDIS_CACHE_KEY)
        payload = _optimus_data.get(key, "{}")
        payload = json.loads(payload)
        if payload == {}:
            return "data not found", None
        timestamp = payload.get("timestamp", 0)
        now = time.time()
        if now - timestamp > expire_in:
            return "expired", None
        data = payload.get("data", None)
        return None, data

    def request_resource_list(
        self, key: str, path: str, data_keys: list, force: bool = False
    ):
        try:
            err, result = self.check_in_mem(key, 3600)
            if force:
                err = "force trigger"
            if err is not None:
                r = requests.get(path, headers=self.headers, timeout=30, verify=False)
                if r.status_code == 200:
                    result = r.json()
                    err = None
                    if isinstance(result, dict):
                        err = result.get("detail", None)
                    processed_result = []
                    for data in result:
                        processed_data = {}
                        for dk in data_keys:
                            processed_data[dk] = data[dk]
                        processed_result.append(processed_data)
                    self.store_in_mem(key, processed_result)
                    result = processed_result
                else:
                    err, result = f"HTTP Error: {r.status_code} {r.text}", None
            return err, result
        except Exception as e:
            return str(e), None

    def get_all_accounts(self):
        key = self.payloads[0]["key"]
        path = self.payloads[0]["path"]
        data_keys = self.payloads[0]["data_keys"]
        return copy.deepcopy(self.request_resource_list(key, path, data_keys))

    # Append memo in account address if channel is not BTC
    def get_accounts(self):
        err, accounts = self.get_all_accounts()
        if not err:
            for i in range(len(accounts)):
                accounts[i]["addresses"] = [
                    {
                        **addr,
                        "address": addr["address"]
                        if addr["channel"] == "BTC"
                        else (
                            f'{addr["address"]} {addr["memo"]}'
                            if addr["memo"]
                            else addr["address"]
                        ),
                    }
                    for addr in accounts[i]["addresses"]
                ]
            return err, [a for a in accounts if a["internal_status"] != "Dormant"]
        else:
            return err, accounts

    def get_account_product_info(self, account_product_id):
        err, accounts = self.get_accounts()
        account = [
            a for a in accounts if a["account_product_id"] == int(account_product_id)
        ]
        if account == []:
            return {}
        else:
            return account[0]

    def get_portfolios(self):
        key = self.payloads[1]["key"]
        path = self.payloads[1]["path"]
        data_keys = self.payloads[1]["data_keys"]
        return self.request_resource_list(key, path, data_keys)
