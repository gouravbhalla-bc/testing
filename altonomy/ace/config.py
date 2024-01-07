from configparser import ConfigParser
from os.path import expanduser

config = ConfigParser()
home = expanduser("~")

config.read(f"{home}/.altonomy/config.ini")

DB_USER = "root"
DB_PASSWORD = "root"
DB_HOST = "localhost:3306"
DB_DATABASE = "Ace"
DB_HOST_LOCAL = "localhost:3306"

try:
    DB_USER = config["AceDB"]["DB_USERNAME"]
    DB_PASSWORD = config["AceDB"]["DB_PASSWORD"]
    DB_HOST = config["AceDB"]["DB_HOSTNAME"]
    DB_DATABASE = config["AceDB"]["DB_INSTNAME"]
except BaseException:
    pass

XALPHA_DB_USER = "root"
XALPHA_DB_PASSWORD = "root"
XALPHA_DB_HOST = "localhost:3306"
XALPHA_DB_DATABASE = "XAlpha"

try:
    XALPHA_DB_USER = config["XAlphaDB"]["DB_USERNAME"]
    XALPHA_DB_PASSWORD = config["XAlphaDB"]["DB_PASSWORD"]
    XALPHA_DB_HOST = config["XAlphaDB"]["DB_HOSTNAME"]
    XALPHA_DB_DATABASE = config["XAlphaDB"]["DB_INSTNAME"]
except BaseException:
    pass

try:
    DB_HOST_LOCAL = config["AceDB"]["DB_HOSTNAME_LOCAL"]
except BaseException:
    DB_HOST_LOCAL = DB_HOST

DB_QUERY_PAGE_SIZE = 100000

try:
    DB_QUERY_PAGE_SIZE = config["AceDB"]["DB_QUERY_PAGE_SIZE"]
except BaseException:
    pass

ALT_CLIENT_ENDPOINT = "http://localhost:8080"

try:
    ALT_CLIENT_ENDPOINT = config["Server"]["ENDPOINT"]
except BaseException:
    pass

EXTERNAL_EP = "http://localhost:3000"

try:
    EXTERNAL_EP = config["Server"]["EXTERNAL_EP"]
except BaseException:
    pass

XALPHA_EP = "http://localhost:8080"

try:
    XALPHA_EP = config["Server"]["XALPHA_EP"]
except BaseException:
    pass

NITRO_EP = "http://localhost:8080"

try:
    NITRO_EP = config["Server"]["NITRO_EP"]
except BaseException:
    pass

ACE_EP = "http://localhost:8080"

try:
    ACE_EP = config["Server"]["ACE_EP"]
except BaseException:
    pass

OPTIMUS_EP = "localhost:8080"

try:
    OPTIMUS_EP = config["Server"]["OPTIMUS_EP"]
except Exception:
    pass

SLACK_TOKEN = ""
OPTIMUS_CHANNEL = ""
OPTIMUS_USERNAME = "alen.key"
OPTIMUS_PASSWORD = "alen.key"

try:
    OPTIMUS_CHANNEL = config["Slack"]["OPTIMUS_CHANNEL"]
    OPTIMUS_USERNAME = config["SettlementEngineDB"]["OPTIMUS_USERNAME"]
    OPTIMUS_PASSWORD = config["SettlementEngineDB"]["OPTIMUS_PASSWORD"]
except BaseException:
    pass

XALPHA_ZMQ_EP = "tcp://localhost:5021"
try:
    XALPHA_ZMQ_EP = config["ZMQ"]["XALPHA_ZMQ_EP"]
except BaseException:
    pass

S_REDIS_HOST = "localhost"
S_REDIS_PORT = 6379
S_REDIS_PASS = None

try:
    S_REDIS_HOST = config["StreamingRedis"].get("REDIS_HOST", "localhost")
    S_REDIS_PORT = config["StreamingRedis"].get("REDIS_PORT", 6379)
    S_REDIS_PASS = config["StreamingRedis"].get("REDIS_PASS", None)
except BaseException:
    pass

S3_BUCKET_ELWOOD = None
S3_ACCESS_KEY = None
S3_SECRET_KEY = None
S3_INVOICE_NAME_PREFIX = None

try:
    S3_BUCKET_ELWOOD = config["S3"].get("S3_BUCKET_ELWOOD")
    S3_ACCESS_KEY = config["S3"].get("S3_ACCESS_KEY")
    S3_SECRET_KEY = config["S3"].get("S3_SECRET_KEY")
    S3_INVOICE_NAME_PREFIX = config["S3"].get("S3_INVOICE_NAME_PREFIX")
except BaseException:
    pass

LOG_TO_FILE = "TRUE"
LOG_TO_STDOUT = "FALSE"

try:
    LOG_TO_FILE = config["Logging"].get("LOG_TO_FILE", "TRUE")
except BaseException:
    pass

try:
    LOG_TO_STDOUT = config["Logging"].get("LOG_TO_STDOUT", "FALSE")
except BaseException:
    pass

OPTIMUS_USERNAME = ""
OPTIMUS_PASSWORD = ""
try:
    OPTIMUS_USERNAME = config["AceVar"]["OPTIMUS_USERNAME"]
    OPTIMUS_PASSWORD = config["AceVar"]["OPTIMUS_PASSWORD"]
except BaseException:
    pass

REDIS_HOST = "localhost"
REDIS_HOST_T = "localhost"
REDIS_PORT = 6379
REDIS_PASS = None
try:
    REDIS_HOST = config['Redis'].get('REDIS_HOST', 'localhost')
    REDIS_HOST_T = config['Redis'].get('REDIS_HOST_T', 'localhost')
    REDIS_PORT = config['Redis'].get('REDIS_PORT', 6379)
    REDIS_PASS = config['Redis'].get('REDIS_PASS', None)
except BaseException:
    pass

LOGGING_LEVEL = "DEBUG"
try:
    LOGGING_LEVEL = config["Logging"]["LOGGINGLEVEL"]
except BaseException:
    pass

ACE_TOKEN = ""
LIVE_BALANCE_CHANNEL = ""
try:
    ACE_TOKEN = config["Slack"]["ACE_TOKEN"]
    LIVE_BALANCE_CHANNEL = config["Slack"]["LIVE_BALANCE_CHANNEL"]
except BaseException:
    pass

LIVE_BALANCE_EXCEPTIONAL_ACCS = []
LIVE_BALANCE_GROUP_BY = ""
try:
    LIVE_BALANCE_EXCEPTIONAL_ACCS = config["AceVar"]["LIVE_BALANCE_EXCEPTIONAL_ACCS"].split(",")
    LIVE_BALANCE_GROUP_BY = config["AceVar"]["LIVE_BALANCE_GROUP_BY"]
except BaseException:
    pass

LIVE_BALANCE_EXCEPTIONAL_EXCHANGE = set()
try:
    LIVE_BALANCE_EXCEPTIONAL_EXCHANGE = set(config["AceVar"]["LIVE_BALANCE_EXCEPTIONAL_EXCHANGE"].split(","))
except BaseException:
    pass

TICKER_FEED_SOURCE = ["Binance", "Coinbase", "Kraken", "Huobi", "MXC", "Gateiov4", "Bitmax"]
try:
    TICKER_FEED_SOURCE = config["AceVar"]["TICKER_FEED_SOURCE"].split(",")
except BaseException:
    pass

FIAT_STABLES_LIST = set(["CUSD", "DAI", "BUSD", "USDT", "USD", "USDC"])
try:
    FIAT_STABLES_LIST = set(config["AceVar"]["FIAT_STABLES_LIST"].split(","))
except BaseException:
    pass

TICKER_FEED_CACHE_SIZE = 1500
try:
    TICKER_FEED_CACHE_SIZE = config["AceVar"]["TICKER_FEED_CACHE_SIZE"]
except BaseException:
    pass

USE_OPTION_PRICER_V2_DATA = 0
try:
    USE_OPTION_PRICER_V2_DATA = int(config["AceVar"]["USE_OPTION_PRICER_V2_DATA"])
except BaseException:
    pass
