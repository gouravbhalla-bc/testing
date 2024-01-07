# TODO: clean this up

import redis

STREAM_KEY = "STREAM:ACE:FEEDV2"

r = redis.Redis(host="localhost", port=6379, password=None, decode_responses=True)
print(r.ping())
# r.xgroup_setid(STREAM_KEY, "ace.v2.feed.consumer.athena_snapshot", "1611308892360-0")

a = r.xrevrange(STREAM_KEY, count=1)
print(a)


# from altonomy.ace.v2.feed.daos import FeedV2Dao
# from altonomy.ace.v2.trade.daos import TradeV2Dao
from altonomy.ace.models import FeedV2, TradeV2
from altonomy.ace.common.utils import row_to_json
from altonomy.ace.db.sessions_l2 import SessionLocal
import redis

STREAM_KEY = "STREAM:ACE:FEEDV2"
STREAM_KEY = "STREAM:ACE:FEEDV2_DELETE"
STREAM_KEY = "STREAM:ACE:TRADEV2:CREATE"
STREAM_KEY = "STREAM:ACE:TRADEV2:DELETE"

# r = redis.Redis(host="localhost", port=6379, password=None, decode_responses=True)
r = redis.Redis(host="web93.altono.me", port=6379, password=None, decode_responses=True)
print(r.ping())


db = SessionLocal()
feed = db.query(FeedV2).order_by(FeedV2.id.desc()).first()
data = {
    "id": feed.id,
    "json": row_to_json(feed),
}
print(data)
resp = r.xadd(STREAM_KEY, data)
print(resp)

db = SessionLocal()
trade = db.query(TradeV2).order_by(TradeV2.id.desc()).first()
trade = db.query(TradeV2).order_by(TradeV2.effective_date_end.desc()).first()
data = {
    "id": trade.id,
    "json": row_to_json(trade),
}
print(data)
resp = r.xadd(STREAM_KEY, data)
print(resp)
