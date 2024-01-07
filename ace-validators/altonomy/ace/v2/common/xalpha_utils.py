import json
from datetime import datetime

from altonomy.ace.v2.common.redis_stream_subscriber import \
    RedisStreamSubscriber


def format_raw_deal(raw_deal: str) -> dict:
    deal = json.loads(raw_deal)

    deal.update({
        "valid_from": datetime.utcfromtimestamp(deal.get("valid_from")),
        "valid_to": datetime.utcfromtimestamp(deal.get("valid_to")) if deal.get("valid_to") is not None else None,
        "trade_date": datetime.utcfromtimestamp(deal.get("trade_date")),
        "value_date": datetime.utcfromtimestamp(deal.get("value_date")) if deal.get("value_date") is not None else None,
    })

    return deal


def subscribe_xalpha_deal(predicate, key: str) -> None:

    def process_message(m_id, fields):
        deal = format_raw_deal(fields["json"])
        predicate(deal)

    s = RedisStreamSubscriber(
        processor=process_message,
        group_name=f"ace.v2.xalpha.consumer.{key}",
        logger_name=f"redis_xalpha_consumer_{key}",
        stream_key="STREAM:XALPHA:DEAL",
    )
    s.run()


def format_raw_feed_v2(raw_feed):
    feed = json.loads(raw_feed)

    feed.update({
        date_field: datetime.utcfromtimestamp(feed.get(date_field)) if feed.get(date_field) is not None else None
        for date_field in ('system_record_date', 'value_date', 'trade_date', 'effective_date_start', 'effective_date_end')
    })

    return feed


def subscribe_ace_feed(predicate, key: str, stream_key: str) -> None:

    def process_message(m_id, fields):
        deal = format_raw_feed_v2(fields["json"])
        predicate(deal)

    s = RedisStreamSubscriber(
        processor=process_message,
        group_name=f"ace.v2.feed.consumer.{key}",
        logger_name=f"redis_feed_consumer_{key}",
        stream_key=stream_key,
    )
    s.run()


def format_raw_trade_v2(raw_trade):
    trade = json.loads(raw_trade)

    trade.update({
        date_field: datetime.utcfromtimestamp(trade.get(date_field)) if trade.get(date_field) is not None else None
        for date_field in ('system_record_date', 'value_date', 'trade_date', 'effective_date_start', 'effective_date_end')
    })

    return trade


def subscribe_ace_trade(predicate, key: str, stream_key: str) -> None:

    def process_message(m_id, fields):
        deal = format_raw_trade_v2(fields["json"])
        predicate(deal)

    s = RedisStreamSubscriber(
        processor=process_message,
        group_name=f"ace.v2.trade.consumer.{key}",
        logger_name=f"redis_trade_consumer_{key}",
        stream_key=stream_key,
    )
    s.run()
