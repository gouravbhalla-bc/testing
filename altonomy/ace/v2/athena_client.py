import argparse
import multiprocessing as mp
import queue
import traceback
from datetime import datetime
from typing import List
from dateutil.relativedelta import relativedelta

from altonomy.ace.db.sessions_l2 import SessionLocal
from altonomy.ace.v2.athena.processor import (
    AthenaProcessor,
    create_invalidate_counterparty_message,
    create_invalidate_summary_v2_message,
    create_invalidate_message,
    hash_feed_message,
    is_feed_message_mergable,
    last_batch_date,
    merge_feed_message,
)
from altonomy.ace.v2.athena.scheduler import (
    create_feed_schedule_process,
    create_trade_schedule_process,
)
from altonomy.ace.v2.athena.trade_processor import (
    AthenaTradeProcessor,
    create_trade_invalidate_message,
    hash_trade_message,
    is_trade_message_mergable,
    last_trade_batch_date,
    merge_trade_message,
)
from altonomy.ace.v2.common.xalpha_utils import subscribe_ace_feed, subscribe_ace_trade
from altonomy.ace.v2.log_util import get_v2_logger

BULK_MSG_PROCESS_LIMIT = 10000


def write(writer_queue) -> None:
    logger = get_v2_logger("athena_client_feed_snapshot")
    while True:
        values = []

        while len(values) < BULK_MSG_PROCESS_LIMIT:
            try:
                values.append(writer_queue.get(True, 1))
            except queue.Empty:
                if len(values) == 0:
                    continue
                else:
                    break

        non_mergable_msg = []
        mergable_msg = {}

        for value in values:
            if is_feed_message_mergable(value):
                key = hash_feed_message(value)

                if key not in mergable_msg:
                    mergable_msg[key] = value
                else:
                    mergable_msg[key] = merge_feed_message(value, mergable_msg[key])
            else:
                non_mergable_msg.append(value)

        msgs = []
        msgs.extend(mergable_msg.values())
        msgs.extend(non_mergable_msg)

        try:
            db = SessionLocal()
            processor = AthenaProcessor(db)
        except Exception as e:
            logger.error(str(e))
            logger.error(traceback.format_exc())
            continue

        for msg in msgs:
            try:
                processor.write(msg)
            except Exception as e:
                logger.error(str(e))
                logger.error(traceback.format_exc())


def update(feed) -> List[dict]:
    portfolio = feed["portfolio"]
    asset = feed["asset"]
    trade_date_start = feed["trade_date"]
    counterparty_ref = feed["counterparty_ref"]
    counterparty_name = feed["counterparty_name"]
    contract = feed["contract"]
    product = feed["product"]
    trade_date_end = datetime.utcnow()

    if trade_date_start < last_batch_date():
        return [
            create_invalidate_message(
                portfolio, asset, trade_date_start, trade_date_end
            ),
            create_invalidate_counterparty_message(
                portfolio,
                asset,
                counterparty_ref,
                counterparty_name,
                trade_date_start,
                trade_date_end,
            ),
            create_invalidate_summary_v2_message(
                portfolio, asset, product, contract, trade_date_start, trade_date_end
            ),
        ]
    else:
        return []


def subscribe(writer_queue, key: str, stream_key: str) -> None:
    def handle_deal(feed):
        msgs = update(feed)

        for msg in msgs:
            writer_queue.put(msg)

    subscribe_ace_feed(handle_deal, key, stream_key)


def snapshot_feed_publisher():

    writer_queue = mp.Queue()

    snapshot_writer = mp.Process(target=write, args=(writer_queue,))
    snapshot_writer.start()

    snapshot_scheduler = create_feed_schedule_process(writer_queue)
    snapshot_scheduler.start()

    create_manual_p = mp.Process(
        target=subscribe, args=(writer_queue, "athena_manual_snapshot", "STREAM:ACE:MANUALFEEDV2:CREATE")
    )
    create_p = mp.Process(
        target=subscribe, args=(writer_queue, "athena_snapshot", "STREAM:ACE:FEEDV2")
    )
    delete_p = mp.Process(
        target=subscribe,
        args=(writer_queue, "athena_snapshot_delete", "STREAM:ACE:FEEDV2_DELETE"),
    )
    create_manual_p.start()
    create_p.start()
    delete_p.start()


def write_trade(writer_queue) -> None:
    logger = get_v2_logger("athena_client_trade_snapshot")
    while True:
        values = []

        while len(values) < BULK_MSG_PROCESS_LIMIT:
            try:
                values.append(writer_queue.get(True, 1))
            except queue.Empty:
                if len(values) == 0:
                    continue
                else:
                    break

        non_mergable_msg = []
        mergable_msg = {}

        for value in values:
            if is_trade_message_mergable(value):
                key = hash_trade_message(value)

                if key not in mergable_msg:
                    mergable_msg[key] = value
                else:
                    mergable_msg[key] = merge_trade_message(value, mergable_msg[key])
            else:
                non_mergable_msg.append(value)

        msgs = []
        msgs.extend(mergable_msg.values())
        msgs.extend(non_mergable_msg)

        try:
            db = SessionLocal()
            processor = AthenaTradeProcessor(db)
        except Exception as e:
            logger.error(str(e))
            logger.error(traceback.format_exc())
            continue

        for msg in msgs:
            try:
                processor.write(msg)
            except Exception as e:
                logger.error(str(e))
                logger.error(traceback.format_exc())


def update_trade(trade) -> List[dict]:
    portfolio = trade["portfolio"]
    base_asset = trade["base_asset"]
    quote_asset = trade["quote_asset"]
    trade_date_start = trade["trade_date"]
    trade_date_end = datetime.utcnow()

    if trade_date_start < last_trade_batch_date():
        return [
            create_trade_invalidate_message(
                portfolio, base_asset, quote_asset, trade_date_start, trade_date_end
            ),
        ]
    else:
        return []


def subscribe_trade(writer_queue, key: str, stream_key: str) -> None:
    def handle_deal(trade):
        msgs = update_trade(trade)

        for msg in msgs:
            writer_queue.put(msg)

    subscribe_ace_trade(handle_deal, key, stream_key)


def snapshot_trade_publisher():

    writer_queue = mp.Queue()

    snapshot_writer = mp.Process(target=write_trade, args=(writer_queue,))
    snapshot_writer.start()

    snapshot_scheduler = create_trade_schedule_process(writer_queue)
    snapshot_scheduler.start()

    create_p = mp.Process(
        target=subscribe_trade,
        args=(
            writer_queue,
            "athena_trade_snapshot_create",
            "STREAM:ACE:TRADEV2:CREATE",
        ),
    )
    delete_p = mp.Process(
        target=subscribe_trade,
        args=(
            writer_queue,
            "athena_trade_snapshot_delete",
            "STREAM:ACE:TRADEV2:DELETE",
        ),
    )

    create_p.start()
    delete_p.start()


def snapshot_feed_init(snapshot_types: List[str] = None, backdate_duration_days=0):
    db = SessionLocal()
    processor = AthenaProcessor(db)
    trade_date_last = last_batch_date()
    if backdate_duration_days is None:
        backdate_duration_days = 0

    for i in range(backdate_duration_days, -1, -1):
        trade_date = trade_date_last - relativedelta(days=i)
        processor.create(trade_date, trade_date, snapshot_types=snapshot_types)

    # update_all generates snapshots from the day after the given time
    update_date_offset = backdate_duration_days + 1
    trade_date = trade_date_last - relativedelta(days=update_date_offset)
    processor.update_all(trade_date, trade_date_last, snapshot_types=snapshot_types)


def snapshot_trade_init():
    db = SessionLocal()
    processor = AthenaTradeProcessor(db)
    d = last_trade_batch_date()
    processor.create(d, d)


def main():
    parser = argparse.ArgumentParser(description="Athena V2 Client")
    parser.add_argument(
        "command", type=str, default=None, help="The command to execute"
    )
    parser.add_argument("--opt1", type=str, default=None, help="first option value")
    parser.add_argument("--opt2", type=str, default=None, help="second option value")

    args = parser.parse_args()

    cmd = args.command
    if cmd == "snapshot_feed_publisher":
        snapshot_feed_publisher()
    elif cmd == "snapshot_feed_init":
        snapshots = args.opt1
        if snapshots is not None:
            snapshots = snapshots.split(",")
        days = args.opt2
        if days is not None:
            days = int(days)
        snapshot_feed_init(snapshots, days)
    elif cmd == "snapshot_trade_publisher":
        snapshot_trade_publisher()
    elif cmd == "snapshot_trade_init":
        snapshot_trade_init()
    else:
        print(f"Unrecognised command: {cmd}")


if __name__ == "__main__":
    main()
