import multiprocessing as mp
import time

import schedule
from altonomy.ace.v2.athena.processor import create_schedule_message, last_batch_date
from altonomy.ace.v2.athena.trade_processor import create_trade_schedule_message, last_trade_batch_date
from altonomy.ace.v2.log_util import get_v2_logger


trade_logger = get_v2_logger("athena_trade_snapshot_scheduler")
feed_logger = get_v2_logger("athena_feed_snapshot_scheduler")


def feed_job(writer_queue, logger):
    effective_date = last_batch_date()
    trade_date = effective_date
    msg = create_schedule_message(effective_date, trade_date)
    logger.info(f"Scheduler | {effective_date} | Put")
    writer_queue.put(msg)
    logger.info(f"Scheduler | {effective_date} | Done")


def trade_job(writer_queue, logger):
    effective_date = last_trade_batch_date()
    trade_date = effective_date
    msg = create_trade_schedule_message(effective_date, trade_date)
    logger.info(f"Scheduler | {effective_date} | Put")
    writer_queue.put(msg)
    logger.info(f"Scheduler | {effective_date} | Done")


def do_schedule(writer_queue, job_predicate, logger):
    schedule.every().day.at("01:00").do(job_predicate, writer_queue, logger)

    while True:
        schedule.run_pending()
        time.sleep(1)


def create_feed_schedule_process(queue: mp.Queue) -> mp.Process:
    return mp.Process(target=do_schedule, args=(queue, feed_job, feed_logger))


def create_trade_schedule_process(queue: mp.Queue) -> mp.Process:
    return mp.Process(target=do_schedule, args=(queue, trade_job, trade_logger))
