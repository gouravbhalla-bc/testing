import argparse
import logging
import requests
import time
import traceback

from altonomy.ace import config
from altonomy.ace.ctrls import SystemFeedCtrl
from altonomy.ace.db.sessions_l2 import SessionLocal
from datetime import datetime
from threading import Thread


def main():
    parser = argparse.ArgumentParser(description='Fetch Deals from XAlpha, convert and insert into Ace feeds.')
    parser.add_argument('date', nargs="?", type=str, default=None, help='Date to use when fetching XAlpha deals.')
    parser.add_argument('-td', '--today', dest='is_daily', action='store_true', help='If set, update current table.')
    parser.set_defaults(is_daily=False)
    parser.add_argument('-ls', '--last_sec', type=int, dest="last_sec", help='Only apply if --today set. Query data from previous x seconds.')
    parser.set_defaults(last_sec=None)
    parser.add_argument('-ps', '--pagesize', type=int, dest='page_size', help='Page Size when requesting deals from XAlpha.')
    parser.set_defaults(page_size=100)
    parser.add_argument('-ci', '--cycle_interval', type=str, dest='cycle_interval', help='long running for x cycles with y interval. E.g. 12,5 = 60s')
    parser.set_defaults(cycle_interval=None)

    args = parser.parse_args()

    as_of = datetime.strptime(args.date, "%d-%m-%Y") if args.date else datetime.utcnow()
    as_of = as_of.replace(hour=0, minute=0, second=0, microsecond=0)
    # as_of_ts = int(as_of.timestamp())

    is_daily = args.is_daily
    last_sec = args.last_sec
    page_size = args.page_size

    cycle_interval = args.cycle_interval

    logging.basicConfig(level=logging.DEBUG, filename=f"pull_xalpha_deals_{is_daily}_{page_size}.log", filemode="a")

    if cycle_interval is not None:
        try:
            cycle, interval = cycle_interval.split(",")
            cycle = int(cycle)
            interval = int(interval)
            for i in range(cycle):
                thread = Thread(target=get_deals, args=(as_of, is_daily, last_sec, page_size, ))
                thread.setDaemon(True)
                thread.start()
                time.sleep(interval)
            exit(9)
        except BaseException:
            get_deals(as_of, is_daily, last_sec, page_size)
    else:
        get_deals(as_of, is_daily, last_sec, page_size)

        if not is_daily:
            update_api_cache()


def update_api_cache():
    url = f"{config.ACE_EP}/ace_api/athena/reset"
    requests.get(url)
    logging.debug("Force cache update")


def get_deals_with_delay(as_of, is_daily, last_sec, page_size, delay):
    time.sleep(delay)
    get_deals(as_of, is_daily, last_sec, page_size)


def get_deals(as_of, is_daily, last_sec, page_size):
    url = f"{config.XALPHA_EP}/xalpha_api/deal/ace/list"
    batch_date = datetime.utcnow()

    logging.debug(f"[START] PULL XALPHA DEAL | Date: {as_of}, last_sec: {last_sec}, is_daily: {is_daily} | {datetime.utcnow()}")

    page_idx = 1
    pages = 1

    session = SessionLocal()
    ctrl = SystemFeedCtrl(session)

    all_deals = []

    while page_idx <= pages:
        logging.debug(f"[FETCH] PULL XALPHA DEAL | Page Index: {page_idx} of {pages}")
        params = {
            "as_of": int(as_of.timestamp()),
            "page_idx": page_idx,
            "page_size": page_size,
        }

        if last_sec is not None:
            params.update({
                "last_sec": last_sec
            })

        try:
            r = requests.get(url, params=params)
        except Exception as e:
            logging.error(f"[ERR  ] PULL XALPHA DEAL | Error: {e}")
            logging.error(traceback.format_exc())
            break

        r = r.json()

        pages = r.get("pages", 1)
        deals = r.get("list", [])

        logging.debug(f"[RECV ] PULL XALPHA DEAL | Page Index: {page_idx} of {pages}, Records On Page: {len(deals)}")

        all_deals.extend(deals)

        page_idx += 1

    logging.debug(f"[SPROC] PULL XALPHA DEAL | Deals: {len(all_deals)} | {datetime.utcnow()}")
    ctrl.handle_xalpha_deals(all_deals, batch_date, is_daily, as_of)
    logging.debug(f"[EPROC] PULL XALPHA DEAL | Deals: {len(all_deals)} | {datetime.utcnow()}")

    logging.debug(f"[END  ] PULL XALPHA DEAL | Date: {as_of}, is_daily: {is_daily}, Pages: {pages} | {datetime.utcnow()}")


if __name__ == "__main__":
    main()
