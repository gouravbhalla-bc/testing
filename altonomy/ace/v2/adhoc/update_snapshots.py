import argparse
import traceback
from datetime import datetime

from dateutil.relativedelta import relativedelta

from altonomy.ace.db.sessions_l2 import SessionLocal
from altonomy.ace.models import FeedV2, TradeV2
from altonomy.ace.v2.athena.processor import AthenaProcessor
from altonomy.ace.v2.athena.trade_processor import AthenaTradeProcessor
from altonomy.ace.v2.feed.daos import FeedV2Dao
from altonomy.ace.v2.log_util import get_v2_logger
from altonomy.ace.v2.trade.daos import TradeV2Dao


db = SessionLocal()
p = AthenaProcessor(db)
pt = AthenaTradeProcessor(db)
feed_dao = FeedV2Dao(db, FeedV2)
trade_dao = TradeV2Dao(db, TradeV2)


def run(inp_ptfs, inp_assets, log_name, start, end):
    logger = get_v2_logger(log_name)
    till = datetime.utcnow()
    while start <= end:
        ptfs = inp_ptfs if inp_ptfs != "all" else feed_dao.get_all_portfolios()
        for ptf in ptfs:
            assets = inp_assets if inp_assets != "all" else feed_dao.get_all_assets(ptf)
            for a in assets:
                try:
                    logger.info(f"{start} | {ptf} | {a}")
                    p.update(ptf, a, {start}, till)

                    contract_product_pairs = feed_dao.get_all_product_contract_pair(
                        ptf, a
                    )
                    for contract, product in contract_product_pairs:
                        p.update_summary_v2(ptf, a, contract, product, {start}, till)

                    counterparties = feed_dao.get_all_counterparties(ptf, a)
                    for cp_ref, cp_name in counterparties:
                        p.update_counterparty(ptf, a, cp_ref, cp_name, {start}, till)
                except Exception:
                    logger.error(traceback.format_exc())

            pairs = trade_dao.get_all_pairs(ptf)
            for base_asset, quote_asset in pairs:
                pt.update(ptf, base_asset, quote_asset, {start}, till)
        start += relativedelta(days=1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("ptfs", type=str, default=None)
    parser.add_argument("assets", type=str, default=None)
    parser.add_argument("start", type=str, default="2020-12-01T01:00:00")
    parser.add_argument("--end", type=str, default=None)
    parser.add_argument(
        "--log", dest="log_name", type=str, default="cmd_update_athena_feed"
    )
    args = parser.parse_args()

    ptfs = args.ptfs.split(",") if args.ptfs != "all" else "all"
    assets = args.assets.split(",") if args.assets != "all" else "all"
    start = datetime.strptime(args.start, "%Y-%m-%dT%H:%M:%S")
    end = datetime.strptime(
        args.end if args.end is not None else args.start, "%Y-%m-%dT%H:%M:%S"
    )
    log_name = args.log_name
    print("portfolio=", ptfs)
    print("assets=", assets)
    print("start=", start)
    print("end=", end)
    print("log_name=", log_name)
    run(ptfs, assets, log_name, start, end)
