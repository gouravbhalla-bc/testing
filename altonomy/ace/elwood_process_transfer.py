from http.client import HTTPException
import inspect
import os
import sys
from altonomy.ace.db.xalphha_sessions import XAplphaSessionLocal
from altonomy.ace.v2.elwood.ctrls.elwood_settlement_ctrl import ElwoodSettlementCtrl


current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
parent_dir = os.path.dirname(parent_dir)
sys.path.insert(0, parent_dir)

import logging
import datetime

import requests
from altonomy.ace.config import (OPTIMUS_EP, OPTIMUS_PASSWORD, OPTIMUS_USERNAME)
from altonomy.ace.v2.elwood.ctrls.elwood_ctrl import ElwoodCtrl
from altonomy.ace.db.sessions import SessionLocal
from altonomy.ace.common import api_utils

logger = logging.getLogger('long_running')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
logger.addHandler(handler)


def login():
    url = f"{OPTIMUS_EP}/auth_api/auth/login"
    body = {
        "username": f"{OPTIMUS_USERNAME}",
        "password": f"{OPTIMUS_PASSWORD}",
    }
    resp = requests.post(url, json=body, verify=False)

    r = resp.json()
    token = r.get("jwt_token")
    return token


def process(token):
    now = datetime.datetime.utcnow()
    logger.info(f"==== Start time: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("Start session")
    session = SessionLocal()
    xalpha_session = XAplphaSessionLocal()

    elwood_ctrl = ElwoodCtrl(session, xalpha_session)
    logger.info("Load Users")
    err, users = api_utils.get_users(token)
    if err:
        raise HTTPException(403, err)
    logger.info("Load Trades")
    elwood_ctrl.process(users)
    elwood_settlement_ctrl = ElwoodSettlementCtrl(session)
    elwood_settlement_ctrl.elwood_trade_transfer(token)

    session.close()
    xalpha_session.close()
    logger.info("End session")
    now = datetime.datetime.utcnow()
    logger.info(f"==== End time: {now.strftime('%Y-%m-%d %H:%M:%S')}")


def main():
    token = login()
    process(token)


if __name__ == "__main__":
    main()
