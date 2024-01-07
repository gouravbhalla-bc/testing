import contextlib
import time
import traceback
from typing import List

import redis
from altonomy.ace import config
from altonomy.ace.db.sessions_l2 import SessionLocal
from altonomy.ace.v2.log_util import get_v2_logger

SLEEP_INTERVAL = 1
BATCH_SIZE = 1000


class AcePublisher(object):

    def get_stream_key(self) -> str:
        raise NotImplementedError()

    def get_log_name(self) -> str:
        raise NotImplementedError()

    def get_items(self, batch_size: int) -> List[dict]:
        raise NotImplementedError()

    def get_trim_len(self) -> int:
        raise NotImplementedError()

    def __init__(self):
        self.r: redis.Redis = redis.Redis(
            host=config.S_REDIS_HOST,
            port=config.S_REDIS_PORT,
            password=config.S_REDIS_PASS,
            decode_responses=True,
        )
        self.logger = get_v2_logger(self.get_log_name())

    def get_last_payload(self) -> dict:
        last = self.r.xrevrange(self.get_stream_key(), count=1)
        if len(last) == 1:
            _stream_id, payload = last[0]
            return payload
        else:
            return {}

    @contextlib.contextmanager
    def get_session(self):
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()

    def _push_to_queue_(self, items: List[dict]):
        for item in items:
            self.r.xadd(self.get_stream_key(), item)

    def _trim_queue_(self):
        self.r.xtrim(self.get_stream_key(), self.get_trim_len(), approximate=True)

    def run(self):
        self.logger.info("[INFO] PublishQueue started")
        try:
            while True:
                items = self.get_items(BATCH_SIZE)

                if len(items) == 0:
                    time.sleep(SLEEP_INTERVAL)
                    continue

                self._push_to_queue_(items)
                self._trim_queue_()

        except Exception as e:
            self.logger.info("[ERROR] PublishQueue exception")
            self.logger.debug(traceback.format_exc())
            raise e

        finally:
            self.logger.info("[INFO] PublishQueue stopped")
