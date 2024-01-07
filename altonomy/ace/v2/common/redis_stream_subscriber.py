import time
import traceback
from typing import Callable, Generic, TypeVar

import redis
from altonomy.ace import config
from altonomy.ace.v2.log_util import get_v2_logger

V = TypeVar('V')

RETRY_INTERVAL = 300
PRINT_PER_LOG = 2000


class RedisStreamSubscriber(Generic[V]):

    def __init__(
        self,
        processor: Callable[[str, V], None],
        stream_key: str,
        logger_name: str = "redis_subscriber_unnamed",
        group_name: str = "g1",
        consumer_name: str = "c1",
        last_id: str = "0-0",
    ):
        self.logger = get_v2_logger(logger_name)
        self.logger.info("[INFO   ] INIT START")
        self.r = redis.Redis(host=config.S_REDIS_HOST, port=config.S_REDIS_PORT, password=config.S_REDIS_PASS, decode_responses=True)
        self.processor = processor
        self.stream_key = stream_key
        self.group_name = group_name
        self.consumer_name = consumer_name
        self.last_id = last_id
        self._create_group_()
        self.logger.info(f"[INFO   ] INIT DONE | REDIS   | {config.S_REDIS_HOST}:{config.S_REDIS_PORT}")
        self.logger.info(f"[INFO   ] INIT DONE | REDIS_2 | stream_key={self.stream_key}, group_name={self.group_name}, consumer_name={self.consumer_name}")

    def _create_group_(self):
        try:
            self.r.xgroup_create(self.stream_key, self.group_name, self.last_id)
        except Exception as e:
            self.logger.warning(f"[WARNING] PublishQueue exception | {e}")

    def _process_message_(self, m_id, fields):
        self.processor(m_id, fields)

    def _ack_item_(self, m_id):
        self.r.xack(self.stream_key, self.group_name, m_id)
        # del item after acknowledge the data
        # self.r.xdel(self.stream_key, _id)

    # def del_group(self):
    #     self.r.xgroup_destroy(self.stream_key, self.group_name)

    def loop(self):
        n = 0
        check_backlog = True

        try:
            while True:
                stream_id = self.last_id if check_backlog else '>'
                items = self.r.xreadgroup(
                    self.group_name,
                    self.consumer_name,
                    {self.stream_key: stream_id},
                    count=10,
                    block=2000,
                )

                if items is None or len(items) == 0:
                    continue

                _stream_key, msgs = items[0]

                if len(msgs) == 0:
                    check_backlog = False
                    time.sleep(0.05)

                for msg in msgs:
                    m_id, fields = msg
                    self._process_message_(m_id, fields)
                    self._ack_item_(m_id)

                    self.last_id = m_id

                    message = f"[INFO   ] Stream | Loop | Processing | n = {n} | last_id = {self.last_id}"
                    if n % PRINT_PER_LOG == 0:
                        self.logger.info(message)
                    else:
                        self.logger.debug(message)
                    n += 1

        except Exception as e:
            self.logger.error(f"[ERROR  ] Stream | Loop | PublishQueue exception | {e}")
            self.logger.debug(traceback.format_exc())

        finally:
            self.logger.info(f"[INFO   ] Stream | Loop | Shutdown | n = {n} | last_id = {self.last_id}")

    def run(self):
        while True:
            self.logger.info("[INFO   ] Stream | Loop | Start")
            self.loop()
            self.logger.info("[INFO   ] Stream | Loop | Stop")
            time.sleep(RETRY_INTERVAL)
