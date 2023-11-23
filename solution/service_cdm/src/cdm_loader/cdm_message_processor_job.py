from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository


class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 batch_size: int,
                 logger: Logger) -> None:

        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._batch_size = batch_size
        self._logger = logger

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            self._logger.info(f"{datetime.utcnow()}: Message received")

            user_category = msg['payload']['user_category_counters']
            user_product = msg['payload']['user_product_counters']

            for item in user_category:
                self._cdm_repository.user_category_counters_insert(item['user_id'],
                                                                   item['category_id'],
                                                                   item['category_name'],
                                                                   item['order_cnt'])

            for item in user_product:
                self._cdm_repository.user_product_counters_insert(item['user_id'],
                                                                  item['product_id'],
                                                                  item['product_name'],
                                                                  item['order_cnt'])

            self._logger.info(f"{datetime.utcnow()}. Data in mart upload.")

        self._logger.info(f"{datetime.utcnow()}: FINISH")
