from datetime import datetime
from logging import Logger
from typing import List, Dict
import uuid

from lib.kafka_connect import KafkaConsumer
from lib.kafka_connect import KafkaProducer
from dds_loader.repository.dds_repository import DdsRepository


class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 batch_size: int,
                 logger: Logger) -> None:

        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._batch_size = batch_size
        self._logger = logger

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            self._logger.info(f"{datetime.utcnow()}: Message received")

            order = msg['payload']
            restaurant = order['restaurant']
            user = order['user']
            products = order['products']

            # Загрузка данных в базу данных postgres слой dds
            # Загружаем данные в хабы, линки и саттелиты по заказу, ресторану, пользователю
            order_uuid = self._dds_repository.h_order_insert(
                order['id'], order['date'], datetime.now(), 'orders-system-kafka')
            self._logger.info(
                f"{datetime.utcnow()}: Order information has been loaded.")

            restaurant_uuid = self._dds_repository.h_restaurant_insert(
                restaurant['id'], datetime.now(), 'orders-system-kafka')
            self._logger.info(
                f"{datetime.utcnow()}: Restaurant information has been loaded.")

            user_uuid = self._dds_repository.h_user_insert(
                user['id'], datetime.now(), 'orders-system-kafka')
            self._logger.info(
                f"{datetime.utcnow()}: User information has been loaded.")

            self._dds_repository.l_order_user_insert(
                order_uuid, user_uuid, datetime.now(), 'orders-system-kafka')

            self._dds_repository.s_order_cost_insert(
                order_uuid, order['cost'], order['payment'], datetime.now(), 'orders-system-kafka')
            self._dds_repository.s_order_status_insert(
                order_uuid, order['status'], datetime.now(), 'orders-system-kafka')
            self._dds_repository.s_restaurant_names_insert(
                restaurant_uuid, restaurant['name'], datetime.now(), 'orders-system-kafka')
            self._dds_repository.s_user_names_insert(
                user_uuid, user['name'], datetime.now(), 'orders-system-kafka')

            msg_products = []
            msg_categories = []
            # Загружаем данные по продуктам и категориям продуктов, хабы, линки, саттелиты
            for product in products:
                # Записываем данные в базу
                category_uuid = self._dds_repository.h_category_insert(
                    product['category'], datetime.now(), 'orders-system-kafka')
                self._logger.info(
                    f"{datetime.utcnow()}: Product category information has been loaded.")

                product_uuid = self._dds_repository.h_product_insert(
                    product['id'], datetime.now(), 'orders-system-kafka')
                self._logger.info(
                    f"{datetime.utcnow()}: Product information has been loaded.")

                self._dds_repository.l_order_product_insert(
                    order_uuid, product_uuid, datetime.now(), 'orders-system-kafka')
                self._dds_repository.l_product_category_insert(
                    product_uuid, category_uuid, datetime.now(), 'orders-system-kafka')
                self._dds_repository.l_product_restaurant_insert(
                    product_uuid, restaurant_uuid, datetime.now(), 'orders-system-kafka')

                self._dds_repository.s_product_names_insert(
                    product_uuid, product['name'], datetime.now(), 'orders-system-kafka')

                # Формируем массивы данных для отправки в кафку
                mart_user_product_counters = self._dds_repository.mart_user_product_counters_select(
                    user_uuid, product_uuid)
                if mart_user_product_counters is not None:
                    msg_products.append(self._format_product(
                        mart_user_product_counters))

                mart_user_category_counters = self._dds_repository.mart_user_category_counters_select(
                    user_uuid, category_uuid)
                if mart_user_category_counters is not None:
                    msg_categories.append(self._format_category(
                        mart_user_category_counters))

            # Собираем сообщение
            mart_msg = {
                "object_type": "data_mart",
                "payload": {
                    "user_category_counters": msg_categories,
                    "user_product_counters": msg_products
                }
            }
            self._logger.info("Message ", mart_msg)
            self._producer.produce(mart_msg)
            self._logger.info(f"{datetime.utcnow()}. Message Sent")

        self._logger.info(f"{datetime.utcnow()}: FINISH")

    def _format_product(self, items: tuple):
        row = {
            "user_id": str(items[0]),
            "product_id": str(items[1]),
            "product_name": items[2],
            "order_cnt": items[3]
        }
        return row

    def _format_category(self, items: tuple):
        row = {
            "user_id": str(items[0]),
            "category_id": str(items[1]),
            "category_name": items[2],
            "order_cnt": items[3]
        }
        return row
