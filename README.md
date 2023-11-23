# Проект по облачным технологиям
### Используемые технологии: Yandex Cloud, Kubernetes, Helm, Postgres, SQL, Kafka, Redis, Python, Docker, Flask.


Необходимо реализовать 2 сервиса для загрузки данных в dds и cdm слои. Сервис stg был реализован ранее в спринте.
Схема работы сервисов на схеме image в корне репозитория.

1. Создание dds сервиса: распланировать работу над сервисом
2. Создание dds сервиса: написать код сервиса
3. Создание dds сервиса: зарелизить сервис в Kubernetes
4. Создание cdm сервиса: распланировать работу над сервисом
5. Создание cdm сервиса: написать код сервиса
6. Создание cdm сервиса: зарелизить сервис в Kubernetes
7. Написать Dockerfile на основе шаблона.
8. Запушить образ в Container Registry.
9. Развернуть образ в Kubernetes.
10. Подключиться к Postgres и убедиться, что таблицы заполняются данными.

Ссылка на репозиторий сервиса dds: cr.yandex/crpgb6pfbqsj4us5jako/dds-service:v2023-11-11-r6
Ссылка на репозиторий сервиса cdm: cr.yandex/crpgb6pfbqsj4us5jako/cdm-service:v2023-11-11-r6
