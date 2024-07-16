# ETL сервис по миграции данных из Postgres в Elasticsearch

## Описание

ETL сервис выполняет отказоустойчивый перенос данных из Postgres в Elasticsearch, обеспечивая:

- **Загрузка фильмов в Elasticsearch:** Данные загружаются в индекс `movies` согласно предложенной схеме индекса.
- **Отказоустойчивость:** При потере связи с Elasticsearch или Postgres используется техника backoff, чтобы избежать помех при восстановлении базы данных.
- **Продолжение работы с места остановки:** При перезапуске приложения оно продолжает работу с места остановки, а не начинает процесс заново, благодаря хранению состояния.
- **Прохождение Postman-тестов:** Данные в Elasticsearch успешно проходят проверку с использованием Postman-тестов.
