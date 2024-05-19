# segmentation-server
Симуляция транспортного уровня: разбиение сообщения на сегменты.
### Запуск:
docker-compose up -d
### Запуск после внесения изменений:
docker-compose up -d --build --remove-orphans
### Остановка:
docker stop $(docker ps -a -q --filter name=segmentation-servergit-service)