## Как проверить:
Поднять кластер:

    docker-compose up -d
Создать топики:
    
    docker exec -it sprint-2-kafka-0-1 kafka-topics.sh \
        --create --topic messages \
        --partitions 2 \
        --replication-factor 2 \
        --bootstrap-server kafka-0:9092

    docker exec -it sprint-2-kafka-0-1 kafka-topics.sh \
        --create --topic filtered_messages \
        --partitions 2 \
        --replication-factor 2 \
        --bootstrap-server kafka-0:9092

    docker exec -it sprint-2-kafka-0-1 kafka-topics.sh \
        --create --topic blocked_users \
        --partitions 2 \
        --replication-factor 2 \
        --bootstrap-server kafka-0:9092

    docker exec -it sprint-2-kafka-0-1 kafka-topics.sh \
        --create --topic forbidden_words \
        --partitions 2 \
        --replication-factor 2 \
        --bootstrap-server kafka-0:9092

Запустить faust_app:
    
    faust -A faust_app worker -l info 

Запустить тесты, если не прошли запустить еще и еще). 
Не очень понимаю почему периодически так долго обрабатываются сообщения.