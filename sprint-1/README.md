## Кратко:
В терминале перейти в папку с docker-compose.yml, выполнить команду:

    docker-compose up -d

Создать топик командой:
    
    docker exec -it sprint-1-kafka-0-1 kafka-topics.sh \
    --create \
    --topic my_topic \
    --bootstrap-server localhost:9092 \
    --partitions 3 \
    --replication-factor 2

Запустить приложение командой:

    docker-compose -f docker-compose.app.yml up --build

Должны отобразиться логи работы двух экземпляров каждого класса

### 1. Как развернуть Kafka-кластер.
В терминале перейти в папку с docker-compose.yml, выполнить команду:

    docker-compose up -d

### 2. Как проверить, что кластер работает.
Выполнить в терминале команду

    docker ps

должно отобразиться 4 запущенных контейнера (сервиса) по одному на брокер и один для UI

### 3. Какие параметры конфигурации использованы.
    - ALLOW_PLAINTEXT_LISTENER=yes
Разрешить запуск брокера в тестовом режиме без шифрования и аутентификации

    - KAFKA_CFG_NODE_ID=2
Id узла (брокера)

    - KAFKA_CFG_PROCESS_ROLES=broker,controller
Узел является как брокером так и контроллером (участвует в кворуме контроллеров)

      - KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER
Какой из определенных ниже слушателей будет использоваться для связи с контроллерами

      - KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@kafka-0:9093,1@kafka-1:9093,2@kafka-2:9093
Список контроллеров кластера с портами

      - KAFKA_KRAFT_CLUSTER_ID=abcdefghijklmnopqrstuv
Id кластера одинаковый для всех узлов в кластере

      - KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:9096
Порты для связи:
9092 - для брокеров и клиентов внутри докер сети, 
9093 - для связи контроллеров
9094 (9095, 9096) - порт который будет доступен на хост машине, пробрасываем из сервиса, 
т.к. в тестовом сетапе все работает на одной машине порты для каждого сервиса различаются

      - KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://kafka-2:9092,EXTERNAL://127.0.0.1:9096
Порты предлагаемые клиентам для подключения внутри докер сети и снаружи соответственно

      - KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT
Использовать PLAINTEXT протокол (без шифрования данных)

### 4. Как проверить работу Kafka через Kafka UI.
После запуска docker-compose, в браузере по адресу http://localhost:8080/ui/clusters/kraft/brokers должно
отображаться три активных брокера.

