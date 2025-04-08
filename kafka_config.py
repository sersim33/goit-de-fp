# Setup Kafka
kafka_config = {
    "bootstrap_servers": '77.81.230.104:9092',
    "username": 'admin',
    "password": 'VawEzo1ikLtrA8Ug8THa',
    "security_protocol": 'SASL_PLAINTEXT',
    "sasl_mechanism": 'PLAIN'
}
kafka_config["sasl_jaas_config"] = f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" password="{kafka_config["password"]}";'

