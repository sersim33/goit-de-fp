from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
import os

from configs import sql_config, kafka_config
# Setup Kafka from PySpark
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 pyspark-shell"
)

# Setup Kafka
kafka_config = {
    "bootstrap_servers": ['77.81.230.104:9092'],
    "username": 'admin',
    "password": 'VawEzo1ikLtrA8Ug8THa',
    "security_protocol": 'SASL_PLAINTEXT',
    "sasl_mechanism": 'PLAIN'
}
kafka_config["sasl_jaas_config"] = f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" password="{kafka_config["password"]}";'

# Setup MySQL
sql_config = {
    "url": "jdbc:mysql://217.61.57.46:3306/olympic_dataset",
    "user": sql_config['user'],
    "password": sql_config['password'],
    "driver": "com.mysql.cj.jdbc.Driver",
    "table_results": sql_config['table_results'],
    "table_bio": sql_config['table_bio'],
}

# Topic names
my_name = "Sergii_Simak"
athlete_event_results_topic = f"{my_name}_athlete_event_results"
athlete_summary_topic = f"{my_name}_athlete_summary"

# Initialize Spark Session
spark = (
    SparkSession.builder.config("spark.jars", "mysql-connector-j-8.0.32.jar")
    .config("spark.sql.streaming.checkpointLocation", "checkpoint")
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .appName("StreamingPipeline")
    .master("local[*]")
    .getOrCreate()
)

# Schema for Kafka
schema = StructType(
    [
        StructField("athlete_id", IntegerType(), True),
        StructField("sport", StringType(), True),
        StructField("medal", StringType(), True),
        StructField("timestamp", StringType(), True),
    ]
)

# Read data from MySQL using JDBC
results_df = (
    spark.read.format("jdbc")
    .options(
        url=sql_config["url"],
        driver=sql_config["driver"],
        dbtable=sql_config["table_results"],
        user=sql_config["user"],
        password=sql_config["password"],
        partitionColumn="result_id",
        lowerBound=1,
        upperBound=1000000,
        numPartitions="10",
    )
    .load()
)

# Send data to Kafka 
results_df.selectExpr(
    "CAST(result_id AS STRING) AS key",
    "to_json(struct(*)) AS value",
).write.format("kafka").option(
    "kafka.bootstrap.servers", kafka_config["bootstrap_servers"]
).option(
    "kafka.security.protocol", kafka_config["security_protocol"]
).option(
    "kafka.sasl.mechanism", kafka_config["sasl_mechanism"]
).option(
    "kafka.sasl.jaas.config", kafka_config["sasl_jaas_config"]
).option(
    "topic", athlete_event_results_topic
).save()

# Read data from Kafka
kafka_streaming_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"])
    .option("kafka.security.protocol", kafka_config["security_protocol"])
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
    .option("kafka.sasl.jaas.config", kafka_config["sasl_jaas_config"])
    .option("subscribe", athlete_event_results_topic)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "5")
    .option("failOnDataLoss", "false")
    .load()
    .withColumn("value", regexp_replace(col("value").cast("string"), "\\\\", ""))
    .withColumn("value", regexp_replace(col("value"), '^"|"$', ""))
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.athlete_id", "data.sport", "data.medal")
)

# Read athlete bio data from MySQL
athlete_bio_df = (
    spark.read.format("jdbc")
    .options(
        url=sql_config["url"],
        driver=sql_config["driver"],
        dbtable=sql_config["table_bio"],
        user=sql_config["user"],
        password=sql_config["password"],
        partitionColumn="athlete_id",
        lowerBound=1,
        upperBound=1000000,
        numPartitions="10",
    )
    .load()
)

# Filter and validation of athlete data
athlete_bio_df = athlete_bio_df.filter(
    (col("height").isNotNull())
    & (col("weight").isNotNull())
    & (col("height").cast("double").isNotNull())
    & (col("weight").cast("double").isNotNull())
)

# Join data stream Kafka with athlete bio data
joined_df = kafka_streaming_df.join(athlete_bio_df, "athlete_id")

# Aggregation by sport and medals
aggregated_df = joined_df.groupBy("sport", "medal", "sex", "country_noc").agg(
    avg("height").alias("avg_height"),
    avg("weight").alias("avg_weight"),
    current_timestamp().alias("timestamp"),
)

# Send data to Kafka and MySQL
def foreach_batch_function(df, epoch_id):
    df.selectExpr(
        "CAST(NULL AS STRING) AS key", "to_json(struct(*)) AS value"
    ).write.format("kafka").option(
        "kafka.bootstrap.servers", kafka_config["bootstrap_servers"]
    ).option(
        "kafka.security.protocol", kafka_config["security_protocol"]
    ).option(
        "kafka.sasl.mechanism", kafka_config["sasl_mechanism"]
    ).option(
        "kafka.sasl.jaas.config", kafka_config["sasl_jaas_config"]
    ).option(
        "topic", athlete_summary_topic
    ).save()

    df.write.format("jdbc").options(
        url=sql_config["url"],
        driver=sql_config["driver"],
        dbtable=sql_config["table_results"],
        user=sql_config["user"],
        password=sql_config["password"],
    ).mode("append").save()


# Streaming processing
aggregated_df.writeStream.outputMode("complete").foreachBatch(
    foreach_batch_function
).option("checkpointLocation", "./checkpoint_dir").start().awaitTermination()
