from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# Initialize Spark session
spark = SparkSession.builder \
    .appName("AdvertiseX Data Processing") \
    .getOrCreate()

# Define schema for ad impressions JSON data
schema_ad_impressions_topic = StructType() \
    .add("ad_creative_id", IntegerType()) \
    .add("user_id", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("website", StringType())
schema_clicks_conversions_topic = StructType() \
    .add("timestamp", TimestampType()) \
    .add("user_id", StringType()) \
    .add("ad_campaign_id", IntegerType()) \    
    .add("conversion_type", StringType())
schema_bid_requests_topic = StructType() \
    .add("user_id", StringType()) \
    .add("auction_id", IntegerType()) \
    .add("ad_targeting", MapType(StringType(), StringType()))

# Function to publish ad impressions data to Kafka
def publish_to_kafka(topic, data):
    for item in data:
        producer.send(topic, item.encode('utf-8'))
    producer.flush()

# Example ad impressions data in JSON format
ad_impressions = [
    {"ad_creative_id": 1, "user_id": "456", "timestamp": "2024-04-18T08:00:00", "website": "example.com"},
    {"ad_creative_id": 2, "user_id": "457", "timestamp": "2024-04-18T09:00:00", "website": "example.net"}
]
clicks_conversions_topic = [
	"timestamp,user_id,ad_campaign_id,conversion_type\n2024-04-10 12:00:00,456,789,signup",
	"timestamp,user_id,ad_campaign_id,conversion_type\n2024-04-18 12:00:00,457,789,signup"
]
bid_requests_topic = [
	{'user_id': '456', 'auction_id': 'abc123', 'ad_targeting': {'geo': 'INDIA'}},
	{'user_id': '457', 'auction_id': 'def123', 'ad_targeting': {'geo': 'INDIA'}}
]

# Publish ad impressions data to Kafka topic
publish_to_kafka('ad_impressions_topic', [json.dumps(impression) for impression in ad_impressions])
publish_to_kafka('clicks_conversions_topic', [csv for csv in clicks_conversions_topic])
publish_to_kafka('bid_requests_topic', [json.dumps(bid_requests) for bid_requests in bid_requests_topic])


# Read ad impressions data from Kafka topic
df3 = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "clicks_conversions_topic") \
    .load()

# Convert Kafka message value to JSON and apply schema
df3 = df3.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema_clicks_conversions_topic).alias("data")) \
    .select("data.*")

# Perform data processing and transformation operations
processed_df3 = df3.filter(col("conversion_type") == "signup")

# Write processed data to HDFS (assuming HDFS is running on localhost)
query3 = processed_df3 \
    .writeStream \
    .format("parquet") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .outputMode("append") \
    .option("path", "/tmp/processed_data") \
    .start()

query3.awaitTermination()


# Read ad impressions data from Kafka topic
df2 = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "bid_requests_topic") \
    .load()

# Convert Kafka message value to JSON and apply schema
df2 = df2.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema_bid_requests_topic).alias("data")) \
    .select("data.*")

# Perform data processing and transformation operations
processed_df2 = df2.filter(col("auction_id") == "def123")

# Write processed data to HDFS (assuming HDFS is running on localhost)
query2 = processed_df2 \
    .writeStream \
    .format("parquet") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .outputMode("append") \
    .option("path", "/tmp/processed_data") \
    .start()

query2.awaitTermination()




# Read ad impressions data from Kafka topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ad_impressions_topic") \
    .load()

# Convert Kafka message value to JSON and apply schema
df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema_ad_impressions_topic).alias("data")) \
    .select("data.*")

# Perform data processing and transformation operations
processed_df = df.filter(col("website") == "example.com")

# Write processed data to HDFS (assuming HDFS is running on localhost)
query = processed_df \
    .writeStream \
    .format("parquet") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .outputMode("append") \
    .option("path", "/tmp/processed_data") \
    .start()

query.awaitTermination()

