import json
from confluent_kafka import Producer

ad_impressions_topic = [
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


def publish(topic, messages):
    p = Producer({'bootstrap.servers': 'localhost:9092'})
    for message in messages:
        p.produce(topic=topic, value=message.encode('utf-8'))
        p.flush()


publish("ad_impressions_topic", [json.dumps(message) for message in ad_impressions_topic])
publish("clicks_conversions_topic", [message for message in clicks_conversions_topic])
publish("bid_requests_topic", [json.dumps(message) for message in bid_requests_topic])

print("Published")
