# from confluent_kafka import Producer
# import requests
# import json
# import time

# # Kafka configuration
# KAFKA_CONFIG = {
#     'bootstrap.servers': 'localhost:9092',  # Kafka broker address
#     'client.id': 'pollution-data-producer'  # Optional: Unique identifier for the client
# }

# TOPIC_NAME = 'pollution_data'

# # Initialize Kafka producer
# producer = Producer(KAFKA_CONFIG)

# # API endpoint and key (replace with actual API details)
# RESOURCE_ID = "3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69"
# API_KEY = "579b464db66ec23bdd000001945da6fa109945426c2343e66982d926"
# API_ENDPOINT = f"https://api.data.gov.in/resource/{RESOURCE_ID}"

# # Function to fetch pollution data from the API
# def fetch_pollution_data():
#     params = {'api-key': API_KEY, 'format': 'json'}
#     try:
#         response = requests.get(API_ENDPOINT, params=params)
#         if response.status_code == 200:
#             return response.json()
#         else:
#             print(f"Failed to fetch data: HTTP {response.status_code}")
#             return None
#     except requests.RequestException as e:
#         print(f"Error while fetching data: {e}")
#         return None

# # Callback function for delivery report
# def delivery_report(err, msg):
#     if err is not None:
#         print(f"Delivery failed for record {msg.key()}: {err}")
#     else:
#         print(f"Record successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

# # Main loop to fetch data and send to Kafka
# while True:
#     data = fetch_pollution_data()
#     if data:
#         try:
#             # Produce the message to Kafka
#             producer.produce(
#                 TOPIC_NAME,
#                 key=None,  # Optional: Add a unique key if needed
#                 value=json.dumps(data),  # Serialize data to JSON
#                 callback=delivery_report  # Callback for delivery status
#             )
#             # Flush the producer buffer to ensure message delivery
#             producer.flush()
#             print("Data sent to Kafka:", data)
#         except Exception as e:
#             print(f"Failed to produce message: {e}")
#     else:
#         print("No data fetched to send.")

#     # Wait 10 seconds before fetching the next set of data
#     time.sleep(10)


from confluent_kafka import Producer
import requests
import json
import time

# Kafka configuration
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',  # Kafka broker address
    'client.id': 'pollution-data-producer'  # Unique identifier for the client
}

TOPIC_NAME = 'pollution_data'

# Initialize Kafka producer
producer = Producer(KAFKA_CONFIG)

# API endpoint and key (replace with actual API details)
RESOURCE_ID = "3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69"
API_KEY = "579b464db66ec23bdd000001945da6fa109945426c2343e66982d926"
API_ENDPOINT = f"https://api.data.gov.in/resource/{RESOURCE_ID}"

# Function to fetch pollution data from the API with pagination
def fetch_pollution_data():
    params = {
        'api-key': API_KEY,
        'format': 'json',
        'offset': 0,   # Start from the first record
        'limit': 100   # Number of records per page (adjust if API supports larger limit)
    }
    all_records = []
    
    try:
        while True:
            response = requests.get(API_ENDPOINT, params=params)
            if response.status_code == 200:
                data = response.json()
                records = data.get('records', [])
                if not records:
                    print("No more records found. Pagination complete.")
                    break
                
                all_records.extend(records)
                print(f"Fetched {len(records)} records (Total: {len(all_records)})")
                
                # Move to the next page
                params['offset'] += len(records)
            else:
                print(f"Failed to fetch data: HTTP {response.status_code}")
                break
                
    except requests.RequestException as e:
        print(f"Error while fetching data: {e}")
    
    return all_records


# Callback function for delivery report
def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Record successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


# Main loop to fetch data and send to Kafka
while True:
    records = fetch_pollution_data()
    if records:
        try:
            # Produce the message to Kafka
            producer.produce(
                TOPIC_NAME,
                key=None,
                value=json.dumps({"records": records}),  # Wrap all records in a dictionary
                callback=delivery_report
            )
            # Flush the producer buffer to ensure message delivery
            producer.flush()
            print(f"{len(records)} records sent to Kafka successfully.")
        except Exception as e:
            print(f"Failed to produce message: {e}")
    else:
        print("No data fetched to send.")

    # Wait 10 seconds before fetching the next set of data
    time.sleep(10)
