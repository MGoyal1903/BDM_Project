# from datetime import datetime
# from confluent_kafka import Consumer, KafkaException, KafkaError
# import mysql.connector
# import json

# # Kafka configuration
# KAFKA_CONFIG = {
#     'bootstrap.servers': 'localhost:9092',  # Kafka broker
#     'group.id': 'pollution_data_consumer_group',  # Consumer group ID
#     'auto.offset.reset': 'earliest'  # Start from the earliest message
# }

# TOPIC_NAME = 'pollution_data'

# # MySQL configuration
# MYSQL_CONFIG = {
#     'host': 'localhost',
#     'user': 'admin',       # Replace with your MySQL username
#     'password': 'admin123',   # Replace with your MySQL password
#     'database': 'airpollutiondata'    # Replace with your database name
# }

# # Create a connection to the MySQL database
# def connect_to_mysql():
#     try:
#         conn = mysql.connector.connect(**MYSQL_CONFIG)
#         cursor = conn.cursor()
#         # Create table if it doesn't exist
#         cursor.execute("""
#             CREATE TABLE IF NOT EXISTS air_quality (
#                 id INT AUTO_INCREMENT PRIMARY KEY,
#                 country VARCHAR(255),
#                 state VARCHAR(255),
#                 city VARCHAR(255),
#                 station VARCHAR(255),
#                 last_update DATETIME,
#                 latitude DECIMAL(10, 7),
#                 longitude DECIMAL(10, 7),
#                 pollutant_id VARCHAR(50),
#                 min_value INT,
#                 max_value INT,
#                 avg_value INT,
#                 created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#             )
#         """)
#         conn.commit()
#         return conn, cursor
#     except mysql.connector.Error as err:
#         print(f"Error connecting to MySQL: {err}")
#         exit(1)

# # Parse and convert date to MySQL-compatible format
# def parse_date(date_str):
#     try:
#         # Convert 'DD-MM-YYYY HH:MM:SS' to 'YYYY-MM-DD HH:MM:SS'
#         return datetime.strptime(date_str, '%d-%m-%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
#     except ValueError as e:
#         print(f"Error parsing date: {e}")
#         return None

# # Save parsed data to MySQL database
# def save_to_mysql(cursor, conn, record):
#     try:
#         # Parse and convert last_update date
#         last_update = parse_date(record.get('last_update'))
#         if not last_update:
#             print(f"Skipping record due to invalid date: {record}")
#             return

#         cursor.execute("""
#             INSERT INTO air_quality (
#                 country, state, city, station, last_update,
#                 latitude, longitude, pollutant_id,
#                 min_value, max_value, avg_value
#             ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#         """, (
#             record.get('country'),
#             record.get('state'),
#             record.get('city'),
#             record.get('station'),
#             last_update,
#             float(record.get('latitude')),
#             float(record.get('longitude')),
#             record.get('pollutant_id'),
#             int(record.get('min_value')),
#             int(record.get('max_value')),
#             int(record.get('avg_value'))
#         ))
#         conn.commit()
#         print(f"Record saved to MySQL: {record}")
#     except mysql.connector.Error as err:
#         print(f"Error saving record to MySQL: {err}")
#     except ValueError as ve:
#         print(f"Error processing record data: {ve}")

# # Kafka Consumer
# def start_consumer():
#     # Initialize Kafka consumer
#     consumer = Consumer(KAFKA_CONFIG)
#     consumer.subscribe([TOPIC_NAME])

#     # Connect to MySQL
#     conn, cursor = connect_to_mysql()

#     print("Starting Kafka consumer...")
#     try:
#         while True:
#             msg = consumer.poll(1.0)  # Poll for messages
#             if msg is None:
#                 continue
#             if msg.error():
#                 if msg.error().code() == KafkaError._PARTITION_EOF:
#                     # End of partition event
#                     continue
#                 else:
#                     # Other errors
#                     raise KafkaException(msg.error())
#             # Process the message
#             try:
#                 data = json.loads(msg.value().decode('utf-8'))  # Decode JSON message
#                 print(f"Consumed message: {data}")

#                 # Parse and process records from the 'records' key
#                 records = data.get('records', [])
#                 for record in records:
#                     save_to_mysql(cursor, conn, record)
#             except json.JSONDecodeError as e:
#                 print(f"Error decoding JSON: {e}")
#     except KeyboardInterrupt:
#         print("Shutting down consumer...")
#     finally:
#         # Close connections
#         consumer.close()
#         cursor.close()
#         conn.close()

# # Run the consumer
# if __name__ == "__main__":
#     start_consumer()




from datetime import datetime
from confluent_kafka import Consumer, KafkaException, KafkaError
import mysql.connector
import json
import time

# Kafka configuration
KAFKA_CONFIG = {
    'bootstrap.servers': '192.168.56.1:9092',
    'group.id': 'pollution_data_consumer_group',
    'auto.offset.reset': 'earliest'
}

TOPIC_NAME = 'pollution_data'

# MySQL configuration
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'admin',
    'password': 'admin123',
    'database': 'airpollutiondata'
}

# Create a connection to the MySQL database
def connect_to_mysql():
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS air_quality (
                id INT AUTO_INCREMENT PRIMARY KEY,
                country VARCHAR(255),
                state VARCHAR(255),
                city VARCHAR(255),
                station VARCHAR(255),
                last_update DATETIME,
                latitude DECIMAL(10, 7),
                longitude DECIMAL(10, 7),
                pollutant_id VARCHAR(50),
                min_value INT,
                max_value INT,
                avg_value INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        print("Connected to MySQL and ensured table exists.")
        return conn, cursor
    except mysql.connector.Error as err:
        print(f"[MySQL Error] {err}")
        exit(1)


# Parse and convert date to MySQL-compatible format
def parse_date(date_str):
    try:
        return datetime.strptime(date_str, '%d-%m-%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
    except ValueError as e:
        print(f"[Date Parsing Error] {e}")
        return None


# Save data to MySQL
def save_to_mysql(cursor, conn, record):
    try:
        last_update = parse_date(record.get('last_update'))
        if not last_update:
            print(f"[Skipping Record] Invalid date: {record}")
            return

        cursor.execute("""
            INSERT INTO air_quality (
                country, state, city, station, last_update,
                latitude, longitude, pollutant_id,
                min_value, max_value, avg_value
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            record.get('country'),
            record.get('state'),
            record.get('city'),
            record.get('station'),
            last_update,
            float(record.get('latitude')),
            float(record.get('longitude')),
            record.get('pollutant_id'),
            int(record.get('min_value')),
            int(record.get('max_value')),
            int(record.get('avg_value'))
        ))
        conn.commit()
        print(f"[MySQL Insert] Record saved: {record}")
    except mysql.connector.Error as err:
        print(f"[MySQL Insert Error] {err}")
        conn.rollback()
    except (TypeError, ValueError) as ve:
        print(f"[Data Parsing Error] {ve}")


# Kafka Consumer
def start_consumer():
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([TOPIC_NAME])

    conn, cursor = connect_to_mysql()
    last_reconnect = time.time()

    print("Starting Kafka consumer...")

    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                time.sleep(1)
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())

            try:
                data = json.loads(msg.value().decode('utf-8'))
                print(f"[Kafka Message] Received: {data}")

                records = data.get('records', [])
                for record in records:
                    save_to_mysql(cursor, conn, record)

                # Reconnect MySQL periodically
                if time.time() - last_reconnect > 300:  # 5 minutes
                    print("[MySQL Reconnect] Reconnecting to MySQL...")
                    cursor.close()
                    conn.close()
                    conn, cursor = connect_to_mysql()
                    last_reconnect = time.time()

            except json.JSONDecodeError as e:
                print(f"[JSON Decode Error] {e}")
            except Exception as e:
                print(f"[Unexpected Error] {e}")

    except KeyboardInterrupt:
        print("[Shutdown] Consumer shutting down...")
    finally:
        consumer.close()
        cursor.close()
        conn.close()
        print("[Cleanup] Resources closed.")


# Run the consumer
if __name__ == "__main__":
    start_consumer()
