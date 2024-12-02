import requests
import json
import csv
from confluent_kafka import Producer
import time
from datetime import datetime

# API Key and Base URL
API_KEY = "1dd0c3ff-12ed-459f-b5de-de86880a85e3"
BASE_URL = "https://bustime.mta.info/api/siri/vehicle-monitoring.json"

# List of LineRef (Routes) to Fetch
LINE_REFS = ["M15", "B44", "Q32", "BX1", "S53"]

# Kafka Configuration
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',  # Update with your Kafka broker address
}
KAFKA_TOPIC = "transit_data_4"

# Initialize Confluent Kafka Producer
producer = Producer(KAFKA_CONFIG)

def delivery_report(err, msg):
    """
    Callback for delivery reports from the producer.
    """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

# Fetch Data from MTA API for Specific Routes
def fetch_data(line_refs):
    all_data = []
    for line in line_refs:
        params = {
            "key": API_KEY,
            "version": "2",
            "LineRef": line,
        }
        try:
            response = requests.get(BASE_URL, params=params)
            if response.status_code == 200:
                data = response.json()

                # Debugging: Print the API response structure
                print(f"Response for Line {line}: {json.dumps(data, indent=2)}")

                # Safely parse the response
                vehicles = (
                    data.get('Siri', {})
                    .get('ServiceDelivery', {})
                    .get('VehicleMonitoringDelivery', [{}])[0]
                    .get('VehicleActivity', [])
                )

                # Skip if no vehicles are found
                if not vehicles:
                    print(f"No vehicle data found for line {line}.")
                    continue

                for bus in vehicles:
                    journey = bus['MonitoredVehicleJourney']
                    monitored_call = journey.get('MonitoredCall', {})
                    capacities = monitored_call.get('Extensions', {}).get('Capacities', {})

                    # Extract aimed and expected arrival times
                    aimed_arrival_time = monitored_call.get('AimedArrivalTime', "Unknown")
                    expected_arrival_time = monitored_call.get('ExpectedArrivalTime', "Unknown")

                    # Calculate estimated capacity and count
                    estimated_capacity = capacities.get('EstimatedPassengerCapacity', 0)
                    estimated_count = capacities.get('EstimatedPassengerCount', 0)

                    all_data.append({
                        "bus_number": journey['VehicleRef'],
                        "route_number": journey['LineRef'],
                        "progress_rate": journey.get('ProgressRate', "Unknown"),
                        "distance_from_stop": monitored_call.get('DistanceFromStop', "Unknown"),
                        "number_of_stops_away": monitored_call.get('NumberOfStopsAway', "Unknown"),
                        "latitude": journey['VehicleLocation']['Latitude'],
                        "longitude": journey['VehicleLocation']['Longitude'],
                        "aimed_arrival_time": aimed_arrival_time,
                        "expected_arrival_time": expected_arrival_time,
                        "estimated_capacity": estimated_capacity,
                        "estimated_count": estimated_count,
                        "event_time": time.time(),
                        "delay": str(int((datetime.strptime(aimed_arrival_time, "%Y-%m-%dT%H:%M:%S.%f%z") - datetime.strptime(expected_arrival_time, "%Y-%m-%dT%H:%M:%S.%f%z")).total_seconds() * 1000)),
                        "availabe_seats": estimated_capacity - estimated_count 
                    })
            else:
                print(f"Failed to fetch data for line {line}. Status code: {response.status_code}")
        except Exception as e:
            print(f"Error fetching data for line {line}: {e}")
    return all_data

# Save Data to CSV
def save_to_csv(data, file_name="transit_data.csv"):
    if not data:
        print("No data to save to CSV.")
        return

    # Define the CSV file header
    header = data[0].keys()  # Extract keys from the first dictionary as header

    try:
        with open(file_name, mode="w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=header)
            writer.writeheader()  # Write the header row
            writer.writerows(data)  # Write the data rows
        print(f"Data successfully saved to {file_name}.")
    except Exception as e:
        print(f"Error saving data to CSV: {e}")

# Send Data to Kafka
def send_to_kafka(data):
    for record in data:
        try:
            producer.produce(
                KAFKA_TOPIC,
                key=record["bus_number"],  # Optional key
                value=json.dumps(record),
                callback=delivery_report
            )
        except Exception as e:
            print(f"Failed to send record to Kafka: {e}")
    producer.flush()
    print(f"Data sent to Kafka topic '{KAFKA_TOPIC}'.")

# Main Script
if __name__ == "__main__":
    # Fetch data for selected LineRefs
    data = fetch_data(LINE_REFS)
    if not data:
        print("No data retrieved. Check API key or LineRef values.")
    else:
        # Save data to CSV
        save_to_csv(data)

        # Send the processed data to Kafka
        send_to_kafka(data)
        print("Data processing and Kafka publishing completed.")
