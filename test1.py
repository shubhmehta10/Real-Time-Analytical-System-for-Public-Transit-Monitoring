import requests
import json
from confluent_kafka import Producer
import time

# API Key and Base URL
API_KEY = "1dd0c3ff-12ed-459f-b5de-de86880a85e3"
BASE_URL = "https://bustime.mta.info/api/siri/vehicle-monitoring.json"

# List of LineRef (Routes) to Fetch
LINE_REFS = ["M15", "B44", "Q32", "BX1", "S53"]

# Kafka Configuration
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',  # Update with your Kafka broker address
}
KAFKA_TOPIC = "transit_data"

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

                    # Calculate available seats
                    estimated_capacity = capacities.get('EstimatedPassengerCapacity', 0)
                    estimated_count = capacities.get('EstimatedPassengerCount', 0)
                    available_seats = estimated_capacity - estimated_count

                    all_data.append({
                        "bus_number": journey['VehicleRef'],
                        "route_number": journey['LineRef'],
                        "progress_rate": journey.get('ProgressRate', "Unknown"),
                        "distance_from_stop": monitored_call.get('DistanceFromStop', "Unknown"),
                        "number_of_stops_away": monitored_call.get('NumberOfStopsAway', "Unknown"),
                        "vehicle_location": {
                            "latitude": journey['VehicleLocation']['Latitude'],
                            "longitude": journey['VehicleLocation']['Longitude']
                        },
                        "available_seats": available_seats,
                        "event_time":time.time()
                    })
            else:
                print(f"Failed to fetch data for line {line}. Status code: {response.status_code}")
        except Exception as e:
            print(f"Error fetching data for line {line}: {e}")
    return all_data

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
        # Send the processed data to Kafka
        send_to_kafka(data)
        print("Data processing and Kafka publishing completed.")
