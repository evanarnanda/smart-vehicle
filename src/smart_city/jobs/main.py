import os
from confluent_kafka  import SerializingProducer
import simplejson as json
from datetime import datetime, timedelta
import uuid
import random

JAKARTA_COORDINATES = {
    'latitude': -6.183333,
    'longitude': 106.816667
}

SURABAYA_COORDINATES = {
    'latitude': -7.2575,
    'longitude': 112.7521
}

# Simple Movement from Jakarta to Surabaya
LATITUDE_INCREMENT = (JAKARTA_COORDINATES['latitude'] - SURABAYA_COORDINATES['latitude']) / 100
LONGITUDE_INCREMENT = (JAKARTA_COORDINATES['longitude'] - SURABAYA_COORDINATES['longitude']) / 100

# Environtment Variables for configuring Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
VEHICLES_TOPIC = os.getenv('VEHICLES_TOPIC', 'vehicles_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_data')

start_time = datetime.now()
start_location = JAKARTA_COORDINATES.copy()

def get_next_timestamp():
    global start_time

    start_time += timedelta(seconds=random.randint(30, 60))
    return start_time

def generate_gps_data(device_id, timestamp, vehicle_type='private'):
    location = simulate_vehicle_movement()
    return {
        'id': uuid.uuid4(),
        'device_id': device_id,
        'timestamp': timestamp,
        'latitude': location['latitude'],
        'longitude': location['longitude'],
        'speed': random.randint(0, 80),
        'vehicle_type': vehicle_type,
    }

def simulate_vehicle_movement():
    global start_location
    
    # move towards Surabaya
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT

    # add some randomness to the movement
    start_location['latitude'] += random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += random.uniform(-0.0005, -0.0005)

    return start_location   

def generate_vehicle_data(device_id):
    return {
        'id': uuid.uuid4(),
        'device_id': device_id,
        'timestamp': get_next_timestamp().isoformat(),
        'make': 'Toyota',
        'model': 'Camry',
        'year': 2018,
        'fuel': 'Hybrid',
    }

def simulate_adventure(producer, vehicle_id):
    while True:
        vehicle_data = generate_vehicle_data(vehicle_id)
        gps_data = generate_gps_data(vehicle_data['device_id'], vehicle_data['timestamp'])

        print(vehicle_data)
        print(gps_data)
        break


if __name__ == '__main__':
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda e: print(f'Kafka error: {e}'),
    }
    producer = SerializingProducer(producer_config)

    try:
        simulate_adventure(producer, 'Vehicle-Test-1')

    except KeyboardInterrupt:
        print(f'Simulation stopped at {datetime.now()}')

    except Exception as e:
        print(f'Simulation failed at {datetime.now()}')
        print(f'Error: {e}')