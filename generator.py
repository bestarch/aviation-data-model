import redis
import sys
import os
import time
import json
from datetime import datetime

sys.path.append(os.path.abspath('redis_connection'))
from connection import RedisConnection


def generate():
    with open("data.json", "r") as r:
        flights = json.load(r)

    for flight in flights:
        key = f"iata:flight:{flight['flight_id']}"
        conn.json().set(key, "$", flight)
        departure_dt = datetime.fromisoformat(flight['departure'])  # Local departure time
        conn.json().set(key, "$.departure_ts", int(departure_dt.timestamp()))

        arrival_dt = datetime.fromisoformat(flight['arrival'])  # Local arrival time
        conn.json().set(key, "$.arrival_ts", int(arrival_dt.timestamp()))

    print(f"Inserted {len(flights)} flights into Redis.")

if __name__ == "__main__":
    conn = RedisConnection().get_connection()
    generate()




