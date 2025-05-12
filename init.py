from datetime import datetime, timedelta
import random
import uuid
import json
from redis_connection.connection import RedisConnection


conn = RedisConnection().get_connection()

cities = [
    "Delhi", "Goa", "Assam", "Srinagar", "Chandigarh", "Pune", "Raipur", "Prayagraj", "Dehradun",
    "Trivandrum", "Hyderabad", "Puducherry", "Mumbai", "Kochi", "Bangalore", "Lucknow", "Bhopal",
    "Nagpur", "Chennai", "Kolkata", "Jaipur", "Ahmedabad", "Gangtok", "Puri"
]

def generate():
    # Date range: June 1 to August 31, 2025
    start_date = datetime(2025, 6, 1)
    end_date = datetime(2025, 8, 31)
    date_range = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]

    print(date_range)

    aircraft_types = ["Airbus A320", "Boeing 737", "Airbus A321"]

    meal_options = ["Included", "Not provided"]

    seat_class_config = {
        "Economy": {"price": (2500, 4000), "baggage": {"check_in": "15kg", "cabin": "7kg"}},
        "Premium_Economy": {"price": (4000, 6000), "baggage": {"check_in": "20kg", "cabin": "8kg"}},
        "Business": {"price": (6000, 9000), "baggage": {"check_in": "30kg", "cabin": "10kg"}}
    }

    flights = []

    for source in cities:
        destinations = [city for city in cities if city != source]
        #daily_destinations = random.sample(destinations, k=3)  # Ensure at least 3
        #print(daily_destinations)

        for dest in destinations:
            date_list = []
            for current_date in date_range:
                daily_flights = random.randint(2, 4)

                for daily_flight in range(1, daily_flights+1):
                    departure_time = datetime(
                        current_date.year, current_date.month, current_date.day,
                        random.randint(0, 23), random.choice([0, 15, 30, 45])
                    )
                    departure_ts = departure_time.timestamp()
                    duration = random.randint(60, 240)  # 1 to 4 hours
                    arrival_time = departure_time + timedelta(minutes=duration)
                    arrival_ts = arrival_time.timestamp()

                    departure_time_iso = departure_time.isoformat()
                    date_list.append(departure_time_iso)

                    flight_no = f"AI-{cities.index(source)}{cities.index(dest)}{daily_flight}"

                    flight = {
                        "flight_id": str(uuid.uuid4()),
                        "airline": "Air India",
                        #"flight_number": f"AI{random.randint(10, 999)}",
                        "flight_number": flight_no,
                        "source": source,
                        "destination": dest,
                        "departure": departure_time_iso,
                        "arrival": arrival_time.isoformat(),
                        "departure_ts": departure_ts,
                        "arrival_ts": arrival_ts,
                        "duration_mins": duration,
                        "stops": random.randint(0, 2),
                        "meal": random.choice(meal_options),
                        "seat_classes": {
                            cls: {
                                "price": random.randint(*seat_class_config[cls]["price"]),
                                "baggage": seat_class_config[cls]["baggage"]
                            } for cls in seat_class_config
                        },
                        "available_seats": {
                            "economy": random.randint(30, 100),
                            "premium_economy": random.randint(10, 40),
                            "business": random.randint(5, 30)
                        },
                        "aircraft": random.choice(aircraft_types)
                    }
                    flights.append(flight)

            conn.hset(source, dest, ", ".join(date_list))
            #print(f"Flights from {source} to {dest} runs on {date_list}")

    print(f"JSON file generated with {len(flights)} records")
    output_path = "data.json"
    with open(output_path, "w") as f:
        json.dump(flights, f, indent=2)
