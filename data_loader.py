import sys
import os
import time
import json
from datetime import datetime
import redis
from redis.commands.search.suggestion import Suggestion
import init
from redis.commands.search.field import NumericField, TextField, TagField
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.query import NumericFilter, Query

#sys.path.append(os.path.abspath('redis_connection'))
from redis_connection.connection import RedisConnection


def load(generate_data):
    if generate_data:
        init.generate()

    with open("data.json", "r") as r:
        flights = json.load(r)

    for flight in flights:
        key = f"iata:flight:{flight['flight_id']}"
        conn.json().set(key, "$", flight)
        #departure_dt = datetime.fromisoformat(flight['departure'])  # Local departure time
        #conn.json().set(key, "$.departure_ts", int(departure_dt.timestamp()))
        #arrival_dt = datetime.fromisoformat(flight['arrival'])  # Local arrival time
        #conn.json().set(key, "$.arrival_ts", int(arrival_dt.timestamp()))

    print(f"Inserted {len(flights)} flights into Redis.")


def add_airport_suggestion():
    airports = init.cities
    airport_suggestions = [Suggestion(name, 100) for name in airports]
    conn.ft().sugadd("airport_autocomplete", *airport_suggestions)


def create_index():
    try:
        '''
            FT.CREATE idx_flight_sch ON JSON PREFIX 1 iata:flight: SCHEMA
            $.flight_id AS flight_id TAG
            $.airline AS airline TAG
            $.flight_number AS flight_number TAG
            $.source AS source TAG
            $.destination AS destination TAG
            $.departure AS departure_time TEXT
            $.arrival AS arrival_time TEXT
            $.departure_ts as departure_ts Numeric
            $.arrival_ts as arrrival_ts Numeric
            $.duration_mins AS duration NUMERIC
            $.stops AS stops NUMERIC
            $.meal AS meal TAG
            $.aircraft AS aircraft TAG
            $.seat_classes.Economy.price AS economy_price NUMERIC
            $.seat_classes.Premium_Economy.price AS premium_economy_price NUMERIC
            $.seat_classes.Business.price AS business_price NUMERIC
            $.available_seats.economy AS economy_seats NUMERIC
            $.available_seats.premium_economy AS premium_economy_seats NUMERIC
            $.available_seats.business AS business_seats NUMERIC
        '''
        schema = (TagField("$.flight_id", as_name="flight_id"),
                  TagField("$.airline", as_name="airline"),
                  TagField("$.flight_number", as_name="flight_number"),
                  TagField("$.source", as_name="source"),
                  TagField("$.destination", as_name="destination"),
                  TextField("$.departure", as_name="departure"),
                  TextField("$.arrival", as_name="arrival"),

                  NumericField("$.departure_ts", as_name="departure_ts"),
                  NumericField("$.arrival_ts", as_name="arrival_ts"),
                  NumericField("$.duration_mins", as_name="duration"),
                  NumericField("$.stops", as_name="stops"),
                  TagField("$.meal", as_name="meal"),
                  TagField("$.aircraft", as_name="aircraft"),

                  NumericField("$.seat_classes.Economy.price", as_name="economy_price"),
                  NumericField("$.seat_classes.Premium_Economy.price", as_name="premium_economy_price"),
                  NumericField("$.seat_classes.Business.price", as_name="business_price"),
                  NumericField("$.available_seats.economy", as_name="economy_seats"),
                  NumericField("$.available_seats.premium_economy", as_name="premium_economy_seats"),
                  NumericField("$.available_seats.business", as_name="business_seats"))
        conn.ft("idx_flight_sch").create_index(schema, definition=IndexDefinition(prefix=["iata:flight:"], index_type=IndexType.JSON))
        print("Created index: idx_flight_sch")
    except Exception as inst:
        print("Exception occurred while creating idx_flight_sch index. It may already exist")


if __name__ == "__main__":
    conn = RedisConnection().get_connection()
    generate_data = True
    load(generate_data)
    add_airport_suggestion()
    create_index()




