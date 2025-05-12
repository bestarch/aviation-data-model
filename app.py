import streamlit as st
import json
import sys
import os
import redis

from datetime import datetime
from redis.commands.search.query import NumericFilter, Query

#sys.path.append(os.path.abspath('redis_connection'))
from redis_connection.connection import RedisConnection


conn = RedisConnection().get_connection()

st.set_page_config(page_title="Airline Booking Portal", layout="wide")
st.title("🛫 Find Your Perfect Flight")

def is_valid(val):
    return val is not None and str(val).strip() != ""

# Sidebar - Filter Options
with st.sidebar:
    st.header("🔍 Filter Flights")
    source_input = st.text_input("Source Airport", value=st.session_state.get("selected_source", ""))
    if len(source_input) >= 2:
        source_suggestions = conn.ft().sugget("airport_autocomplete", source_input, fuzzy=True, num=5)
        for s in source_suggestions:
            if st.button(s.string, key=f"src_{s.string}"):
                st.session_state.selected_source = s
                #st.rerun()
    source = st.session_state.get("selected_source", source_input)

    destination_input = st.text_input("Destination Airport", value=st.session_state.get("selected_destination", ""))
    if len(destination_input) >= 2:
        destination_suggestions = conn.ft().sugget("airport_autocomplete", destination_input, fuzzy=True, num=5)
        for s in destination_suggestions:
            if st.button(s.string, key=f"dest_{s.string}"):
                st.session_state.selected_destination = s
                #st.rerun()
    destination = st.session_state.get("selected_destination", destination_input)

    departure_date = st.date_input("Departure Date")
    stops_filter = st.multiselect("Stops", options=[0, 1, 2], default=[0, 1, 2])
    seat_class = st.selectbox("Seat Class", options=["Any", "Economy", "Premium Economy", "Business"])
    sort_by = st.selectbox("Sort By", options=["Departure Time", "Price"])
    start_search = st.button("🔍 Search Flights")

if start_search:
    if not is_valid(source) or not is_valid(destination):
        st.error("Please enter both Source and Destination airports.")
        st.stop()

    # Calculate timestamp range for the selected date
    start_ts = int(datetime.combine(departure_date, datetime.min.time()).timestamp())
    end_ts = int(datetime.combine(departure_date, datetime.max.time()).timestamp())

    query = f"@source:{{{source}}} @destination:{{{destination}}}"

    if is_valid(seat_class) and seat_class != 'Any':
        query = query + f"@seat_class:{{{seat_class}}}"

    try:
        sort_by_clause = "departure_ts"
        if sort_by == "Price":
            sort_by_clause = "economy_price"

        qry = ((Query(query)
               .add_filter(NumericFilter("departure_ts", start_ts, end_ts))
               .add_filter(NumericFilter("stops", min(stops_filter), max(stops_filter)))
               .sort_by(sort_by_clause, asc=True)).paging(0, 15)
               .return_fields("flight_number", "source", "destination", "duration", "stops", "departure", "arrival",
                                               "economy_price", "premium_economy_price", "business_price",
                                               "economy_seats", "premium_economy_seats", "business_seats"))

        print(f"Search query --> FT.SEARCH idx_flight_sch \"{qry.query_string()}\" FILTER departure_ts {start_ts} {end_ts} FILTER stops {min(stops_filter)} {max(stops_filter)} SORTBY {sort_by_clause} ASC DIALECT 2 LIMIT 0 15 RETURN 13 flight_number source destination duration stops departure_time arrival_time economy_price premium_economy_price business_price economy_seats premium_economy_seats business_seats")
        result = conn.ft("idx_flight_sch").search(qry)
        print(result)
        flights = []
        for doc in result.docs:
            flights.append({
                'id': doc.id.split(':')[-1],
                'flight_number': doc.flight_number,
                'source': doc.source,
                'destination': doc.destination,
                'duration': doc.duration,
                'departure': datetime.strptime(doc.departure, "%Y-%m-%dT%H:%M:%S").strftime("%b %d, %-I %p"),
                'arrival': datetime.strptime(doc.arrival, "%Y-%m-%dT%H:%M:%S").strftime("%b %d, %-I %p"),
                'economy_price': doc.economy_price,
                'premium_economy_price': doc.premium_economy_price,
                'business_price': doc.business_price,
                'economy_seats': doc.economy_seats,
                'premium_economy_seats': doc.economy_seats,
                'business_seats': doc.business_seats,
                'stops': doc.stops
            })

        print("Result --> " + str(result))

        # Flight Search Results
        if flights:
            st.subheader("✈️ Available Flights")
            for flight in flights:
                with st.container():
                    st.markdown(f"**Flight {flight['flight_number']}**")
                    st.write(f"🕓 Departs: {flight['departure']} | 🛬 Arrives: {flight['arrival']} | Stops: {flight['stops']}")
                    st.write(f"💺 Economy: ₹{flight['economy_price']}, Premium Economy: ₹{flight['economy_price']}, Business: ₹{flight['economy_price']}")
                    print(flight['id'])
                    # if st.button("View Details", key=flight['id']):
                    #     st.session_state.selected_flight_id = flight['id']
                    #     st.session_state.selected_flight_num = flight['flight_number']
                    #     st.session_state.view = 'details'
                    #     #st.rerun()
        else:
            st.warning("No flights found with the selected criteria.")

    except redis.exceptions.ResponseError as e:
        st.error(f"Redis error: {e}")

    # Flight Detail View
    # if st.session_state.get("view") == "details":
    #     flight_id = st.session_state.selected_flight_id
    #     flight_n = st.session_state.selected_flight_num
    #     flight_data = conn.json().get(f"iata:flight:{flight_id}")
    #     print(flight_data)
    #     if flight_data:
    #         st.subheader(f"🛫 Flight Details - {flight_n}")
    #         st.json(flight_data)
    #         if st.button("🔙 Back to Results"):
    #             st.session_state.view = 'results'
    #             st.rerun()
    #     else:
    #         st.error("Flight details not found.")
