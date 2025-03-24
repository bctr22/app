from fastapi import FastAPI, Query
import pandas as pd
import json
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import time

app = FastAPI()

# Load data from JSON file into memory for fast processing
with open("taxi_trips_data.json", "r", encoding="utf-8") as file:
    trips_data = json.load(file)

df = pd.DataFrame(trips_data)  # Convert data into DataFrame for easy processing

geolocator = Nominatim(user_agent="taxi_app")
location_cache = {}

def get_coordinates(address, city):
    full_address = f"{address}, {city}, Vietnam"
    if full_address in location_cache:
        return location_cache[full_address]
    try:
        location = geolocator.geocode(full_address, timeout=10)
        if location:
            coords = (location.latitude, location.longitude)
            location_cache[full_address] = coords
            return coords
    except GeocoderTimedOut:
        time.sleep(1)
        return get_coordinates(address, city)
    return (None, None)

@app.get("/")
def read_root():
    return {"message": "Taxi Data API is running!"}

# API fetches all data (limited to 100 records to avoid overload)
@app.get("/trips")
def get_trips(limit: int = Query(100, description="Number of records returned")):
    return df.head(limit).to_dict(orient="records")

# API take trips by city
@app.get("/trips/city/{city_name}")
def get_trips_by_city(city_name: str, limit: int = 100):
    city_trips = df[df["city"].str.lower() == city_name.lower()]
    return city_trips.head(limit).to_dict(orient="records")

# API to count number of trips and total revenue by city
@app.get("/trips/summary")
def get_summary():
    summary = df.groupby("city").agg({
        "trip_id": "count",
        "fare": "sum"
    }).rename(columns={"trip_id": "total_trips", "fare": "total_revenue"}).reset_index()
    return summary.to_dict(orient="records")

# API filter by weather, traffic
@app.get("/trips/filter")
def filter_trips(weather: str = None, traffic: str = None, limit: int = 100):
    filtered_df = df.copy()
    if weather:
        filtered_df = filtered_df[filtered_df["weather"].str.lower() == weather.lower()]
    if traffic:
        filtered_df = filtered_df[filtered_df["traffic_condition"].str.lower() == traffic.lower()]
    return filtered_df.head(limit).to_dict(orient="records")

# Fraud Rate Statistics API
@app.get("/trips/fraud-rate")
def fraud_analysis():
    total_trips = len(df)
    fraud_trips = df[df["fraud_flag"] == 1]
    fraud_rate = len(fraud_trips) / total_trips * 100
    return {"total_trips": total_trips, "fraud_trips": len(fraud_trips), "fraud_rate": f"{fraud_rate:.2f}%"}

# API calculates key metrics
@app.get("/trips/metrics")
def trip_metrics():
    df["distance_km"] = pd.to_numeric(df["distance_km"], errors="coerce")
    df["duration"] = (pd.to_datetime(df["dropoff_time"]) - pd.to_datetime(df["pickup_time"])).dt.total_seconds() / 60
    
    avg_fare = df["fare"].mean()
    avg_distance = df["distance_km"].mean()
    avg_duration = df["duration"].mean()
    df["hour"] = pd.to_datetime(df["pickup_time"]).dt.hour
    revenue_per_hour = df.groupby("hour")["fare"].sum().to_dict()
    
    return {
        "average_fare": round(avg_fare, 2),
        "average_distance": round(avg_distance, 2),
        "average_duration": round(avg_duration, 2),
        "revenue_per_hour": revenue_per_hour
    }

# API calculates fraud analysis by city, route, time, weather, and traffic
@app.get("/trips/fraud-analysis")
def fraud_analysis_by_factors():
    fraud_df = df[df["fraud_flag"] == 1]
    
    # Most fraudulent route per city
    most_fraud_routes = fraud_df.groupby(["city", "pickup_location", "dropoff_location"]).size().reset_index(name="fraud_count")
    most_fraud_routes = most_fraud_routes.loc[most_fraud_routes.groupby("city")["fraud_count"].idxmax()]
    
    most_fraud_routes[["pickup_lat", "pickup_lon"]] = most_fraud_routes.apply(lambda row: get_coordinates(row["pickup_location"], row["city"]), axis=1, result_type="expand")
    most_fraud_routes[["dropoff_lat", "dropoff_lon"]] = most_fraud_routes.apply(lambda row: get_coordinates(row["dropoff_location"], row["city"]), axis=1, result_type="expand")

    # Most fraudulent hour
    fraud_by_hour = fraud_df.groupby(fraud_df["pickup_time"].str[11:13]).size().idxmax()
    
    # Most fraudulent weather condition
    fraud_by_weather = fraud_df["weather"].mode()[0] if not fraud_df["weather"].isna().all() else "Unknown"
    
    # Most fraudulent traffic condition
    fraud_by_traffic = fraud_df["traffic_condition"].mode()[0] if not fraud_df["traffic_condition"].isna().all() else "Unknown"
    
    return {
        "most_fraud_routes": most_fraud_routes.to_dict(orient="records"),
        "most_fraudulent_hour": fraud_by_hour,
        "most_fraudulent_weather": fraud_by_weather,
        "most_fraudulent_traffic": fraud_by_traffic
    }
