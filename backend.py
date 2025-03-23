from fastapi import FastAPI, Query
import pandas as pd
import json

app = FastAPI()

# Load data from JSON file into memory for fast processing
with open("taxi_trips_data.json", "r", encoding="utf-8") as file:
    trips_data = json.load(file)

df = pd.DataFrame(trips_data)  # Convert data into DataFrame for easy processing

@app.get("/")
def read_root():
    return {"message": "Taxi Data API is running!"}

# API fetches all data (limited to 100 records to avoid overload)
@app.get("/trips")
def get_trips(limit: int = Query(100, description="Số lượng bản ghi trả về")):
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
