from fastapi import FastAPI
import pandas as pd

app = FastAPI()

# Read data from CSV
df = pd.read_csv("trips.csv", parse_dates=["start_time", "end_time"])

# API returns total revenue by city
@app.get("/revenue_per_city")
def revenue_per_city():
    revenue = df.groupby("city")["fare"].sum().to_dict()
    return {"revenue_per_city": revenue}

# API returns total trips by city
@app.get("/trips_per_city")
def trips_per_city():
    trips = df["city"].value_counts().to_dict()
    return {"trips_per_city": trips}

# API returns total distance by city
@app.get("/distance_per_city")
def distance_per_city():
    distance = df.groupby("city")["distance"].sum().to_dict()
    return {"distance_per_city": distance}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
