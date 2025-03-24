import streamlit as st
import requests
import pandas as pd
import pydeck as pdk

# Call API to get fraud analysis data
API_URL = "http://127.0.0.1:8000/trips/fraud-analysis"

st.title("Taxi Fraud Analysis Dashboard 🚖")

# Send request to get fraud data
response = requests.get(API_URL)
if response.status_code == 200:
    fraud_data = response.json()

    # Show overview information
    st.write("### 📊 Fraud statistics")
    st.write(f"🕒 **Time with the most cheating:** {fraud_data['most_fraudulent_hour']}:00")
    st.write(f"🌦 **Weather with the most fraud:** {fraud_data['most_fraudulent_weather']}")
    st.write(f"🚦 **Traffic conditions with the most fraud:** {fraud_data['most_fraudulent_traffic']}")

    # Fraudulent route data processing
    fraud_routes_df = pd.DataFrame(fraud_data["most_fraud_routes"])

    if not fraud_routes_df.empty:
        st.write("### 🗺️ Most Fraudulent Routes by City")
        
        # Use lat/lon data directly from API
        if fraud_routes_df[["pickup_lat", "pickup_lon"]].isna().sum().sum() == 0:
            st.map(fraud_routes_df.rename(columns={"pickup_lat": "lat", "pickup_lon": "lon"})[["lat", "lon"]])
        else:
            st.warning("⚠️ Map cannot be displayed due to missing latitude/longitude data.")
    else:
        st.write("🚫 Map cannot be displayed due to missing latitude/longitude data.")
else:
    st.error("❌ Unable to get data from API. Check backend server!")