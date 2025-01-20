import streamlit as st
import boto3
import json
from datetime import datetime
import pandas as pd

# Initialize S3 client
s3_client = boto3.client("s3")

# Define the bucket name
bucket_name = "your-bucket-name"  # Replace with your S3 bucket name

# Function to fetch the weather data from S3
def fetch_weather_data(city, data_type):
    prefix = f"weather-data/{city}-{data_type}"
    try:
        # List all files in the S3 bucket with the given prefix
        files = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix).get("Contents", [])
        
        # Get the latest file
        if files:
            latest_file = max(files, key=lambda x: x["LastModified"])
            file_key = latest_file["Key"]
            response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
            data = json.loads(response["Body"].read().decode("utf-8"))
            return data
        else:
            return None
    except Exception as e:
        st.error(f"Error fetching data for {city} from S3: {e}")
        return None

# Function to create a dataframe from the current weather data
def create_current_weather_df(city):
    current_data = fetch_weather_data(city, "current")
    if current_data:
        current_weather = {
            "City": city,
            "Temperature (°F)": current_data["main"]["temp"],
            "Feels Like (°F)": current_data["main"]["feels_like"],
            "Humidity (%)": current_data["main"]["humidity"],
            "Pressure (hPa)": current_data["main"]["pressure"],
            "Wind Speed (m/s)": current_data["wind"]["speed"],
            "Wind Direction (°)": current_data["wind"]["deg"],
            "Cloudiness (%)": current_data["clouds"]["all"],
            "Description": current_data["weather"][0]["description"],
            "Sunrise": datetime.utcfromtimestamp(current_data["sys"]["sunrise"]).strftime('%Y-%m-%d %H:%M:%S'),
            "Sunset": datetime.utcfromtimestamp(current_data["sys"]["sunset"]).strftime('%Y-%m-%d %H:%M:%S')
        }
        return pd.DataFrame([current_weather])
    else:
        return None

# Function to create a dataframe from the forecasted weather data
def create_forecast_weather_df(city):
    forecast_data = fetch_weather_data(city, "forecast")
    if forecast_data:
        forecast_list = []
        for forecast in forecast_data["list"]:
            forecast_weather = {
                "City": city,
                "Date & Time": datetime.utcfromtimestamp(forecast["dt"]).strftime('%Y-%m-%d %H:%M:%S'),
                "Temperature (°F)": forecast["main"]["temp"],
                "Feels Like (°F)": forecast["main"]["feels_like"],
                "Humidity (%)": forecast["main"]["humidity"],
                "Wind Speed (m/s)": forecast["wind"]["speed"],
                "Wind Direction (°)": forecast["wind"]["deg"],
                "Cloudiness (%)": forecast["clouds"]["all"],
                "Description": forecast["weather"][0]["description"]
            }
            forecast_list.append(forecast_weather)
        return pd.DataFrame(forecast_list)
    else:
        return None

# Streamlit UI
st.title("Weather Dashboard")

# List of cities to visualize
cities = ["Accra", "Kumasi", "Cape coast"]

# Create a section for each city
for city in cities:
    st.header(f"Weather Data for {city}")

    # Display current weather data
    current_df = create_current_weather_df(city)
    if current_df is not None:
        st.subheader("Current Weather")
        st.dataframe(current_df)
    else:
        st.warning(f"Could not fetch current weather data for {city}.")

    # Display forecasted weather data
    forecast_df = create_forecast_weather_df(city)
    if forecast_df is not None:
        st.subheader("Forecasted Weather")
        st.dataframe(forecast_df)
    else:
        st.warning(f"Could not fetch forecasted weather data for {city}.")
