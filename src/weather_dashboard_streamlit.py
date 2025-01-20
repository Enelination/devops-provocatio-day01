# import streamlit as st
# import boto3
# import json
# from datetime import datetime

# # Set up S3 client
# s3_client = boto3.client("s3", region_name="eu-west-3")
# bucket_name = "devops-enel"  # Replace with your bucket name

# def fetch_weather_data_from_s3(city):
#     """Fetch the weather data for a given city from S3"""
#     try:
#         # List objects to get keys (file names) containing weather data for the city
#         response = s3_client.list_objects_v2(
#             Bucket=bucket_name,
#             Prefix=f"weather-data/{city}-",
#         )

#         if "Contents" in response:
#             # Sort by timestamp to get the latest weather data
#             weather_files = sorted(response["Contents"], key=lambda x: x["LastModified"], reverse=True)
#             latest_file_key = weather_files[0]["Key"]

#             # Fetch the latest file content from S3
#             file_data = s3_client.get_object(Bucket=bucket_name, Key=latest_file_key)
#             weather_data = json.loads(file_data["Body"].read().decode("utf-8"))
#             return weather_data
#         else:
#             st.warning(f"No weather data found for {city}.")
#             return None
#     except Exception as e:
#         st.error(f"Error fetching weather data for {city}: {e}")
#         return None

# def display_weather_data(weather_data, city):
#     """Display the weather data"""
#     if weather_data:
#         st.header(f"Weather for {city}")
#         st.write(f"**Temperature**: {weather_data['main']['temp']}°F")
#         st.write(f"**Feels Like**: {weather_data['main']['feels_like']}°F")
#         st.write(f"**Humidity**: {weather_data['main']['humidity']}%")
#         st.write(f"**Condition**: {weather_data['weather'][0]['description']}")
#         st.write(f"**Timestamp**: {weather_data['timestamp']}")

# # Main Streamlit app logic
# st.title("Weather Dashboard")

# # City input
# city = st.selectbox("Select a City", ["Accra", "Kumasi", "Cape coast"])

# # Fetch and display weather data
# if city:
#     weather_data = fetch_weather_data_from_s3(city)
#     display_weather_data(weather_data, city)



import streamlit as st
import boto3
import json
from datetime import datetime

# Set up S3 client
s3_client = boto3.client("s3", region_name="eu-west-3")
bucket_name = "devops-enel"  # Replace with your bucket name

def fetch_weather_data_from_s3(city):
    """Fetch the weather data for a given city from S3"""
    try:
        # List objects to get keys (file names) containing weather data for the city
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=f"weather-data/{city}-",
        )

        if "Contents" in response:
            # Sort by timestamp to get the latest weather data
            weather_files = sorted(response["Contents"], key=lambda x: x["LastModified"], reverse=True)
            latest_file_key = weather_files[0]["Key"]

            # Fetch the latest file content from S3
            file_data = s3_client.get_object(Bucket=bucket_name, Key=latest_file_key)
            weather_data = json.loads(file_data["Body"].read().decode("utf-8"))
            return weather_data
        else:
            st.warning(f"No weather data found for {city}.")
            return None
    except Exception as e:
        st.error(f"Error fetching weather data for {city}: {e}")
        return None

def display_weather_data(weather_data, city):
    """Display the weather data in tabular form"""
    if weather_data:
        st.header(f"Weather for {city}")
        
        # Prepare the data for the table
        data = {
            "Temperature (°F)": [weather_data['main']['temp']],
            "Feels Like (°F)": [weather_data['main']['feels_like']],
            "Humidity (%)": [weather_data['main']['humidity']],
            "Condition": [weather_data['weather'][0]['description']],
            "Timestamp": [weather_data['timestamp']],
        }

        # Convert the dictionary to a DataFrame and display it
        import pandas as pd
        df = pd.DataFrame(data)
        st.dataframe(df)

# Main Streamlit app logic
st.title("Weather Dashboard")

# City input
city = st.selectbox("Select a City", ["Accra", "Kumasi", "Cape coast"])

# Fetch and display weather data
if city:
    weather_data = fetch_weather_data_from_s3(city)
    display_weather_data(weather_data, city)
