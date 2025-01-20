import os
import json
import boto3
import requests
import logging
from datetime import datetime
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

class WeatherDashboard:
    def __init__(self):
        self.api_key = os.getenv("OPENWEATHER_API_KEY")
        self.bucket_name = os.getenv("AWS_BUCKET_NAME")
        self.region = os.getenv("AWS_REGION", "eu-west-3")  # Updated default region
        self.s3_client = boto3.client("s3", region_name=self.region)

        self.validate_env_vars()

    def validate_env_vars(self):
        """Ensure required environment variables are set"""
        if not self.api_key:
            logger.error("OPENWEATHER_API_KEY is not set in the environment.")
            raise ValueError("Missing required environment variable: OPENWEATHER_API_KEY")
        if not self.bucket_name:
            logger.error("AWS_BUCKET_NAME is not set in the environment.")
            raise ValueError("Missing required environment variable: AWS_BUCKET_NAME")

    def create_bucket_if_not_exists(self):
        """Create an S3 bucket if it doesn't exist"""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"Bucket '{self.bucket_name}' already exists.")
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                logger.info(f"Bucket '{self.bucket_name}' not found. Creating...")
                try:
                    if self.region == "us-east-1":
                        self.s3_client.create_bucket(Bucket=self.bucket_name)
                    else:
                        self.s3_client.create_bucket(
                            Bucket=self.bucket_name,
                            CreateBucketConfiguration={"LocationConstraint": self.region},
                        )
                    logger.info(f"Bucket '{self.bucket_name}' created successfully.")
                except Exception as e:
                    logger.error(f"Error creating bucket: {e}")
                    raise
            else:
                logger.error(f"Error checking bucket existence: {e}")
                raise

    def fetch_weather(self, city):
        """Fetch current weather data for a given city using OpenWeather API"""
        base_url = "http://api.openweathermap.org/data/2.5/weather"
        params = {"q": city, "appid": self.api_key, "units": "imperial"}
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            logger.info(f"Successfully fetched current weather data for {city}.")
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch current weather data for {city}: {e}")
            return None

    def fetch_forecast(self, city):
        """Fetch forecasted weather data for a given city"""
        base_url = "http://api.openweathermap.org/data/2.5/forecast"
        params = {"q": city, "appid": self.api_key, "units": "imperial", "cnt": 5}  # Forecast for 5 days
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            logger.info(f"Successfully fetched forecasted weather data for {city}.")
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch forecasted weather data for {city}: {e}")
            return None

    def save_to_s3(self, weather_data, city, data_type):
        """Save weather data to the S3 bucket"""
        if not weather_data:
            logger.warning(f"No {data_type} weather data to save for {city}.")
            return False

        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        file_name = f"weather-data/{city}-{data_type}-{timestamp}.json"
        try:
            weather_data["timestamp"] = timestamp
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=file_name,
                Body=json.dumps(weather_data),
                ContentType="application/json",
            )
            logger.info(f"{data_type.capitalize()} weather data for {city} saved to S3 at '{file_name}'.")
            return True
        except Exception as e:
            logger.error(f"Error saving {data_type} weather data for {city} to S3: {e}")
            return False

def main():
    dashboard = WeatherDashboard()

    # Ensure the S3 bucket exists
    dashboard.create_bucket_if_not_exists()

    # List of cities to fetch weather for
    cities = ["Accra", "Kumasi", "Cape coast"]

    for city in cities:
        logger.info(f"\nFetching current weather for {city}...")
        current_weather_data = dashboard.fetch_weather(city)
        if current_weather_data:
            # Log current weather details
            temp = current_weather_data["main"]["temp"]
            feels_like = current_weather_data["main"]["feels_like"]
            humidity = current_weather_data["main"]["humidity"]
            pressure = current_weather_data["main"]["pressure"]
            wind_speed = current_weather_data["wind"]["speed"]
            wind_deg = current_weather_data["wind"]["deg"]
            cloudiness = current_weather_data["clouds"]["all"]
            description = current_weather_data["weather"][0]["description"]
            sunrise = datetime.utcfromtimestamp(current_weather_data["sys"]["sunrise"]).strftime('%Y-%m-%d %H:%M:%S')
            sunset = datetime.utcfromtimestamp(current_weather_data["sys"]["sunset"]).strftime('%Y-%m-%d %H:%M:%S')

            logger.info(
                f"Current weather in {city}: Temp={temp}°F, Feels Like={feels_like}°F, "
                f"Humidity={humidity}%, Pressure={pressure} hPa, Wind={wind_speed} m/s, "
                f"Cloudiness={cloudiness}%, Conditions='{description}', Sunrise={sunrise}, Sunset={sunset}."
            )

            # Save current data to S3
            if dashboard.save_to_s3(current_weather_data, city, "current"):
                logger.info(f"Current weather data for {city} saved successfully.")

        # Fetch and log forecast data
        logger.info(f"\nFetching forecasted weather for {city}...")
        forecast_data = dashboard.fetch_forecast(city)
        if forecast_data:
            for forecast in forecast_data["list"]:
                dt = datetime.utcfromtimestamp(forecast["dt"]).strftime('%Y-%m-%d %H:%M:%S')
                temp = forecast["main"]["temp"]
                feels_like = forecast["main"]["feels_like"]
                humidity = forecast["main"]["humidity"]
                description = forecast["weather"][0]["description"]
                wind_speed = forecast["wind"]["speed"]
                wind_deg = forecast["wind"]["deg"]
                cloudiness = forecast["clouds"]["all"]

                logger.info(
                    f"Forecasted weather for {city} on {dt}: Temp={temp}°F, Feels Like={feels_like}°F, "
                    f"Humidity={humidity}%, Wind={wind_speed} m/s, Cloudiness={cloudiness}%, "
                    f"Conditions='{description}'."
                )

            # Save forecast data to S3
            if dashboard.save_to_s3(forecast_data, city, "forecast"):
                logger.info(f"Forecasted weather data for {city} saved successfully.")
        else:
            logger.warning(f"Failed to fetch or save weather data for {city}.")

if __name__ == "__main__":
    main()
