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
        """Fetch weather data for a given city using OpenWeather API"""
        base_url = "http://api.openweathermap.org/data/2.5/weather"
        params = {"q": city, "appid": self.api_key, "units": "imperial"}
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            logger.info(f"Successfully fetched weather data for {city}.")
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch weather data for {city}: {e}")
            return None

    def save_to_s3(self, weather_data, city):
        """Save weather data to the S3 bucket"""
        if not weather_data:
            logger.warning(f"No weather data to save for {city}.")
            return False

        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        file_name = f"weather-data/{city}-{timestamp}.json"
        try:
            weather_data["timestamp"] = timestamp
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=file_name,
                Body=json.dumps(weather_data),
                ContentType="application/json",
            )
            logger.info(f"Weather data for {city} saved to S3 at '{file_name}'.")
            return True
        except Exception as e:
            logger.error(f"Error saving weather data for {city} to S3: {e}")
            return False

def main():
    dashboard = WeatherDashboard()

    # Ensure the S3 bucket exists
    dashboard.create_bucket_if_not_exists()

    # List of cities to fetch weather for
    cities = ["Accra", "Kumasi", "Cape coast"]

    for city in cities:
        logger.info(f"\nFetching weather for {city}...")
        weather_data = dashboard.fetch_weather(city)
        if weather_data:
            # Log weather details
            temp = weather_data["main"]["temp"]
            feels_like = weather_data["main"]["feels_like"]
            humidity = weather_data["main"]["humidity"]
            description = weather_data["weather"][0]["description"]

            logger.info(
                f"Weather in {city}: Temp={temp}°F, Feels Like={feels_like}°F, "
                f"Humidity={humidity}%, Conditions='{description}'."
            )

            # Save data to S3
            if dashboard.save_to_s3(weather_data, city):
                logger.info(f"Weather data for {city} saved successfully.")
        else:
            logger.warning(f"Failed to fetch or save weather data for {city}.")

if __name__ == "__main__":
    main()
