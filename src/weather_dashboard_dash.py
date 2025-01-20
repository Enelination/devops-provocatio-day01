import dash
import dash_bootstrap_components as dbc
from dash import dcc, html
import boto3
import json
from dash.dependencies import Input, Output

# Initialize the Dash app with a Bootstrap theme
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Set up S3 client (make sure the AWS region and credentials match your project settings)
s3_client = boto3.client("s3", region_name="eu-west-3")
bucket_name = "devops-enel"  # Update with your bucket name if needed

def fetch_weather_data_from_s3(city):
    """Fetch weather data from S3"""
    try:
        # Fetch the latest weather data file from S3 based on the city
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=f"weather-data/{city}-",
        )

        if "Contents" in response:
            # Sort by modification time to get the latest data file
            weather_files = sorted(response["Contents"], key=lambda x: x["LastModified"], reverse=True)
            latest_file_key = weather_files[0]["Key"]

            # Get the file's content
            file_data = s3_client.get_object(Bucket=bucket_name, Key=latest_file_key)
            weather_data = json.loads(file_data["Body"].read().decode("utf-8"))
            return weather_data
        else:
            return None
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

def create_weather_layout(city, weather_data):
    """Generate the layout based on fetched weather data"""
    if weather_data:
        return dbc.Card(
            dbc.CardBody([
                html.H5(f"Weather for {city}", className="card-title"),
                html.P(f"Temperature: {weather_data['main']['temp']}°F", className="card-text"),
                html.P(f"Humidity: {weather_data['main']['humidity']}%", className="card-text"),
                html.P(f"Condition: {weather_data['weather'][0]['description']}", className="card-text"),
                html.P(f"Timestamp: {weather_data['timestamp']}", className="card-text"),
            ]),
            color="primary",
            outline=True
        )
    else:
        return dbc.Alert(f"No weather data available for {city}.", color="danger")

# Dash layout with city dropdown and output area for displaying weather data
app.layout = dbc.Container([
    dbc.Row([
        dbc.Col(html.H1("Weather Dashboard", className="text-center mb-4"), width=12)
    ]),
    dbc.Row([
        dbc.Col(dcc.Dropdown(
            id="city-dropdown",
            options=[
                {"label": "Accra", "value": "Accra"},
                {"label": "Kumasi", "value": "Kumasi"},
                {"label": "Cape Coast", "value": "Cape coast"},
            ],
            value="Accra",  # Default city
            style={"width": "50%"},
            className="mx-auto"
        ), width=12)
    ]),
    dbc.Row([
        dbc.Col(html.Div(id="weather-output"), width=12)
    ])
], fluid=True)

@app.callback(
    Output("weather-output", "children"),
    Input("city-dropdown", "value")
)
def update_weather(city):
    weather_data = fetch_weather_data_from_s3(city)
    return create_weather_layout(city, weather_data)

# Run the Dash app
if __name__ == "__main__":
    app.run_server(debug=True)

