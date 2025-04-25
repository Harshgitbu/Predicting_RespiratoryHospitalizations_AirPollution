import logging
import requests
import json
from azure.eventhub import EventHubProducerClient, EventData
# from azure.identity import DefaultAzureCredential
import datetime
from datetime import timedelta
import os
import pytz
import azure.functions as func

app = func.FunctionApp()

# Configure logging
logging.basicConfig(level=logging.INFO)
# Define your Event Hub connection details
connection_str = os.getenv("EVENT_HUB_CONNECTION_STRING")

pollution_eventhub = "air-pollution-stream" 

cdc_eventhub = "cdc-hospitalization-stream"

api_key = "86515a6313f750bc5bb1c6f6ed60ca47"
pollution_url = "http://api.openweathermap.org/data/2.5/air_pollution"
cdc_base_url = "https://data.cdc.gov/resource/rhwp-grxi.json"

# Function to get real-time air pollution data
def get_pollution_data(lat, lon, api_key):
    try:
        params = {
        # specify the city and the API key?
        "lat": lat,
        "lon": lon,
        "appid": api_key,
        }
        response = requests.get(pollution_url, params=params)
        response.raise_for_status()
        logging.info(f"Air pollution data response: {response.json()}")
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Error fetching air pollution data: {e}")
        return None
    # copy the format of the weather data function, but using air pollution URL instead

# ----- UTILITY FUNCTIONS -----
def get_est_time():
    utc_time = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
    return utc_time.astimezone(pytz.timezone('America/New_York')).isoformat()

# Function to send data to Event Hub
def send_to_eventhub(data, hub_name):
    try:
        producer = EventHubProducerClient.from_connection_string(conn_str=connection_str, eventhub_name=hub_name)
        batch = producer.create_batch()
        batch.add(EventData(json.dumps(data)))
        producer.send_batch(batch)
        producer.close()
        logging.info(f"✅ Sent to Event Hub: {hub_name}")
    except Exception as e:
        logging.error(f"❌ Failed to send to Event Hub {hub_name}: {e}")
    
@app.timer_trigger(schedule="0 */5 * * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=True) 
def project_timer_trigger_Boston(myTimer: func.TimerRequest) -> None:
    try:
        if myTimer.past_due:
            logging.info('The timer is past due!')

        # Get the current time in UTC and convert to EST
        utc_time = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        est_timezone = pytz.timezone('America/New_York')
        est_time = utc_time.astimezone(est_timezone).isoformat()

        logging.info("Starting function execution...")
        lat, lon = 42.3601, -71.0589  # Coordinates for Boston


        logging.info("Fetching pollution data...")
        pollution_data = get_pollution_data(lat, lon, api_key)

        if pollution_data:
            data = {
                "city": "Boston",
                "latitude": lat,
                "longitude": lon,
                "timestamp": est_time,
                "pollution": pollution_data
            }

            logging.info(f"Data to be sent to Event Hub: {data}")
            send_to_eventhub(data, pollution_eventhub)
        else:
            logging.error("Failed to retrieve weather or pollution data.")

    except Exception as e:
        logging.error(f"Error in function execution: {e}")


# ----- CDC TIMER TRIGGER (daily at 2:00 AM) -----
@app.timer_trigger(schedule="0 */2 * * * *", arg_name="cdcTimer", run_on_startup=True, use_monitor=True)
def cdc_trigger_Boston(cdcTimer: func.TimerRequest) -> None:
    try:
        logging.info("🏥 Starting CDC ingestion...")
        seven_days_ago = (datetime.datetime.utcnow() - timedelta(days=7)).strftime('%Y-%m-%d')
        cdc_url = f"{cdc_base_url}?$limit=1000&$where=jurisdiction='MA' AND weekendingdate >= '{seven_days_ago}'"


        response = requests.get(cdc_url)
        response.raise_for_status()
        cdc_data = response.json()
        est_time = get_est_time()

        for record in cdc_data:
            record["ingestion_time"] = est_time
            send_to_eventhub(record, cdc_eventhub)

        logging.info(f"✅ Sent {len(cdc_data)} CDC records from the last 7 days")

    except Exception as e:
        logging.error(f"❌ CDC trigger failed: {e}")

    logging.info('Python timer trigger function executed.')
